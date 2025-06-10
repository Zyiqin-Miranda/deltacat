import pyarrow as pa
import pyarrow.compute as pc
import deltacat.compute.converter.utils.iceberg_columns as sc
from deltacat.compute.converter.utils.io import (
    download_data_table_and_append_iceberg_columns,
)
from deltacat.compute.converter.utils.converter_session_utils import (
    sort_data_files_maintaining_order,
)
import logging
from deltacat import logs
from typing import List, Dict, Tuple, Optional, Any
from pyiceberg.manifest import DataFile
import mmh3  # MurmurHash3 for Bloom filter
import numpy as np

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class VersionedBloomFilter:
    def __init__(self, expected_elements: int, false_positive_rate: float = 0.01):
        """Initialize versioned Bloom filter that tracks latest version of each element."""
        self.size = self._get_size(expected_elements, false_positive_rate)
        self.hash_count = self._get_hash_count(self.size, expected_elements)
        # Each slot contains (version, batch_idx, row_idx) or None
        self.slots = np.full(self.size, None, dtype=object)
        logger.info(
            f"Initialized versioned Bloom filter with size {self.size} bits and {self.hash_count} hash functions"
        )

    def _get_size(self, n: int, p: float) -> int:
        """Calculate optimal size of bit array."""
        m = -(n * np.log(p)) / (np.log(2) ** 2)
        return int(m)

    def _get_hash_count(self, m: int, n: int) -> int:
        """Calculate optimal number of hash functions."""
        k = (m / n) * np.log(2)
        return max(1, int(k))  # Ensure at least 1 hash function

    def _get_hash_values(self, item: str) -> List[int]:
        """Generate hash values for an item."""
        hash_values = []
        for seed in range(self.hash_count):
            hash_val = mmh3.hash(str(item), seed) % self.size
            hash_values.append(hash_val)
        return hash_values

    def add_or_update(
        self, key: str, version: int, batch_idx: int, row_idx: int
    ) -> bool:
        """
        Add or update an item with its version info.
        Returns True if this is the latest version seen for this key.
        For records within same version (sequence_number), keeps the one with highest row_idx.
        """
        slots = self._get_hash_values(key)
        is_latest = True

        # First pass: check if we've seen a higher version
        for slot in slots:
            if self.slots[slot] is not None:
                curr_version, curr_batch_idx, curr_row_idx = self.slots[slot]
                if curr_version > version:
                    is_latest = False
                    break
                elif curr_version == version:
                    # For same version, compare row indices
                    if curr_row_idx >= row_idx:
                        is_latest = False
                        break

        # If this is the latest version or has highest row_idx in same version
        if is_latest:
            for slot in slots:
                self.slots[slot] = (version, batch_idx, row_idx)

        return is_latest

    def get_latest_records(self) -> Dict[int, List[Tuple[int, int]]]:
        """
        Returns a mapping of version -> list of (batch_idx, row_idx) for latest records.
        Only returns unique latest records.
        """
        seen_versions = {}  # (version, batch_idx, row_idx) -> True
        version_to_records = {}

        for record in self.slots:
            if record is not None:
                version, batch_idx, row_idx = record
                record_key = (version, batch_idx, row_idx)
                if record_key not in seen_versions:
                    seen_versions[record_key] = True
                    if version not in version_to_records:
                        version_to_records[version] = []
                    version_to_records[version].append((batch_idx, row_idx))

        return version_to_records


def dedupe_data_files(
    data_file_to_dedupe: List[Tuple[int, DataFile]],
    identifier_columns: List[str],
    remaining_data_table_after_convert: Optional[pa.Table],
    merge_sort_column: str,
    s3_client_kwargs: Optional[Dict[str, Any]],
) -> Tuple[pa.Table, int, int]:
    """
    Deduplicate records across data files, ensuring records with highest sequence numbers are kept.
    Uses a versioned Bloom filter to track latest records directly in the filter structure.
    """
    # Sort files by sequence number in ascending order
    data_file_to_dedupe = sort_data_files_maintaining_order(
        data_files=data_file_to_dedupe
    )

    # Count total records to size Bloom filter
    total_records = 0
    if remaining_data_table_after_convert:
        total_records += len(remaining_data_table_after_convert)
    for file_tuple in data_file_to_dedupe:
        data_file = file_tuple[1]
        total_records += data_file.record_count

    # Initialize versioned Bloom filter
    bloom = VersionedBloomFilter(total_records)

    # Process all files to track latest records
    downloaded_data_file_record_count = 0
    data_file_tables = []

    # First handle remaining data table if it exists
    if remaining_data_table_after_convert:
        data_file_tables.append(remaining_data_table_after_convert)
        downloaded_data_file_record_count += len(remaining_data_table_after_convert)

        for batch_idx, batch in enumerate(
            remaining_data_table_after_convert.to_batches()
        ):
            for row_idx in range(len(batch)):
                key = "_".join(
                    str(batch[col][row_idx].as_py()) for col in identifier_columns
                )
                bloom.add_or_update(
                    key, -1, batch_idx, row_idx
                )  # Use -1 for remaining table sequence

    # Process each file in sequence number order
    for sequence_number, data_file in data_file_to_dedupe:
        data_file_table = download_data_table_and_append_iceberg_columns(
            file=data_file,
            columns_to_download=identifier_columns,
            additional_columns_to_append=[
                sc._FILE_PATH_COLUMN_NAME,
                sc._ORDERED_RECORD_IDX_COLUMN_NAME,
            ],
            s3_client_kwargs=s3_client_kwargs,
        )
        logger.info(
            f"Processing file with sequence {sequence_number}, records: {len(data_file_table)}"
        )

        data_file_tables.append(data_file_table)
        downloaded_data_file_record_count += len(data_file_table)

        # Process records to track latest version
        for batch_idx, batch in enumerate(data_file_table.to_batches()):
            for row_idx in range(len(batch)):
                key = "_".join(
                    str(batch[col][row_idx].as_py()) for col in identifier_columns
                )
                bloom.add_or_update(key, sequence_number, batch_idx, row_idx)

    # Get latest records from Bloom filter
    version_to_records = bloom.get_latest_records()

    # Build final table keeping only latest records
    final_tables = []

    # Process remaining table if it exists
    if remaining_data_table_after_convert and -1 in version_to_records:
        records = version_to_records[-1]
        indices_to_keep = [row_idx for _, row_idx in records]
        if indices_to_keep:
            mask = pc.is_in(
                pa.array(range(len(remaining_data_table_after_convert))),
                value_set=pa.array(indices_to_keep),
            )
            final_tables.append(remaining_data_table_after_convert.filter(mask))

    # Process each data file
    for sequence_number, data_file in data_file_to_dedupe:
        if sequence_number in version_to_records:
            records = version_to_records[sequence_number]
            indices_to_keep = [row_idx for _, row_idx in records]
            if indices_to_keep:
                table = data_file_tables[
                    sequence_number + 1
                    if remaining_data_table_after_convert
                    else sequence_number
                ]
                mask = pc.is_in(
                    pa.array(range(len(table))), value_set=pa.array(indices_to_keep)
                )
                final_tables.append(table.filter(mask))

    # Combine all tables
    final_data_to_dedupe = (
        pa.concat_tables(final_tables) if final_tables else pa.table([])
    )
    logger.info(f"Final deduplicated table length: {len(final_data_to_dedupe)}")

    return (
        final_data_to_dedupe,
        downloaded_data_file_record_count,
        int(final_data_to_dedupe.nbytes),
    )
