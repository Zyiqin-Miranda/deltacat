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


def drop_duplicates(table: pa.Table, subset: List[str]) -> Tuple[pa.Table, pa.Table]:
    """
    Drop duplicate rows from a PyArrow table based on specified columns.
    Uses PyArrow's group_by and aggregate functions to efficiently drop duplicates.
    Returns both the table with duplicates to keep and the table with duplicates to delete.

    Args:
        table: The input PyArrow table
        subset: List of column names to check for duplicates

    Returns:
        Tuple of (table_to_keep, table_to_delete)
    """
    if not table or not subset:
        return table, pa.Table.from_arrays([], [])

    # Group by the hash column and take the first record for each group
    grouped = table.group_by(sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME).aggregate(
        [(sc._ORDERED_RECORD_IDX_COLUMN_NAME, "max")]
    )

    # Get the indices of records to keep
    indices_to_keep = grouped[f"{sc._ORDERED_RECORD_IDX_COLUMN_NAME}_max"].to_numpy()

    # Create masks for the rows to keep and delete
    keep_mask = pc.is_in(
        pa.array(range(len(table))),
        value_set=pa.array(indices_to_keep),
    )
    delete_mask = pc.invert(keep_mask)

    # Return both tables
    return table.filter(keep_mask), table.filter(delete_mask)


class VersionedBloomFilter:
    def __init__(self, expected_elements: int, false_positive_rate: float = 0.01):
        """Initialize versioned Bloom filter that tracks latest version of each element."""
        self.size = self._get_size(expected_elements, false_positive_rate)
        self.hash_count = self._get_hash_count(self.size, expected_elements)
        # Each slot contains (version, batch_idx, row_idx) or None
        self.slots = np.full(self.size, None, dtype=object)
        # Track individual records by their hash
        self.record_versions = {}  # hash -> (version, batch_idx, row_idx)
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
    ) -> Tuple[bool, Optional[Tuple[int, int, int]]]:
        """
        Add or update an item with its version info.
        Returns (is_latest, previous_version_info) where:
        - is_latest: True if this is the latest version seen for this key
        - previous_version_info: (version, batch_idx, row_idx) of previous version if exists
        """
        slots = self._get_hash_values(key)
        is_latest = True
        previous_version = None

        # Check if we've seen this record before
        if key in self.record_versions:
            prev_version, prev_batch_idx, prev_row_idx = self.record_versions[key]
            if prev_version > version:
                is_latest = False
                previous_version = (prev_version, prev_batch_idx, prev_row_idx)
            elif prev_version == version and prev_row_idx >= row_idx:
                is_latest = False
                previous_version = (prev_version, prev_batch_idx, prev_row_idx)

        # If this is the latest version, update the record
        if is_latest:
            self.record_versions[key] = (version, batch_idx, row_idx)
            for slot in slots:
                self.slots[slot] = (version, batch_idx, row_idx)

        return is_latest, previous_version

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
    Files are processed in descending order by sequence number to ensure latest records are kept.
    Returns the table containing records to be deleted.
    """
    # Sort files by sequence number in descending order
    data_file_to_dedupe = sort_data_files_maintaining_order(
        data_files=data_file_to_dedupe, reverse=True
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
    tables_to_delete = []

    # First handle remaining data table if it exists
    if remaining_data_table_after_convert:
        downloaded_data_file_record_count += len(remaining_data_table_after_convert)

        # Drop duplicates within the remaining table first
        remaining_table_to_keep, remaining_table_to_delete = drop_duplicates(
            remaining_data_table_after_convert, identifier_columns
        )

        # Process records to track latest version
        for batch_idx, batch in enumerate(remaining_table_to_keep.to_batches()):
            for row_idx in range(len(batch)):
                key = batch[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME][row_idx].as_py()
                bloom.add_or_update(key, -1, batch_idx, row_idx)

        if len(remaining_table_to_delete) > 0:
            tables_to_delete.append(remaining_table_to_delete)

    # Process each file in sequence number order (descending)
    for sequence_number, data_file in data_file_to_dedupe:
        # Download and process one file at a time
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

        downloaded_data_file_record_count += len(data_file_table)

        # Drop duplicates within this file first
        data_file_table_to_keep, data_file_table_to_delete = drop_duplicates(
            data_file_table, identifier_columns
        )

        # Process records to track latest version
        records_to_keep = []
        records_to_delete = []

        for batch_idx, batch in enumerate(data_file_table_to_keep.to_batches()):
            for row_idx in range(len(batch)):
                key = batch[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME][row_idx].as_py()
                is_latest, previous_version = bloom.add_or_update(
                    key, sequence_number, batch_idx, row_idx
                )

                if not is_latest:
                    # If we've seen this record in a higher sequence number, add to delete
                    records_to_delete.append(row_idx)
                else:
                    # If this is a new record or has a higher sequence number, keep it
                    records_to_keep.append(row_idx)

        # Add records to delete
        if records_to_delete:
            delete_mask = pc.is_in(
                pa.array(range(len(data_file_table_to_keep))),
                value_set=pa.array(records_to_delete),
            )
            tables_to_delete.append(data_file_table_to_keep.filter(delete_mask))

        # Add any records from the initial deduplication
        if len(data_file_table_to_delete) > 0:
            tables_to_delete.append(data_file_table_to_delete)

    # Combine all tables to delete
    if not tables_to_delete:
        return pa.Table.from_arrays([], []), 0, downloaded_data_file_record_count

    final_table_to_delete = pa.concat_tables(tables_to_delete)

    # Drop the hash column from the final result
    final_table_to_delete = final_table_to_delete.drop(
        [sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME]
    )

    return (
        final_table_to_delete,
        len(final_table_to_delete),
        downloaded_data_file_record_count,
    )
