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
import ray
from ray.util.annotations import PublicAPI

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@PublicAPI
@ray.remote
def hash_partition_single_file(
    data_file: Tuple[int, DataFile],
    identifier_columns: List[str],
    num_sub_partitions: int,
    s3_client_kwargs: Optional[Dict[str, Any]],
) -> Dict[int, List[Tuple[int, DataFile]]]:
    """
    Hash partition a single data file into sub-buckets based on identifier columns hash.
    Returns a mapping of sub-bucket index to list of (sequence_number, data_file).
    """
    sequence_number, file = data_file

    # Download just this file
    data_file_table = download_data_table_and_append_iceberg_columns(
        file=file,
        columns_to_download=identifier_columns,
        additional_columns_to_append=[
            sc._FILE_PATH_COLUMN_NAME,
            sc._ORDERED_RECORD_IDX_COLUMN_NAME,
        ],
        s3_client_kwargs=s3_client_kwargs,
    )

    # Calculate hash values for partitioning
    hash_values = pc.hash(data_file_table[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME])

    # Assign each record to a sub-bucket
    sub_bucket_indices = pc.modulo(hash_values, num_sub_partitions)

    # Group by sub-bucket
    sub_bucket_to_files = {i: [] for i in range(num_sub_partitions)}

    # Count records in each bucket
    bucket_counts = {}
    for bucket_idx in sub_bucket_indices:
        bucket_idx = bucket_idx.as_py()
        bucket_counts[bucket_idx] = bucket_counts.get(bucket_idx, 0) + 1

    # Only add file to buckets that have records
    for bucket_idx, count in bucket_counts.items():
        if count > 0:
            sub_bucket_to_files[bucket_idx].append((sequence_number, file))

    return sub_bucket_to_files


@PublicAPI
@ray.remote
def hash_partition_remaining_table(
    remaining_table: pa.Table,
    num_sub_partitions: int,
) -> Dict[int, pa.Table]:
    """
    Hash partition the remaining table into sub-buckets.
    Returns a mapping of sub-bucket index to table slice.
    """
    if not remaining_table:
        return {i: pa.Table.from_arrays([], []) for i in range(num_sub_partitions)}

    # Calculate hash values for partitioning
    hash_values = pc.hash(remaining_table[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME])

    # Assign each record to a sub-bucket
    sub_bucket_indices = pc.modulo(hash_values, num_sub_partitions)

    # Split table into sub-buckets
    sub_bucket_tables = {}
    for i in range(num_sub_partitions):
        mask = pc.equal(sub_bucket_indices, i)
        sub_bucket_tables[i] = remaining_table.filter(mask)

    return sub_bucket_tables


@PublicAPI
@ray.remote
def dedupe_sub_bucket(
    sub_bucket_files: List[Tuple[int, DataFile]],
    identifier_columns: List[str],
    s3_client_kwargs: Optional[Dict[str, Any]],
    remaining_tables: Optional[Dict[int, pa.Table]] = None,
) -> Tuple[pa.Table, int, int]:
    """
    Deduplicate records within a sub-bucket.
    Returns (table_to_delete, total_records, total_bytes).
    """
    # Sort files by sequence number
    sub_bucket_files = sort_data_files_maintaining_order(data_files=sub_bucket_files)

    # Process files in the sub-bucket
    data_file_table = []
    downloaded_data_file_record_count = 0

    for sequence_number, data_file in sub_bucket_files:
        data_file_to_dedupe_table = download_data_table_and_append_iceberg_columns(
            file=data_file,
            columns_to_download=identifier_columns,
            additional_columns_to_append=[
                sc._FILE_PATH_COLUMN_NAME,
                sc._ORDERED_RECORD_IDX_COLUMN_NAME,
            ],
            s3_client_kwargs=s3_client_kwargs,
        )
        downloaded_data_file_record_count += len(data_file_to_dedupe_table)
        data_file_table.append(data_file_to_dedupe_table)

    if not data_file_table:
        return pa.Table.from_arrays([], []), 0, 0

    final_data_to_dedupe = pa.concat_tables(data_file_table)

    # Append global record index
    record_idx_iterator = iter(range(len(final_data_to_dedupe)))
    final_data_to_dedupe = sc.append_global_record_idx_column(
        final_data_to_dedupe, record_idx_iterator
    )

    # Group by hash and take max record index
    final_data_table_indices = final_data_to_dedupe.group_by(
        sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME, use_threads=False
    ).aggregate([(sc._GLOBAL_RECORD_IDX_COLUMN_NAME, "max")])

    # Find records to delete
    pos_delete_indices = pc.invert(
        pc.is_in(
            final_data_to_dedupe[sc._GLOBAL_RECORD_IDX_COLUMN_NAME],
            value_set=final_data_table_indices[
                f"{sc._GLOBAL_RECORD_IDX_COLUMN_NAME}_max"
            ],
        )
    )

    final_data_table_to_delete = final_data_to_dedupe.filter(pos_delete_indices)

    # Drop hash and index columns
    final_data_table_to_delete = final_data_table_to_delete.drop(
        [sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME, sc._GLOBAL_RECORD_IDX_COLUMN_NAME]
    )

    return (
        final_data_table_to_delete,
        len(final_data_to_dedupe),
        int(final_data_to_dedupe.nbytes),
    )


def dedupe_data_files(
    data_file_to_dedupe: List[Tuple[int, DataFile]],
    identifier_columns: List[str],
    remaining_data_table_after_convert: Optional[pa.Table],
    merge_sort_column: str,
    s3_client_kwargs: Optional[Dict[str, Any]],
    num_sub_partitions: Optional[int] = None,
) -> Tuple[pa.Table, int, int]:
    """
    Deduplicate records across data files, with optional sub-partitioning for large datasets.

    Args:
        data_file_to_dedupe: List of (sequence_number, data_file) tuples
        identifier_columns: List of column names to use for deduplication
        remaining_data_table_after_convert: Optional table with remaining data
        merge_sort_column: Column to use for sorting
        s3_client_kwargs: Optional S3 client arguments
        num_sub_partitions: Optional number of sub-partitions to use for large datasets

    Returns:
        Tuple of (table_to_delete, total_records, total_bytes)
    """
    if not num_sub_partitions:
        # Use original single-partition implementation
        data_file_table = []
        if remaining_data_table_after_convert:
            data_file_table.append(remaining_data_table_after_convert)

        data_file_to_dedupe = sort_data_files_maintaining_order(
            data_files=data_file_to_dedupe
        )
        downloaded_data_file_record_count = 0
        for file_tuple in data_file_to_dedupe:
            data_file = file_tuple[1]
            data_file_to_dedupe_table = download_data_table_and_append_iceberg_columns(
                file=data_file,
                columns_to_download=identifier_columns,
                additional_columns_to_append=[
                    sc._FILE_PATH_COLUMN_NAME,
                    sc._ORDERED_RECORD_IDX_COLUMN_NAME,
                ],
                s3_client_kwargs=s3_client_kwargs,
            )
            logger.info(
                f"Length of downloaded data file table: {len(data_file_to_dedupe_table)}"
            )
            downloaded_data_file_record_count += len(data_file_to_dedupe_table)
            data_file_table.append(data_file_to_dedupe_table)

        final_data_to_dedupe = pa.concat_tables(data_file_table)

        dedupe_input_record_count = downloaded_data_file_record_count
        if remaining_data_table_after_convert:
            dedupe_input_record_count += len(remaining_data_table_after_convert)
        assert len(final_data_to_dedupe) == dedupe_input_record_count, (
            f"Mismatch record count while performing table concat, Got {len(final_data_to_dedupe)} in final table, "
            f"while input table length is: {dedupe_input_record_count}"
        )

        logger.info(f"Length of pyarrow table to dedupe:{len(final_data_to_dedupe)}")

        record_idx_iterator = iter(range(len(final_data_to_dedupe)))

        final_data_to_dedupe = sc.append_global_record_idx_column(
            final_data_to_dedupe, record_idx_iterator
        )

        final_data_table_indices = final_data_to_dedupe.group_by(
            sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME, use_threads=False
        ).aggregate([(sc._GLOBAL_RECORD_IDX_COLUMN_NAME, "max")])

        pos_delete_indices = pc.invert(
            pc.is_in(
                final_data_to_dedupe[sc._GLOBAL_RECORD_IDX_COLUMN_NAME],
                value_set=final_data_table_indices[
                    f"{sc._GLOBAL_RECORD_IDX_COLUMN_NAME}_max"
                ],
            )
        )

        final_data_table_to_delete = final_data_to_dedupe.filter(pos_delete_indices)

        final_data_table_to_delete = final_data_table_to_delete.drop(
            [sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME, sc._GLOBAL_RECORD_IDX_COLUMN_NAME]
        )
        logger.info(
            f"Deduped {len(final_data_table_to_delete)} Records based off identifier columns."
        )
        return (
            final_data_table_to_delete,
            len(final_data_to_dedupe),
            int(final_data_to_dedupe.nbytes),
        )
    else:
        # Use sub-partitioning approach
        # First, partition each file individually
        file_partition_tasks = []
        for data_file in data_file_to_dedupe:
            task = hash_partition_single_file.remote(
                data_file=data_file,
                identifier_columns=identifier_columns,
                num_sub_partitions=num_sub_partitions,
                s3_client_kwargs=s3_client_kwargs,
            )
            file_partition_tasks.append(task)

        # Partition remaining table if it exists
        if remaining_data_table_after_convert:
            remaining_table_task = hash_partition_remaining_table.remote(
                remaining_table=remaining_data_table_after_convert,
                num_sub_partitions=num_sub_partitions,
            )
            file_partition_tasks.append(remaining_table_task)

        # Collect all partition results
        partition_results = ray.get(file_partition_tasks)

        # Combine file partitions into sub-buckets
        sub_bucket_to_files = {i: [] for i in range(num_sub_partitions)}
        sub_bucket_tables = {i: [] for i in range(num_sub_partitions)}

        for result in partition_results:
            if isinstance(result, dict) and all(
                isinstance(v, list) for v in result.values()
            ):
                # This is a file partition result
                for bucket_idx, files in result.items():
                    sub_bucket_to_files[bucket_idx].extend(files)
            else:
                # This is a remaining table partition result
                for bucket_idx, table in result.items():
                    if len(table) > 0:
                        sub_bucket_tables[bucket_idx].append(table)

        # Process each sub-bucket in parallel
        dedupe_tasks = []
        for sub_bucket_idx in range(num_sub_partitions):
            files = sub_bucket_to_files[sub_bucket_idx]
            tables = sub_bucket_tables[sub_bucket_idx]

            if files or tables:  # Only process non-empty buckets
                task = dedupe_sub_bucket.remote(
                    sub_bucket_files=files,
                    identifier_columns=identifier_columns,
                    s3_client_kwargs=s3_client_kwargs,
                    remaining_tables=tables,
                )
                dedupe_tasks.append(task)

        # Collect results from all sub-buckets
        sub_bucket_results = ray.get(dedupe_tasks)

        # Combine results
        total_records = 0
        total_bytes = 0
        tables_to_delete = []

        for table_to_delete, records, bytes_ in sub_bucket_results:
            if len(table_to_delete) > 0:
                tables_to_delete.append(table_to_delete)
            total_records += records
            total_bytes += bytes_

        if not tables_to_delete:
            return pa.Table.from_arrays([], []), total_records, total_bytes

        final_table_to_delete = pa.concat_tables(tables_to_delete)
        return final_table_to_delete, total_records, total_bytes
