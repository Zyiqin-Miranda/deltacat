import logging
import importlib
from deltacat.compute.compactor_v2.model.merge_input import MergeInput
import numpy as np
import pyarrow as pa
import ray
import time
import pyarrow.compute as pc
from collections import defaultdict
from deltacat import logs
from typing import List, Optional
from deltacat.types.media import DELIMITED_TEXT_CONTENT_TYPES
from deltacat.compute.compactor_v2.model.merge_result import MergeResult
from deltacat.compute.compactor.model.materialize_result import MaterializeResult
from deltacat.compute.compactor.model.pyarrow_write_result import PyArrowWriteResult
from deltacat.compute.compactor import (
    RoundCompletionInfo,
    DeltaFileEnvelope,
)

from contextlib import nullcontext
from deltacat.types.tables import TABLE_CLASS_TO_SIZE_FUNC
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
    get_current_ray_worker_id,
)
from deltacat.compute.compactor.utils import system_columns as sc

from deltacat.utils.performance import timed_invocation
from deltacat.utils.metrics import emit_timer_metrics
from deltacat.utils.resources import get_current_node_peak_memory_usage_in_bytes
from deltacat.compute.compactor_v2.utils.primary_key_index import (
    generate_pk_hash_column,
    hash_group_index_to_hash_bucket_indices
)


if importlib.util.find_spec("memray"):
    import memray

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def create_chunked_index_array(array: pa.Array) -> pa.Array:
    """
    Creates an chunked array where each chunk is of same size in the input array. 
    """
    chunk_lengths = [len(array.chunk(chunk_index))
                     for chunk_index in range(len(array.chunks))]
    result = np.array([np.arange(cl) for cl in chunk_lengths], dtype="object")
    chunk_lengths = ([0] + chunk_lengths)[:-1]
    result = pa.chunked_array(result + np.cumsum(chunk_lengths))
    return result


def drop_duplicates(table: pa.Table, on: str) -> pa.Table:
    """
    It is important to not combine the chunks for performance reasons. 
    """
    index_array, array_latency = timed_invocation(create_chunked_index_array, table[on])

    logger.info("Created a chunked index array of length "
                f" {len(index_array)} in {array_latency}s")

    table = table.set_column(
        table.shape[1], sc._ORDERED_RECORD_IDX_COLUMN_NAME, index_array)
    selector = table.group_by([on]).aggregate(
        [(sc._ORDERED_RECORD_IDX_COLUMN_NAME, "max")])

    table = table.filter(pc.is_in(table[sc._ORDERED_RECORD_IDX_COLUMN_NAME],
                                  value_set=selector[f'{sc._ORDERED_RECORD_IDX_COLUMN_NAME}_max']))

    table = table.drop([sc._ORDERED_RECORD_IDX_COLUMN_NAME])

    return table


def _dedupe_incremental(
    hash_bucket_index: int, df_envelopes_list: List[List[DeltaFileEnvelope]],
) -> pa.Table:
    logger.info(
        f"[Hash bucket index {hash_bucket_index}] Reading dedupe input for "
        f"{len(df_envelopes_list)} delta file envelope lists..."
    )
    hb_tables = []
    # sort by delta file stream position now instead of sorting every row later
    df_envelopes = [d for dfe_list in df_envelopes_list for d in dfe_list]
    df_envelopes = sorted(
        df_envelopes,
        key=lambda df: (df.stream_position, df.file_index),
        reverse=False,  # ascending
    )
    for df_envelope in df_envelopes:
        hb_tables.append(df_envelope.table)

    hb_table = pa.concat_tables(hb_tables)

    start = time.monotonic()
    # TODO: We do not need to run this when rebasing.
    hb_table = drop_duplicates(
        hb_table, on=sc._PK_HASH_STRING_COLUMN_NAME
    )
    end = time.monotonic()
    logger.info(
        f"Dropping duplicates for incremental in {hash_bucket_index} took: {end - start}"
    )

    logger.info(
        f"Total records in hash bucket {hash_bucket_index} is {hb_table.num_rows}"
    )
    return hb_table


def merge_tables(table, old_table):
    start = time.monotonic()
    mask = pc.invert(
        pc.is_in(
            old_table[sc._PK_HASH_STRING_COLUMN_NAME],
            table[sc._PK_HASH_STRING_COLUMN_NAME],
        )
    )

    result = old_table.filter(mask)
    end = time.monotonic()

    logger.info(
        f"Merging with old table took: {end - start}. Total records: {len(result)} and {len(old_table)} and {len(table)}"
    )

    # TODO(zyiqin): Support merging DELETE DeltaType.

    return pa.concat_tables([result, table])


def download_old_table_and_hash(
    hb_index: int,
    rcf: RoundCompletionInfo,
    read_kwargs_provider,
    primary_keys,
    deltacat_storage,
    deltacat_storage_kwargs: Optional[dict] = None,
):
    tables = []
    hb_index_to_indices = rcf.hb_index_to_entry_range
    indices = hb_index_to_indices[str(hb_index)]

    assert (
        indices is not None and len(indices) == 2
    ), "indices should not be none and contains exactly two elements"

    start = time.monotonic()

    for offset in range(indices[1] - indices[0]):
        table = deltacat_storage.download_delta_manifest_entry(
            rcf.compacted_delta_locator,
            entry_index=(indices[0] + offset),
            file_reader_kwargs_provider=read_kwargs_provider,
            **deltacat_storage_kwargs,
        )

        tables.append(table)

    end = time.monotonic()

    logger.info(
        f"Downloaded {indices[1] - indices[0]} files for hash bucket: {hb_index} in {end - start}s"
    )

    result = pa.concat_tables(tables)
    return generate_pk_hash_column(result, primary_keys=primary_keys)


def _timed_merge(input: MergeInput) -> MergeResult:
    # TODO (rkenmi): Add docstrings for the steps in the compaction workflow
    #  https://github.com/ray-project/deltacat/issues/79
    def _materialize(
        hash_bucket_index,
        compacted_tables: List[pa.Table],
        deltacat_storage_kwargs: Optional[dict] = None,
    ) -> MaterializeResult:
        compacted_table = pa.concat_tables(compacted_tables)
        if input.compacted_file_content_type in DELIMITED_TEXT_CONTENT_TYPES:
            # TODO (rkenmi): Investigate if we still need to convert this table to pandas DataFrame
            # TODO (pdames): compare performance to pandas-native materialize path
            df = compacted_table.to_pandas(split_blocks=True, self_destruct=True)
            compacted_table = df
        delta, stage_delta_time = timed_invocation(
            input.deltacat_storage.stage_delta,
            compacted_table,
            input.write_to_partition,
            max_records_per_entry=input.max_records_per_output_file,
            content_type=input.compacted_file_content_type,
            s3_table_writer_kwargs=input.s3_table_writer_kwargs,
            **deltacat_storage_kwargs,
        )
        compacted_table_size = TABLE_CLASS_TO_SIZE_FUNC[type(compacted_table)](
            compacted_table
        )
        logger.debug(
            f"Time taken for materialize task"
            f" to upload {len(compacted_table)} records"
            f" of size {compacted_table_size} is: {stage_delta_time}s"
        )
        manifest = delta.manifest
        manifest_records = manifest.meta.record_count
        assert manifest_records == len(compacted_table), \
            f"Unexpected Error: Materialized delta manifest record count " \
            f"({manifest_records}) does not equal compacted table record count " \
            f"({len(compacted_table)})"
        materialize_result = MaterializeResult.of(
            delta=delta,
            task_index=hash_bucket_index,
            # TODO (pdames): Generalize WriteResult to contain in-memory-table-type
            #  and in-memory-table-bytes instead of tight coupling to paBytes
            pyarrow_write_result=PyArrowWriteResult.of(
                len(manifest.entries),
                TABLE_CLASS_TO_SIZE_FUNC[type(compacted_table)](compacted_table),
                manifest.meta.content_length,
                len(compacted_table),
            ),
        )
        logger.info(f"Materialize result: {materialize_result}")
        return materialize_result

    task_id = get_current_ray_task_id()
    worker_id = get_current_ray_worker_id()
    with memray.Tracker(
        f"dedupe_{worker_id}_{task_id}.bin"
    ) if input.enable_profiler else nullcontext():
        # TODO (pdames): mitigate risk of running out of memory here in cases of
        #  severe skew of primary key updates in deltas
        logger.info(
            f"[Dedupe task {input.merge_task_index}] Getting delta file envelope "
            f"groups for {len(input.dfe_groups_refs)} object refs..."
        )

        delta_file_envelope_groups_list = input.object_store.get_many(
            input.dfe_groups_refs
        )
        hb_index_to_delta_file_envelopes_list = defaultdict(list)
        for delta_file_envelope_groups in delta_file_envelope_groups_list:
            assert input.hash_bucket_count == len(delta_file_envelope_groups), \
                f"The hash bucket count must match the dfe size as {input.hash_bucket_count}" \
                f" != {len(delta_file_envelope_groups)}"

            for hb_idx, dfes in enumerate(delta_file_envelope_groups):
                if dfes is not None:
                    hb_index_to_delta_file_envelopes_list[hb_idx].append(dfes)

        # TODO(zyiqin): Support copy all manifest files from empty hash bucket

        logger.info(
            f"[Dedupe task {input.merge_task_index}] Running {len(hb_index_to_delta_file_envelopes_list)} "
            f"dedupe rounds..."
        )
        total_deduped_records = 0

        materialized_results: List[MaterializeResult] = []

        valid_hb_indices_iterable = hash_group_index_to_hash_bucket_indices(input.hash_group_index,
                                                                            input.hash_bucket_count,
                                                                            input.num_hash_groups)

        for hb_idx in valid_hb_indices_iterable:
            dfe_list = hb_index_to_delta_file_envelopes_list[hb_idx]

            if dfe_list:
                logger.info(
                    f"{input.merge_task_index}: union primary keys for hb_index: {hb_idx}"
                )

                table, union_time = timed_invocation(
                    func=_dedupe_incremental,
                    hash_bucket_index=hb_idx,
                    df_envelopes_list=dfe_list,
                )

                logger.info(
                    f"[Dedupe {input.merge_task_index}] Dedupe round input "
                    f"record count: {len(table)}, took {union_time}s"
                )

                print(f"Deduped Table: {table}")

                # FIXME: sort must be done before dedupe
                if input.sort_keys:
                    table = table.take(
                        pc.sort_indices(table, sort_keys=input.sort_keys)
                    )

                # drop duplicates by primary key hash column
                logger.info(
                    f"[Dedupe task index {input.merge_task_index}] Dropping duplicates for {hb_idx}"
                )

                if input.round_completion_info is not None:
                    compacted_table = download_old_table_and_hash(
                        hb_idx,
                        input.round_completion_info,
                        input.read_kwargs_provider,
                        input.primary_keys,
                        input.deltacat_storage,
                        input.deltacat_storage_kwargs,
                    )

                    hb_table_record_count = len(table) + len(compacted_table)
                    table, drop_time = timed_invocation(
                        func=merge_tables, old_table=compacted_table, table=table
                    )
                    deduped_record_count = hb_table_record_count - len(table)
                    total_deduped_records += deduped_record_count

                    logger.info(
                        f"[Dedupe task index {input.merge_task_index}] Dedupe round output "
                        f"record count: {len(table)}, took: {drop_time}s"
                    )

                table = table.drop(
                    [
                        sc._PK_HASH_STRING_COLUMN_NAME,
                    ]
                )

                materialized_results.append(
                    _materialize(hb_idx, [table], input.deltacat_storage_kwargs)
                )
            else:
                # TODO: copy by reference
                pass

        peak_memory_usage_bytes = get_current_node_peak_memory_usage_in_bytes()

        return MergeResult(
            materialized_results,
            np.int64(total_deduped_records),
            np.double(peak_memory_usage_bytes),
            np.double(0.0),
            np.double(time.time()),
        )


@ray.remote
def merge(input: MergeInput) -> MergeResult:

    logger.info(f"Starting merge task...")
    merge_result, duration = timed_invocation(func=_timed_merge, input=input)

    emit_metrics_time = 0.0
    if input.metrics_config:
        emit_result, latency = timed_invocation(
            func=emit_timer_metrics,
            metrics_name="merge",
            value=duration,
            metrics_config=input.metrics_config,
        )
        emit_metrics_time = latency

    logger.info(f"Finished merge task...")
    return MergeResult(
        merge_result[0],
        merge_result[1],
        merge_result[2],
        np.double(emit_metrics_time),
        merge_result[4],
    )
