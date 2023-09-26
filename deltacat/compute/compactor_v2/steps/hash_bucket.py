import importlib
import logging
import time
from contextlib import nullcontext
from typing import List, Optional, Tuple
from deltacat.compute.compactor_v2.model.hash_bucket_input import HashBucketInput
import numpy as np
import pyarrow as pa
import ray
from deltacat import logs
from deltacat.compute.compactor import (
    DeltaAnnotated,
    DeltaFileEnvelope,
)
from deltacat.compute.compactor.model.delta_file_envelope import DeltaFileEnvelopeGroups
from deltacat.compute.compactor_v2.model.hash_bucket_result import HashBucketResult
from deltacat.compute.compactor_v2.utils.primary_key_index import (
    group_hash_bucket_indices,
    group_by_pk_hash_bucket,
)
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.types.media import StorageType
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
    get_current_ray_worker_id,
)
from deltacat.utils.common import ReadKwargsProvider
from deltacat.utils.performance import timed_invocation
from deltacat.utils.metrics import emit_timer_metrics
from deltacat.utils.resources import get_current_node_peak_memory_usage_in_bytes

if importlib.util.find_spec("memray"):
    import memray

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _read_delta_file_envelopes(
    annotated_delta: DeltaAnnotated,
    read_kwargs_provider: Optional[ReadKwargsProvider],
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[dict] = None,
) -> Tuple[Optional[List[DeltaFileEnvelope]], int, int]:

    tables = deltacat_storage.download_delta(
        annotated_delta,
        max_parallelism=1,
        file_reader_kwargs_provider=read_kwargs_provider,
        storage_type=StorageType.LOCAL,
        **deltacat_storage_kwargs,
    )
    annotations = annotated_delta.annotations
    assert (
        len(tables) == len(annotations),
        f"Unexpected Error: Length of downloaded delta manifest tables "
        f"({len(tables)}) doesn't match the length of delta manifest "
        f"annotations ({len(annotations)}).",
    )
    if not tables:
        return None, 0, 0

    delta_stream_position = annotations[0].annotation_stream_position
    delta_type = annotations[0].annotation_delta_type

    for annotation in annotations:
        assert annotation.annotation_stream_position == delta_stream_position, (
            f"Annotation stream position does not match - {annotation.annotation_stream_position} "
            f"!= {delta_stream_position}"
        )
        assert annotation.annotation_delta_type == delta_type, (
            f"Annotation delta type does not match - {annotation.annotation_delta_type} "
            f"!= {delta_type}"
        )

    delta_file_envelopes = []
    table = pa.concat_tables(tables)
    total_record_count = len(table)
    total_size_bytes = int(table.nbytes)

    delta_file = DeltaFileEnvelope.of(
        stream_position=delta_stream_position,
        delta_type=delta_type,
        table=table,
    )
    delta_file_envelopes.append(delta_file)
    return delta_file_envelopes, total_record_count, total_size_bytes


def _group_file_records_by_pk_hash_bucket(
    annotated_delta: DeltaAnnotated,
    num_hash_buckets: int,
    primary_keys: List[str],
    read_kwargs_provider: Optional[ReadKwargsProvider] = None,
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[dict] = None,
) -> Tuple[Optional[DeltaFileEnvelopeGroups], int, int]:
    # read input parquet s3 objects into a list of delta file envelopes
    (
        delta_file_envelopes,
        total_record_count,
        total_size_bytes,
    ) = _read_delta_file_envelopes(
        annotated_delta,
        read_kwargs_provider,
        deltacat_storage,
        deltacat_storage_kwargs,
    )

    if delta_file_envelopes is None:
        return None, 0, 0

    logger.info(
        f"Read all delta file envelopes: {len(delta_file_envelopes)} "
        f"and total_size_bytes={total_size_bytes} and records={total_record_count}"
    )

    # group the data by primary key hash value
    hb_to_delta_file_envelopes = np.empty([num_hash_buckets], dtype="object")
    for dfe in delta_file_envelopes:
        logger.info("Grouping by pk hash bucket")
        start = time.monotonic()
        hash_bucket_to_table = group_by_pk_hash_bucket(
            dfe.table,
            num_hash_buckets,
            primary_keys,
        )
        group_end = time.monotonic()
        logger.info(f"Grouping took: {group_end - start}")

        for hb, table in enumerate(hash_bucket_to_table):
            if table:
                if hb_to_delta_file_envelopes[hb] is None:
                    hb_to_delta_file_envelopes[hb] = []
                hb_to_delta_file_envelopes[hb].append(
                    DeltaFileEnvelope.of(
                        stream_position=dfe.stream_position,
                        file_index=dfe.file_index,
                        delta_type=dfe.delta_type,
                        table=table,
                    )
                )
    return hb_to_delta_file_envelopes, total_record_count, total_size_bytes


# def _timed_hash_bucket(input: HashBucketInput):
#     task_id = get_current_ray_task_id()
#     worker_id = get_current_ray_worker_id()
#     with memray.Tracker(
#         f"hash_bucket_{worker_id}_{task_id}.bin"
#     ) if input.enable_profiler else nullcontext():
#         (
#             delta_file_envelope_groups,
#             total_record_count,
#             total_size_bytes,
#         ) = _group_file_records_by_pk_hash_bucket(
#             annotated_delta=input.annotated_delta,
#             num_hash_buckets=input.num_hash_buckets,
#             primary_keys=input.primary_keys,
#             read_kwargs_provider=input.read_kwargs_provider,
#             deltacat_storage=input.deltacat_storage,
#             deltacat_storage_kwargs=input.deltacat_storage_kwargs,
#         )
#         hash_bucket_group_to_obj_id_size_tuple = np.empty([input.num_hash_groups], dtype="object")
#
#         """
#          This method persists all tables for a given hash bucket into the object store
#          and returns the object references for each hash group.
#          """
#
#         num_groups = input.num_hash_groups
#         object_store = input.object_store
#         num_buckets = input.num_hash_buckets
#         hash_bucket_group_to_obj_id_size_tuple = np.empty([num_groups], dtype="object")
#
#         if delta_file_envelope_groups is None:
#             return hash_bucket_group_to_obj_id_size_tuple
#
#         hb_group_to_object = np.empty([num_groups], dtype="object")
#         hash_group_to_size = np.empty([num_groups], dtype="int64")
#         hash_group_to_num_rows = np.empty([num_groups], dtype="int64")
#
#         for hb_index, obj in enumerate(delta_file_envelope_groups):
#             if obj:
#                 hb_group = hb_index % num_groups
#                 if hb_group_to_object[hb_group] is None:
#                     hb_group_to_object[hb_group] = np.empty([num_buckets], dtype="object")
#                     hash_group_to_size[hb_group] = np.int64(0)
#                     hash_group_to_num_rows[hb_group] = np.int64(0)
#                 hb_group_to_object[hb_group][hb_index] = obj
#                 for dfe in obj:
#                     casted_dfe: DeltaFileEnvelope = dfe
#                     hash_group_to_size[hb_group] += casted_dfe.table_size_bytes
#                     hash_group_to_num_rows[hb_group] += casted_dfe.table_num_rows
#
#         for hb_group, obj in enumerate(hb_group_to_object):
#             if obj is None:
#                 continue
#             object_ref = object_store.put(obj)
#             # hash_bucket_group_to_obj_id_size_tuple[hb_group] = (
#             #     object_ref,
#             #     hash_group_to_size[hb_group],
#             #     hash_group_to_num_rows[hb_group],
#             # )
#             # del object_ref
#             hg_res = (hb_group, (
#                 object_ref,
#                 hash_group_to_size[hb_group],
#                 hash_group_to_num_rows[hb_group],
#             ))
#             _, close_latency = timed_invocation(object_store.close)
#             logger.info(f"Active connections to the object store closed in {close_latency}")
#             yield hg_res
#             while object_store.get(object_ref):
#                 time.sleep(3)
#
#     # hg_res = group_hash_bucket_indices(
#         #     hash_bucket_object_groups=delta_file_envelope_groups,
#         #     num_buckets=input.num_hash_buckets,
#         #     num_groups=input.num_hash_groups,
#         #     object_store=input.object_store,
#         # )
#         # yield hg_res
#         # hash_bucket_group_to_obj_id_size_tuple[hg_res[0]] = hg_res[1]
#         # peak_memory_usage_bytes = get_current_node_peak_memory_usage_in_bytes()
#         # return HashBucketResult(
#         #     hash_bucket_group_to_obj_id_size_tuple,
#         #     np.int64(total_size_bytes),
#         #     np.int64(total_record_count),
#         #     np.double(peak_memory_usage_bytes),
#         #     np.double(0.0),
#         #     np.double(time.time()),
#         # )


@ray.remote(num_returns="streaming")
def hash_bucket(input: HashBucketInput) -> HashBucketResult:

    logger.info(f"Starting hash bucket task...")
    task_id = get_current_ray_task_id()
    worker_id = get_current_ray_worker_id()
    with memray.Tracker(
        f"hash_bucket_{worker_id}_{task_id}.bin"
    ) if input.enable_profiler else nullcontext():
        (
            delta_file_envelope_groups,
            total_record_count,
            total_size_bytes,
        ) = _group_file_records_by_pk_hash_bucket(
            annotated_delta=input.annotated_delta,
            num_hash_buckets=input.num_hash_buckets,
            primary_keys=input.primary_keys,
            read_kwargs_provider=input.read_kwargs_provider,
            deltacat_storage=input.deltacat_storage,
            deltacat_storage_kwargs=input.deltacat_storage_kwargs,
        )
        hash_bucket_group_to_obj_id_size_tuple = np.empty(
            [input.num_hash_groups], dtype="object"
        )

        """
         This method persists all tables for a given hash bucket into the object store
         and returns the object references for each hash group.
         """

        num_groups = input.num_hash_groups
        object_store = input.object_store
        num_buckets = input.num_hash_buckets
        hash_bucket_group_to_obj_id_size_tuple = np.empty([num_groups], dtype="object")

        if delta_file_envelope_groups is None:
            return hash_bucket_group_to_obj_id_size_tuple

        hb_group_to_object = np.empty([num_groups], dtype="object")
        hash_group_to_size = np.empty([num_groups], dtype="int64")
        hash_group_to_num_rows = np.empty([num_groups], dtype="int64")

        for hb_index, obj in enumerate(delta_file_envelope_groups):
            if obj:
                hb_group = hb_index % num_groups
                if hb_group_to_object[hb_group] is None:
                    hb_group_to_object[hb_group] = np.empty(
                        [num_buckets], dtype="object"
                    )
                    hash_group_to_size[hb_group] = np.int64(0)
                    hash_group_to_num_rows[hb_group] = np.int64(0)
                hb_group_to_object[hb_group][hb_index] = obj
                for dfe in obj:
                    casted_dfe: DeltaFileEnvelope = dfe
                    hash_group_to_size[hb_group] += casted_dfe.table_size_bytes
                    hash_group_to_num_rows[hb_group] += casted_dfe.table_num_rows

        print(f"hash_group_to_size:{hash_group_to_size}")
        logger.info(f"hash_group_to_size:{hash_group_to_size}")
        for hb_group, obj in enumerate(hb_group_to_object):
            if obj is None:
                hg_res = None
            else:
                # before_hb_stats = object_store.stats("10.0.89.221")
                # print(f"Before_hb_put_object_store_stats:{before_hb_stats}")
                # logger.info(f"Before_hb_put_object_store_stats:{before_hb_stats}")
                object_ref = object_store.put_many(obj)
                # after_hb_stats = object_store.stats("10.0.89.221")
                # print(f"After_hb_put_object_store_stats:{after_hb_stats}")
                # logger.info(f"After_hb_put_object_store_stats:{after_hb_stats}")
                # hash_bucket_group_to_obj_id_size_tuple[hb_group] = (
                #     object_ref,
                #     hash_group_to_size[hb_group],
                #     hash_group_to_num_rows[hb_group],
                # )
                # del object_ref
                hg_res = (
                    hb_group,
                    (
                        object_ref,
                        hash_group_to_size[hb_group],
                        hash_group_to_num_rows[hb_group],
                    ),
                )
            _, close_latency = timed_invocation(object_store.close)
            logger.info(
                f"Active connections to the object store closed in {close_latency}"
            )
            yield hg_res
            while not object_store.check_get_many_exist(object_ref):
                print(f"Waiting for object ref to be deleted")
                time.sleep(2)

    # hg_res = group_hash_bucket_indices(
    #     hash_bucket_object_groups=delta_file_envelope_groups,
    #     num_buckets=input.num_hash_buckets,
    #     num_groups=input.num_hash_groups,
    #     object_store=input.object_store,
    # )
    # yield hg_res
    # hash_bucket_group_to_obj_id_size_tuple[hg_res[0]] = hg_res[1]
    # peak_memory_usage_bytes = get_current_node_peak_memory_usage_in_bytes()
    # return HashBucketResult(
    #     hash_bucket_group_to_obj_id_size_tuple,
    #     np.int64(total_size_bytes),
    #     np.int64(total_record_count),
    #     np.double(peak_memory_usage_bytes),
    #     np.double(0.0),
    #     np.double(time.time()),
    # )

    # emit_metrics_time = 0.0
    # if input.metrics_config:
    #     emit_result, latency = timed_invocation(
    #         func=emit_timer_metrics,
    #         metrics_name="hash_bucket",
    #         value=duration,
    #         metrics_config=input.metrics_config,
    #     )
    #     emit_metrics_time = latency
    #
    # logger.info(f"Finished hash bucket task...")
    # return HashBucketResult(
    #     hash_bucket_result[0],
    #     hash_bucket_result[1],
    #     hash_bucket_result[2],
    #     hash_bucket_result[3],
    #     np.double(emit_metrics_time),
    #     hash_bucket_result[5],
    # )
