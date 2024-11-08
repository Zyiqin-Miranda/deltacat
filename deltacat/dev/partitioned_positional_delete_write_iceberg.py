import pyarrow as pa
import uuid
import boto3


from deltacat.utils.pyarrow import s3_file_to_table
from typing import Optional

def get_s3_path(bucket_name: str, database_name: Optional[str] = None, table_name: Optional[str] = None) -> str:
    result_path = f"s3://{bucket_name}"
    if database_name is not None:
        result_path += f"/{database_name}.db"

    if table_name is not None:
        result_path += f"/{table_name}"
    return result_path

def get_bucket_name():
    return "metadata-py4j-zyiqin1"

def get_credential():
    boto3_session = boto3.Session()
    credentials = boto3_session.get_credentials()
    return credentials

def get_glue_catalog():
    from pyiceberg.catalog.glue import GLUE_CATALOG_ENDPOINT, GlueCatalog
    from pyiceberg.catalog import load_catalog
    credential = get_credential()
    access_key_id = credential.access_key
    secret_access_key = credential.secret_key
    session_token = credential.token
    print(f"session_token: {session_token}")
    s3_path = get_s3_path(get_bucket_name())
    glue_catalog = load_catalog("glue", **{"warehouse": s3_path,
                    "type": "glue",
                    "aws_access_key_id": access_key_id,
                    "aws_secret_access_key": secret_access_key,
                    "aws_session_token": session_token,
                    "region_name": "us-east-1",
                    "s3.access-key-id": access_key_id,
                    "s3.secret-access-key": secret_access_key,
                    "s3.session-token": session_token,
                    "s3.region": "us-east-1"})

    return glue_catalog

def get_table_schema():
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType, DoubleType, DecimalType, IntegerType, BooleanType, LongType
    return Schema(
        NestedField(field_id=1, name="partitionkey", field_type=StringType(), required=False),
        NestedField(field_id=2, name="bucket", field_type=LongType(), required=False),
        NestedField(field_id=3, name="primarykey", field_type=StringType(), required=False),
        NestedField(field_id=4, name='file_path', field_type=StringType(), required=False),
        NestedField(field_id=6, name="pos", field_type=LongType(), require=False),
        schema_id=1
    )

def get_partition_spec():
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import BucketTransform, IdentityTransform
    NUM_OF_BUCKETS = 3
    partition_field_identity = PartitionField(
        source_id=1, field_id=101, transform=IdentityTransform(), name="partitionkey"
    )
    partition_field_bucket = PartitionField(
        source_id=2, field_id=102, transform=BucketTransform(num_buckets=NUM_OF_BUCKETS), name="primarykey"
    )
    partition_spec = PartitionSpec(
        partition_field_identity,
        partition_field_bucket
    )
    return partition_spec

def create_table_with_data_files_and_eqality_deletes(table_version):
    glue_catalog = get_glue_catalog()
    schema = get_table_schema()
    ps = get_partition_spec()
    # glue_catalog.create_namespace("testio")
    glue_catalog.create_table(f"testio.example_{table_version}_partitioned", schema=schema, partition_spec=ps)
    loaded_table = glue_catalog.load_table(f"testio.example_{table_version}_partitioned")

def load_table(table_version):
    glue_catalog = get_glue_catalog()
    loaded_table = glue_catalog.load_table(f"testio.example_{table_version}_partitioned")
    return loaded_table

def get_s3_file_system():
    import pyarrow
    credential = get_credential()
    access_key_id = credential.access_key
    secret_access_key = credential.secret_key
    session_token = credential.token
    return pyarrow.fs.S3FileSystem(access_key=access_key_id, secret_key=secret_access_key, session_token=session_token)

def write_delete_table(tmp_path: str, data_file_path) -> str:
    import pyarrow.parquet as pq
    uuid_path = uuid.uuid4()
    deletes_file_path = f"{tmp_path}/deletes_{uuid_path}.parquet"
    # Note: The following path should reference correct data file path to make sure positional delete are correctly applied
    # Hardcoded file path for quick POC purpose
    path = data_file_path
    # path = "s3://metadata-py4j-zyiqin1/data_a4f15d4a-20f6-4253-9926-d01c2cfbf884.parquet"
    table = pa.table({"file_path": [path, path, path], "pos": [0,1,2]})
    file_system = get_s3_file_system()
    pq.write_table(table, deletes_file_path, filesystem=file_system)
    file_size_in_bytes = table.nbytes
    return build_delete_data_file(f"s3://{deletes_file_path}")

def write_data_table(tmp_path: str) -> str:
    import pyarrow.parquet as pq
    uuid_path = uuid.uuid4()
    deletes_file_path = f"{tmp_path}/data_{uuid_path}.parquet"
    table = pa.table({"partitionkey": ["1", "1", "1"], "bucket": [1, 1, 1], "primarykey":[]})
    file_system = get_s3_file_system()
    pq.write_table(table, deletes_file_path, filesystem=file_system)
    file_size_in_bytes = table.nbytes
    return build_delete_data_file(f"s3://{deletes_file_path}")

def build_delete_data_file(file_path):
    from pyiceberg.manifest import DataFile, DataFileContent
    from pyiceberg.manifest import FileFormat
    print(f"build_delete_file_path:{file_path}")
    return file_path

def commit_delete_to_table(table, data_file_paths):
    delete_s3_url = "metadata-py4j-zyiqin1"
    data_files = [write_delete_table(delete_s3_url, data_file_paths)]
    add_delete_files(file_paths=data_files)

#     How daft writes Iceberg df, but append_data_file assumes file are DATA only, refer:
#     https://github.com/apache/iceberg-python/blob/052a9cdab1078b15754a519d9d20f8767b3c59cb/pyiceberg/io/pyarrow.py#L2520
#     tx = table.transaction()
#     update_snapshot = tx.update_snapshot()
#     append_method = update_snapshot.fast_append
#
#     with append_method() as append_files:
#         for data_file in data_files:
#             append_files.append_data_file(data_file)
#
#     new_table = tx.commit_transaction()


def commit_data_to_table(table):
    delete_s3_url = "metadata-py4j-zyiqin1"
    data_files = [write_data_table(delete_s3_url)]
    add_data_files(file_paths=data_files)
    return data_files

def commit_equality_delete_to_table(table):
    delete_s3_url = "metadata-py4j-zyiqin1"
    data_files = [write_equality_data_table(delete_s3_url)]
    add_equality_data_files(file_paths=data_files)
    return data_files

def write_equality_data_table(tmp_path: str):
    import pyarrow.parquet as pq
    uuid_path = uuid.uuid4()
    deletes_file_path = f"{tmp_path}/data_{uuid_path}.parquet"
    table = pa.table({"pk": ["111", "222", "333"], "bucket": [1, 1, 1]})
    file_system = get_s3_file_system()
    pq.write_table(table, deletes_file_path, filesystem=file_system)
    file_size_in_bytes = table.nbytes
    return build_delete_data_file(f"s3://{deletes_file_path}")

def scan_new_table(table):
    task = table.scan(snapshot_id=table.snapshot_id).plan_files()
    file = task.file
    path = file.file_path
    record_count = file.record_count
    file_format = file.file_format
    iceberg_delete_files = [f.file_path for f in task.delete_files]
    # Call s3_file_to_table() to download file into df
    # s3_file_to_table()


# commit to s3
def parquet_files_to_positional_delete_files(io, table_metadata, file_paths):
    from pyiceberg.io.pyarrow import (_check_pyarrow_schema_compatible, data_file_statistics_from_parquet_metadata,
                                      compute_statistics_plan, parquet_path_to_id_mapping)
    from pyiceberg.manifest import (
        DataFile,
        DataFileContent,
        FileFormat,
    )
    import pyarrow.parquet as pq
    from pyiceberg.typedef import Record
    for file_path in file_paths:
        input_file = io.new_input(file_path)
        with input_file.open() as input_stream:
            parquet_metadata = pq.read_metadata(input_stream)

        schema = table_metadata.schema()
        _check_pyarrow_schema_compatible(schema, parquet_metadata.schema.to_arrow_schema())

        statistics = data_file_statistics_from_parquet_metadata(
            parquet_metadata=parquet_metadata,
            stats_columns=compute_statistics_plan(schema, table_metadata.properties),
            parquet_column_mapping=parquet_path_to_id_mapping(schema),
        )
        data_file = DataFile(
            content=DataFileContent.POSITION_DELETES,
            file_path=file_path,
            file_format=FileFormat.PARQUET,
            partition=Record(pk="111", bucket=1),
            # partition=Record(**{"pk": "111", "bucket": 2}),
            file_size_in_bytes=len(input_file),
            sort_order_id=None,
            spec_id=table_metadata.default_spec_id,
            equality_ids=None,
            key_metadata=None,
            **statistics.to_serialized_dict(),
        )

        yield data_file

def produce_pos_delete_file(io, table_metadata, file_path):
    from pyiceberg.io.pyarrow import (_check_pyarrow_schema_compatible, data_file_statistics_from_parquet_metadata,
                                      compute_statistics_plan, parquet_path_to_id_mapping)
    from pyiceberg.manifest import (
        DataFile,
        DataFileContent,
        FileFormat,
    )
    import pyarrow.parquet as pq
    from pyiceberg.typedef import Record
    input_file = io.new_input(file_path)
    with input_file.open() as input_stream:
        parquet_metadata = pq.read_metadata(input_stream)

    schema = table_metadata.schema()
    _check_pyarrow_schema_compatible(schema, parquet_metadata.schema.to_arrow_schema())

    statistics = data_file_statistics_from_parquet_metadata(
        parquet_metadata=parquet_metadata,
        stats_columns=compute_statistics_plan(schema, table_metadata.properties),
        parquet_column_mapping=parquet_path_to_id_mapping(schema),
    )
    data_file = DataFile(
        content=DataFileContent.POSITION_DELETES,
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        partition=Record(pk="111", bucket=1),
        # partition=Record(**{"pk": "111", "bucket": 2}),
        file_size_in_bytes=len(input_file),
        sort_order_id=None,
        spec_id=table_metadata.default_spec_id,
        equality_ids=None,
        key_metadata=None,
        **statistics.to_serialized_dict(),
    )

    return data_file
def parquet_files_to_data_files(io, table_metadata, file_paths):
    from pyiceberg.io.pyarrow import (_check_pyarrow_schema_compatible, data_file_statistics_from_parquet_metadata,
                                      compute_statistics_plan, parquet_path_to_id_mapping)
    from pyiceberg.manifest import (
        DataFile,
        DataFileContent,
        FileFormat,
    )
    from pyiceberg.schema import Schema
    from pyiceberg.types import StructType, NestedField, StringType, DoubleType, DecimalType, IntegerType, BooleanType, LongType
    import pyarrow.parquet as pq
    from pyiceberg.typedef import Record
    for file_path in file_paths:
        input_file = io.new_input(file_path)
        with input_file.open() as input_stream:
            parquet_metadata = pq.read_metadata(input_stream)

        schema = table_metadata.schema()
        _check_pyarrow_schema_compatible(schema, parquet_metadata.schema.to_arrow_schema())

        statistics = data_file_statistics_from_parquet_metadata(
            parquet_metadata=parquet_metadata,
            stats_columns=compute_statistics_plan(schema, table_metadata.properties),
            parquet_column_mapping=parquet_path_to_id_mapping(schema),
        )
        # pv = Record(**{"pk": "222", "bucket": 1})
        pv = Record(**{"pk": "111", "bucket": 1})
        data_file = DataFile(
            content=DataFileContent.DATA,
            file_path=file_path,
            file_format=FileFormat.PARQUET,
            partition=pv,
            file_size_in_bytes=len(input_file),
            sort_order_id=None,
            spec_id=table_metadata.default_spec_id,
            equality_ids=None,
            key_metadata=None,
            **statistics.to_serialized_dict(),
        )

        yield data_file


def add_delete_files(file_paths):
    table = load_table(TABLE_VERSION)

    tx = table.transaction()
    update_snapshot = tx.update_snapshot()
    append_method = update_snapshot.fast_append
    data_files = parquet_files_to_positional_delete_files(
        table_metadata=table.metadata, file_paths=file_paths, io=table.io
    )
    with append_method() as append_files:
        for data_file in data_files:
            append_files.append_data_file(data_file)

    new_table = tx.commit_transaction()

    # with table.transaction() as tx:
    #     if table.metadata.name_mapping() is None:
    #         table.set_properties(**{
    #             "schema.name-mapping.default": table.table_metadata.schema().name_mapping.model_dump_json()
    #         })
    #     with tx.update_snapshot().fast_append() as update_snapshot:
    #         data_files = parquet_files_to_positional_delete_files(
    #             table_metadata=table.metadata, file_paths=file_paths, io=table.io
    #         )
    #         for data_file in data_files:
    #             update_snapshot.append_data_file(data_file)
    #         update_snapshot.commit()
    #     tx.commit_transaction()

def yield_position_delete_file(data_file_paths):
    delete_s3_url = "metadata-py4j-zyiqin1"
    data_files = [write_delete_table(delete_s3_url, data_file_paths)]
    pos_delete_file = produce_pos_delete_file(
                table_metadata=table.metadata, file_path=data_files[0], io=table.io
            )
    return pos_delete_file

def add_data_files(file_paths):
    table = load_table(TABLE_VERSION)

    tx = table.transaction()
    update_snapshot = tx.update_snapshot()
    append_method = update_snapshot.fast_append
    data_files = parquet_files_to_data_files(
        table_metadata=table.metadata, file_paths=file_paths, io=table.io
    )
    with append_method() as append_files:
        for data_file in data_files:
            append_files.append_data_file(data_file)

    new_table = tx.commit_transaction()
    # table = load_table(TABLE_VERSION)
    # table.refresh()
    # with table.transaction() as tx:
    #     if table.metadata.name_mapping() is None:
    #         tx.set_properties(**{
    #             "schema.name-mapping.default": table.metadata.schema().name_mapping.model_dump_json()
    #         })
    #     with tx.update_snapshot().fast_append() as update_snapshot:
    #         data_files = parquet_files_to_data_files(
    #             table_metadata=table.metadata, file_paths=file_paths, io=table.io
    #         )
    #         for data_file in data_files:
    #             update_snapshot.append_data_file(data_file)
    #         update_snapshot.commit()
    #     tx.commit_transaction()

def add_equality_data_files(file_paths):
    table = load_table(TABLE_VERSION)
    with table.transaction() as tx:
        if table.metadata.name_mapping() is None:
            tx.set_properties(**{
                "schema.name-mapping.default": table.metadata.schema().name_mapping.model_dump_json()
            })
        with tx.update_snapshot().fast_append() as update_snapshot:
            data_files = parquet_files_to_equality_data_files(
                table_metadata=table.metadata, file_paths=file_paths, io=table.io
            )
            for data_file in data_files:
                update_snapshot.append_data_file(data_file)
            update_snapshot.commit()
        tx.commit_transaction()

def parquet_files_to_equality_data_files(io, table_metadata, file_paths):
    from pyiceberg.io.pyarrow import (_check_pyarrow_schema_compatible, data_file_statistics_from_parquet_metadata,
                                      compute_statistics_plan, parquet_path_to_id_mapping)
    from pyiceberg.manifest import (
        DataFile,
        DataFileContent,
        FileFormat,
    )
    import pyarrow.parquet as pq
    from pyiceberg.typedef import Record
    for file_path in file_paths:
        input_file = io.new_input(file_path)
        with input_file.open() as input_stream:
            parquet_metadata = pq.read_metadata(input_stream)

        schema = table_metadata.schema()
        _check_pyarrow_schema_compatible(schema, parquet_metadata.schema.to_arrow_schema())

        statistics = data_file_statistics_from_parquet_metadata(
            parquet_metadata=parquet_metadata,
            stats_columns=compute_statistics_plan(schema, table_metadata.properties),
            parquet_column_mapping=parquet_path_to_id_mapping(schema),
        )
        data_file = DataFile(
            content=DataFileContent.EQUALITY_DELETES,
            file_path=file_path,
            file_format=FileFormat.PARQUET,
            partition=Record(pk="111", bucket=1),
            # partition=Record(**{"pk": "111", "bucket": 2}),
            file_size_in_bytes=len(input_file),
            sort_order_id=None,
            spec_id=table_metadata.default_spec_id,
            equality_ids=None,
            key_metadata=None,
            **statistics.to_serialized_dict(),
        )

        yield data_file
def scan_table(table):
    print(f"scan_table result:{table.scan().to_arrow().to_pydict()}")

def fetch_all_equality_delete_files(table):
    # step 1: filter manifests using partition summaries
    # the filter depends on the partition spec used to write the manifest file, so create a cache of filters for each spec id
    from pyiceberg.typedef import (
        EMPTY_DICT,
        IcebergBaseModel,
        IcebergRootModel,
        Identifier,
        KeyDefaultDict,
    )

    data_scan = table.scan()
    snapshot = data_scan.snapshot()
    if not snapshot:
        return iter([])
    manifest_evaluators = KeyDefaultDict(data_scan._build_manifest_evaluator)

    manifests = [
        manifest_file
        for manifest_file in snapshot.manifests(data_scan.io)
        if manifest_evaluators[manifest_file.partition_spec_id](manifest_file)
    ]

    # step 2: filter the data files in each manifest
    # this filter depends on the partition spec used to write the manifest file
    from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator
    from pyiceberg.types import (
        strtobool,
    )
    from pyiceberg.table import _min_sequence_number, _open_manifest
    from pyiceberg.utils.concurrent import ExecutorFactory
    from itertools import chain
    from pyiceberg.manifest import DataFileContent

    partition_evaluators = KeyDefaultDict(data_scan._build_partition_evaluator)
    metrics_evaluator = _InclusiveMetricsEvaluator(
        data_scan.table_metadata.schema(),
        data_scan.row_filter,
        data_scan.case_sensitive,
        strtobool(data_scan.options.get("include_empty_files", "false")),
    ).eval

    min_sequence_number = _min_sequence_number(manifests)

    equality_data_entries = []
    # positional_delete_entries = SortedList(key=lambda entry: entry.sequence_number or INITIAL_SEQUENCE_NUMBER)

    executor = ExecutorFactory.get_or_create()
    for manifest_entry in chain(
            *executor.map(
                lambda args: _open_manifest(*args),
                [
                    (
                            data_scan.io,
                            manifest,
                            partition_evaluators[manifest.partition_spec_id],
                            metrics_evaluator,
                    )
                    for manifest in manifests
                    if data_scan._check_sequence_number(min_sequence_number, manifest)
                ],
            )
    ):
        data_file = manifest_entry.data_file
        if data_file.content == DataFileContent.EQUALITY_DELETES:
            equality_data_entries.append(data_file)
    return equality_data_entries
        # if data_file.content == DataFileContent.DATA:
        #     data_entries.append(manifest_entry)
        # elif data_file.content == DataFileContent.POSITION_DELETES:
        #     positional_delete_entries.add(manifest_entry)
        # elif data_file.content == DataFileContent.EQUALITY_DELETES:
        #     raise ValueError(
        #         "PyIceberg does not yet support equality deletes: https://github.com/apache/iceberg/issues/6568")
        # else:
        #     raise ValueError(f"Unknown DataFileContent ({data_file.content}): {manifest_entry}")

# def plan_files(self) -> Iterable[FileScanTask]:
#     """Plans the relevant files by filtering on the PartitionSpecs.
#
#     Returns:
#         List of FileScanTasks that contain both data and delete files.
#     """
#     snapshot = self.snapshot()
#     if not snapshot:
#         return iter([])
#
#     # step 1: filter manifests using partition summaries
#     # the filter depends on the partition spec used to write the manifest file, so create a cache of filters for each spec id
#
#     manifest_evaluators: Dict[int, Callable[[ManifestFile], bool]] = KeyDefaultDict(self._build_manifest_evaluator)
#
#     manifests = [
#         manifest_file
#         for manifest_file in snapshot.manifests(self.io)
#         if manifest_evaluators[manifest_file.partition_spec_id](manifest_file)
#     ]
#
#     # step 2: filter the data files in each manifest
#     # this filter depends on the partition spec used to write the manifest file
#
#     partition_evaluators: Dict[int, Callable[[DataFile], bool]] = KeyDefaultDict(self._build_partition_evaluator)
#     metrics_evaluator = _InclusiveMetricsEvaluator(
#         self.table_metadata.schema(),
#         self.row_filter,
#         self.case_sensitive,
#         strtobool(self.options.get("include_empty_files", "false")),
#     ).eval
#
#     min_sequence_number = _min_sequence_number(manifests)
#
#     data_entries: List[ManifestEntry] = []
#     positional_delete_entries = SortedList(key=lambda entry: entry.sequence_number or INITIAL_SEQUENCE_NUMBER)
#
#     executor = ExecutorFactory.get_or_create()
#     for manifest_entry in chain(
#             *executor.map(
#                 lambda args: _open_manifest(*args),
#                 [
#                     (
#                             self.io,
#                             manifest,
#                             partition_evaluators[manifest.partition_spec_id],
#                             metrics_evaluator,
#                     )
#                     for manifest in manifests
#                     if self._check_sequence_number(min_sequence_number, manifest)
#                 ],
#             )
#     ):
#         data_file = manifest_entry.data_file
#         if data_file.content == DataFileContent.DATA:
#             data_entries.append(manifest_entry)
#         elif data_file.content == DataFileContent.POSITION_DELETES:
#             positional_delete_entries.add(manifest_entry)
#         elif data_file.content == DataFileContent.EQUALITY_DELETES:
#             raise ValueError(
#                 "PyIceberg does not yet support equality deletes: https://github.com/apache/iceberg/issues/6568")
#         else:
#             raise ValueError(f"Unknown DataFileContent ({data_file.content}): {manifest_entry}")
#
#     return [
#         FileScanTask(
#             data_entry.data_file,
#             delete_files=_match_deletes_to_data_file(
#                 data_entry,
#                 positional_delete_entries,
#             ),
#         )
#         for data_entry in data_entries
#     ]


TABLE_VERSION = "18"
create_table_with_data_files_and_eqality_deletes(TABLE_VERSION)
table = load_table(TABLE_VERSION)
# data_file_paths = commit_equality_delete_to_table(table)
# table = load_table(table_version)
commit_data_to_table(table)
# table = load_table(table_version)
# eq_deletes = fetch_all_equality_delete_files(table)
# for field_name, v in eq_deletes[0].partition.items():
#     print(f"field_name:{field_name}, v:{v}")
# print(f"eq_deletes:{eq_deletes[0].partition}")
# # scan_table(table)
# print(table.metadata)

# data_file_paths = ["s3://metadata-py4j-zyiqin1/data_574c5c5e-3528-4096-80e4-ff8e4244e0d5.parquet"]
data_file_paths = ["s3://metadata-py4j-zyiqin1/data_49dd9e39-b09d-4471-a7cc-729cef7e8fc5.parquet"]
# commit_delete_to_table(table, data_file_paths)
# table = load_table(TABLE_VERSION)
# scan_table(table)
# print(table.inspect.files())
# print(table.metadata)
# table = load_table(table_version)

# from pyiceberg.expressions import GreaterThanOrEqual, And, EqualTo
# # row_filter = And(EqualTo("pk", '111'), EqualTo("bucket", 1))
# row_filter = EqualTo("pk", "111")
# file_scan_tasks = table.scan(row_filter=row_filter).plan_files()
file_scan_tasks = table.scan().plan_files()

delete_file_set = set()
data_file_list = []
for st in file_scan_tasks:
    print("FileScanTask:")
    print(f"data_file_path:{st.file.file_path}")
    data_file_list.append(st.file)
    for de in st.delete_files:
        print(f"delete_file_path:{de.file_path}")
        print(f"delete_file_partition:{de.partition}")
        delete_file_set.add(de)
    print(f"data_file_partition:{st.file.partition}")

table = load_table(TABLE_VERSION)
scan_table(table)

# print(table.metadata)
# Create a clean table version
# Only data files, show table
# Commit same data file, show delete correctly applies
# Show snapshot history
# # Delete all records in one file and demo
# equality_delete_file = fetch_all_equality_delete_files(table)
# print(f"all_equality_delete_file:{equality_delete_file}")
# data_file_path ="s3://metadata-py4j-zyiqin1/data_8998947d-f296-434e-827f-0121c0ab6c7f.parquet"

# data_file_path = "s3://metadata-py4j-zyiqin1/test-sequence-1.parquet"
# table = load_table(TABLE_VERSION)
# data_file_path = data_file_list[1].file_path
# position_delete_file = yield_position_delete_file(data_file_path)
# previous_delete_file = delete_file_set.pop()
#
#
# print(f"previous_delete_file:{previous_delete_file.file_path}")
# previous_delete_file = data_file_list[1]
# tx = table.transaction()
# snapshot_properties = {}
# commit_uuid = uuid.uuid4()
# with tx.update_snapshot(snapshot_properties=snapshot_properties).replace(
#         commit_uuid=commit_uuid, using_starting_sequence=True
# ) as replace_snapshot:
#     replace_snapshot.append_data_file(position_delete_file)
#     replace_snapshot.delete_data_file(previous_delete_file)
#     replace_snapshot._commit()
# new_table = tx.commit_transaction()

    # for original_data_file, replaced_data_files in replaced_files:
    #     replace_snapshot.delete_data_file(original_data_file)
    #     for replaced_data_file in replaced_data_files:
    #         replace_snapshot.append_data_file(replaced_data_file)

# with tx.update_snapshot(snapshot_properties=snapshot_properties).overwrite(
#         commit_uuid=commit_uuid
# ) as overwrite_snapshot:
#     overwrite_snapshot.append_data_file(position_delete_file)
#     overwrite_snapshot.delete_data_file(equality_delete_file[0])
#     new_table = tx.commit_transaction()

# print(new_table.metadata)
# table = load_table(TABLE_VERSION)
# scan_table(table)
# print(table.metadata)
print(table.inspect.entries())