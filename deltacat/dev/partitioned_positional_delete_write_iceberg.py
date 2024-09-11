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
        NestedField(field_id=1, name="pk", field_type=StringType(), required=False),
        NestedField(field_id=2, name="bucket", field_type=LongType(), required=False),
        NestedField(field_id=3, name="random", field_type=BooleanType(), required=False),
        NestedField(field_id=4, name='file_path', field_type=StringType(), required=False),
        NestedField(field_id=6, name="pos", field_type=LongType(), require=False),
        schema_id=1
    )

def get_partition_spec():
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import BucketTransform, IdentityTransform
    NUM_OF_BUCKETS = 3
    partition_field_identity = PartitionField(
        source_id=1, field_id=101, transform=IdentityTransform(), name="pk"
    )
    partition_field_bucket = PartitionField(
        source_id=2, field_id=102, transform=BucketTransform(num_buckets=NUM_OF_BUCKETS), name="bucket"
    )
    partition_spec = PartitionSpec(
        partition_field_identity,
        partition_field_bucket
    )
    return partition_spec

def create_table_with_data_files_and_eqality_deletes():
    glue_catalog = get_glue_catalog()
    schema = get_table_schema()
    ps = get_partition_spec()
    # glue_catalog.create_namespace("testio")
    glue_catalog.create_table("testio.example6_partitioned", schema=schema, partition_spec=ps)
    loaded_table = glue_catalog.load_table("testio.example6_partitioned")

def load_table():
    glue_catalog = get_glue_catalog()
    loaded_table = glue_catalog.load_table("testio.example6_partitioned")
    return loaded_table

def get_s3_file_system():
    import pyarrow
    credential = get_credential()
    access_key_id = credential.access_key
    secret_access_key = credential.secret_key
    session_token = credential.token
    return pyarrow.fs.S3FileSystem(access_key=access_key_id, secret_key=secret_access_key, session_token=session_token)

def write_delete_table(tmp_path: str) -> str:
    import pyarrow.parquet as pq
    uuid_path = uuid.uuid4()
    deletes_file_path = f"{tmp_path}/deletes_{uuid_path}.parquet"
    # Note: The following path should reference correct data file path to make sure positional delete are correctly applied
    # Hardcoded file path for quick POC purpose
    path = "s3://metadata-py4j-zyiqin1/data_1318c33d-fb18-4ea2-83c1-772b60595f22.parquet"
    table = pa.table({"file_path": [path, path, path], "pos": [1, 2, 4]})
    file_system = get_s3_file_system()
    pq.write_table(table, deletes_file_path, filesystem=file_system)
    file_size_in_bytes = table.nbytes
    return build_delete_data_file(f"s3://{deletes_file_path}")

def write_data_table(tmp_path: str) -> str:
    import pyarrow.parquet as pq
    uuid_path = uuid.uuid4()
    deletes_file_path = f"{tmp_path}/data_{uuid_path}.parquet"
    table = pa.table({"pk": ["111", "222", "333"], "bucket": [1, 1, 1]})
    file_system = get_s3_file_system()
    pq.write_table(table, deletes_file_path, filesystem=file_system)
    file_size_in_bytes = table.nbytes
    return build_delete_data_file(f"s3://{deletes_file_path}")

def build_delete_data_file(file_path):
    from pyiceberg.manifest import DataFile, DataFileContent
    from pyiceberg.manifest import FileFormat
    print(f"build_delete_file_path:{file_path}")
    return file_path

def commit_delete_to_table():
    delete_s3_url = "metadata-py4j-zyiqin1"
    data_files = [write_delete_table(delete_s3_url)]
    table = load_table()
    add_delete_files(table=table, file_paths=data_files)

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


def commit_data_to_table():
    delete_s3_url = "metadata-py4j-zyiqin1"
    data_files = [write_data_table(delete_s3_url)]
    table = load_table()
    add_data_files(table=table, file_paths=data_files)

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
            partition=Record(**{"pk": "111", "bucket":1}),
            file_size_in_bytes=len(input_file),
            sort_order_id=None,
            spec_id=table_metadata.default_spec_id,
            equality_ids=None,
            key_metadata=None,
            **statistics.to_serialized_dict(),
        )

        yield data_file

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
        pv = Record(**{"pk": "111", "bucket":1})
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


def add_delete_files(table, file_paths):
    with table.transaction() as tx:
        if table.metadata.name_mapping() is None:
            table.set_properties(**{
                "schema.name-mapping.default": table.table_metadata.schema().name_mapping.model_dump_json()
            })
        with tx.update_snapshot().fast_append() as update_snapshot:
            data_files = parquet_files_to_positional_delete_files(
                table_metadata=table.metadata, file_paths=file_paths, io=table.io
            )
            for data_file in data_files:
                update_snapshot.append_data_file(data_file)

def add_data_files(table, file_paths):
    with table.transaction() as tx:
        if table.metadata.name_mapping() is None:
            tx.set_properties(**{
                "schema.name-mapping.default": table.metadata.schema().name_mapping.model_dump_json()
            })
        with tx.update_snapshot().fast_append() as update_snapshot:
            data_files = parquet_files_to_data_files(
                table_metadata=table.metadata, file_paths=file_paths, io=table.io
            )
            for data_file in data_files:
                update_snapshot.append_data_file(data_file)


def scan_table(table):
    print(table.scan().to_arrow())

# create_table_with_data_files_and_eqality_deletes()
table = load_table()
# commit_data_to_table(table)
commit_delete_to_table(table)
table = load_table()
scan_table(table)
print(table.inspect.files())