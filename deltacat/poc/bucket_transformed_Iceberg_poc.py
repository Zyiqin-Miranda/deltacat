from decimal import Decimal
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.io.pyarrow import (
    schema_to_pyarrow,
)
from pyiceberg.types import NestedField, StringType, DoubleType, DecimalType
from pyiceberg.partitioning import PartitionSpec, PartitionField

from pyiceberg.transforms import BucketTransform
from collections import defaultdict, OrderedDict
import mmh3
import daft
import decimal
import random
import numpy

# STEP 1: Define partition spec
# For this experiment, we have 5000 hash buckets, primary key is city.
# We utilize Iceberg bucket transform, which aligns with current Ray compactor's hash partitioned writes.
# Icerberg bucket transform: https://iceberg.apache.org/spec/#bucket-transform-details

NUM_OF_BUCKETS = 5000
def bucket_N(x, num_of_buckets):
    return (mmh3.hash(x) & 2147483647) % num_of_buckets

iceberg_schema = Schema(
    NestedField(1, "city", StringType(), required=True),
    NestedField(2, "lat", DecimalType(38, 10), required=False),
    NestedField(3, "long", DecimalType(38, 10), required=False),
)

partition_field = PartitionField(
        source_id=1, field_id=101, transform=BucketTransform(num_buckets=NUM_OF_BUCKETS), name="city"
    )
partition_spec = PartitionSpec(
   partition_field
)


# STEP 2: General 100 million random record with distinct primary keys
def _generate_random_record(count):
    record_list = []
    for i in range(count):
        record = {"city": str(i),
                  "lat": decimal.Decimal('%d.%d' % (random.randint(0, 180),random.randint(0, 180))),
                  "long": decimal.Decimal('%d.%d' % (random.randint(0, 180),random.randint(0, 180)))}
        record_list.append(record)
    return record_list

hashed_record_list = defaultdict(list)

record_list = _generate_random_record(100000000)

for record in record_list:
    bucket_idx = bucket_N(record["city"], NUM_OF_BUCKETS)
    hashed_record_list[bucket_idx].append(record)




# STEP 3: Hash partition records, for poc simplicity, each partition bucket written to 1 manifest entry. Commit delta to Cairns.
hashed_df = defaultdict()
for idx, records in hashed_record_list.items():
    hashed_df[idx] = pa.Table.from_pylist(records, schema_to_pyarrow(iceberg_schema))
df = pa.Table.from_pylist(record_list, schema_to_pyarrow(iceberg_schema))

# for key, dfs in hashed_df.items():
    # print(f"key:{key}, dfs:{dfs}")

sorted_hash_df = OrderedDict(sorted(hashed_df.items()))

# for key, dfs in sorted_hash_df.items():
#     print(f"key:{key}, dfs:{dfs}")

import sungate as sg
DS = sg.andes

PROVIDER_NAME = "bdt-ray-dev"
TABLE_NAME = "iceberg_hash_partitioned_poc1"
TABLE_VERSION = 1
stream = DS.get_stream(PROVIDER_NAME, TABLE_NAME, TABLE_VERSION)
staged_partition = DS.stage_partition(stream, [])
deltas = []
for idx, df in sorted_hash_df.items():
    delta = DS.stage_delta(df, staged_partition)
    deltas.append(delta)

from deltacat.storage import Delta
merged_delta = Delta.merge_deltas(
        deltas,
        stream_position=deltas[-1].stream_position,
    )

committed_delta = DS.commit_delta(merged_delta)
DS.commit_partition(staged_partition)



# STEP 4: Initialize Iceberg metadata, for the use of registering new snapshot and datafiles.
from pyiceberg.io.pyarrow import _ConvertToIceberg, visit_pyarrow
from pyiceberg.schema import Schema


# Note that this UUID is test table stream ID
UUID = "5a4e4558-b4a0-47aa-849b-00123aab067d"
METADATA_LOCATION = F"s3://cairns-metastore-prod/{UUID}"
UNIQUE_ICEBERG_TABLE_ID = UUID

from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.io import load_file_io
from pyiceberg.serializers import ToOutputFile
from pyiceberg.catalog import Catalog

# Ideally, we want to set this to correct metadata version if it already existed
# TODO: Change Icerberg metadata version to correct version
previous_metadata_version = 6

schema: Schema = visit_pyarrow(hashed_df[0].schema, _ConvertToIceberg())
#
table_metadata = new_table_metadata(location=METADATA_LOCATION,
                   schema=schema,
                   partition_spec=partition_spec,
                   sort_order=UNSORTED_SORT_ORDER,
                   properties={},
                   table_uuid=UNIQUE_ICEBERG_TABLE_ID)

metadata_location = Catalog._get_metadata_location(METADATA_LOCATION, previous_metadata_version)

print(f"first_metadata_location:{metadata_location}")

io = load_file_io(location=metadata_location)
ToOutputFile.table_metadata(table_metadata, io.new_output(metadata_location))

from pyiceberg.table import StaticTable


static_table = StaticTable.from_metadata(metadata_location)
print(static_table)


# STEP 5: Register partitioned data files with Iceberg table

# uploaded_file = DS.download_delta_manifest_entry(delta, 0)
# print(f"uploaded_file.schema:{uploaded_file.schema}")

from pyiceberg.io.pyarrow import (
        compute_statistics_plan,
        fill_parquet_file_metadata,
        parquet_path_to_id_mapping
)

from pyiceberg.manifest import DataFile, DataFileContent
from pyiceberg.manifest import FileFormat as IcebergFileFormat
from pyiceberg.typedef import Record

from deltacat.types.media import (
    ContentEncoding,
    ContentType,
    TableType,
)

# We can call list deltas here instead of committing a new delta for experiments with data files.
# input_deltas = DS.list_deltas(
#                 "bdt-ray-dev",
#                 "iceberg_hash_partitioned_poc1",
#                 [],
#                 1,
#                 equivalent_table_types=[],
#                 include_manifest=True,
#             ).read_page()[0]
#
# delta = input_deltas

delta = committed_delta
data_files = []

for entry_id, entry in enumerate(delta.manifest.entries):
    # just downloading ParquetFile metadata again
    # In production, we can avoid this re-read after some refactoring
    table = DS.download_delta_manifest_entry(delta, entry_id, table_type=TableType.PYARROW_PARQUET)
    metadata = table.metadata
    data_file = DataFile(
        content=DataFileContent.DATA,
        file_path=entry.uri,
        file_format=IcebergFileFormat.PARQUET,
        # Partition id is equal to manifest entry index in this case.
        # The assumption is that we sort the manifest entries already in STEP 3.
        partition=Record(city_bucket=entry_id),
        file_size_in_bytes=entry.meta.content_length,
        sort_order_id=None,
        # Just copy these from the table for now
        spec_id=static_table.spec().spec_id,
        equality_ids=None,
        key_metadata=None,
    )
    fill_parquet_file_metadata(
        data_file=data_file,
        parquet_metadata=metadata,
        stats_columns=compute_statistics_plan(schema, static_table.properties),
        parquet_column_mapping=parquet_path_to_id_mapping(schema),
    )
    data_files.append(data_file)


#  Step 6: Register data files in Step 5 with new snapshot and metadata version.
from pyiceberg.manifest import (
    write_manifest,
    ManifestEntryStatus,
    ManifestEntry,
    write_manifest_list,
)
import uuid

def _new_manifest_path(location: str, num: int, commit_uuid: uuid.UUID) -> str:
    return f'{location}/metadata/{commit_uuid}-m{num}.avro'

table_metadata = static_table.metadata
output_file_location = _new_manifest_path(location=table_metadata.location, num=0, commit_uuid=uuid.uuid4())

manifest_file = None
snapshot_id = static_table.new_snapshot_id()
output_file = static_table.io.new_output(output_file_location)

with write_manifest(
    format_version=static_table.format_version,
    spec=static_table.spec(),
    schema=static_table.schema(),
    output_file=output_file,
    snapshot_id=snapshot_id,
) as writer:
    for data_file in data_files:
        writer.add_entry(
            ManifestEntry(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=snapshot_id,
                data_sequence_number=None,
                file_sequence_number=None,
                data_file=data_file,
            )
        )

manifest_file = writer.to_manifest_file()
next_sequence_number = static_table.next_sequence_number()
import uuid

def _generate_manifest_list_path(location: str, snapshot_id: int, attempt: int, commit_uuid: uuid.UUID) -> str:
    # Mimics the behavior in Java:
    # https://github.com/apache/iceberg/blob/c862b9177af8e2d83122220764a056f3b96fd00c/core/src/main/java/org/apache/iceberg/SnapshotProducer.java#L491
    print(f'{location}/metadata/snap-{snapshot_id}-{attempt}-{commit_uuid}.avro')
    return f'{location}/metadata/snap-{snapshot_id}-{attempt}-{commit_uuid}.avro'


manifest_list_file_path = _generate_manifest_list_path(location=static_table.location(), snapshot_id=snapshot_id, attempt=0, commit_uuid=uuid.uuid4())
with write_manifest_list(
    format_version=static_table.metadata.format_version,
    output_file=static_table.io.new_output(manifest_list_file_path),
    snapshot_id=snapshot_id,
    parent_snapshot_id=None,
    sequence_number=next_sequence_number,
) as writer:
    writer.add_manifests([manifest_file])
from pyiceberg.table.snapshots import (
    Operation,
    Snapshot,
    SnapshotLogEntry,
    SnapshotSummaryCollector,
    Summary,
    update_snapshot_summaries,
)

def _summary() -> Summary:
    ssc = SnapshotSummaryCollector()

    for data_file in data_files:
        ssc.add_file(data_file=data_file)

    return update_snapshot_summaries(
        summary=Summary(operation=Operation.APPEND, **ssc.build()),
        previous_summary=None,
        truncate_full_table=False,
    )

snapshot = Snapshot(
            snapshot_id=snapshot_id,
            parent_snapshot_id=None,
            manifest_list=manifest_list_file_path,
            sequence_number=next_sequence_number,
            summary=_summary(),
            schema_id=static_table.schema().schema_id,
        )
from pyiceberg.table import Transaction, update_table_metadata

txn = Transaction(table=static_table)
txn.add_snapshot(snapshot)
txn.set_ref_snapshot(snapshot_id=snapshot.snapshot_id, parent_snapshot_id=None, ref_name="main", type="branch")
from pyiceberg.table import update_table_metadata

new_metadata = update_table_metadata(table_metadata, txn._updates)
new_metadata_version = previous_metadata_version + 1

metadata_location = Catalog._get_metadata_location(METADATA_LOCATION, new_metadata_version)
io = load_file_io(location=metadata_location)
ToOutputFile.table_metadata(new_metadata, io.new_output(metadata_location))
print(metadata_location)

# Read Iceberg table, will get permission denied when reading from Cairns
# from pyiceberg.table import StaticTable
#
# metadata_location = "s3://cairns-metastore-prod/5a4e4558-b4a0-47aa-849b-00123aab067d/metadata/00002-ddeb4d76-47ee-4b08-b1a1-5d05b2220f3c.metadata.json"
# static_table = StaticTable.from_metadata(metadata_location)
# print(len(static_table.scan().to_arrow()))
#
#
# manifest_entries = manifest.fetch_manifest_entry(PyArrowFileIO())
# manifest_entry = manifest_entries[0]
#
# import daft
#
# iceberg_df = daft.read_iceberg(static_table)
# # iceberg_df = iceberg_df.where(iceberg_df["city"] == 1)
# iceberg_df.show(10)

