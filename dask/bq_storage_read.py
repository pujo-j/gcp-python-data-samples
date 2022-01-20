from dask.delayed import delayed
import dask.bag as db
import google.cloud.bigquery_storage_v1 as bqs

project = "jpujo-gcp-devzone"
dataset = "ds1"
table = "test"


@delayed
def load_stream(stream_name):
    client = bqs.BigQueryReadClient()
    reader = client.read_rows(stream_name)
    print(f"reading rows from {stream_name=}")
    for row in reader.rows():
        yield row


def load_table(project: str, dataset: str, table: str, partitions: int = 4):
    client = bqs.BigQueryReadClient()
    parent = f"projects/{project}"
    table_ref = f"projects/{project}/datasets/{dataset}/tables/{table}"
    request = bqs.types.ReadSession()
    request.table = table_ref
    request.data_format = bqs.types.DataFormat.AVRO
    session = client.create_read_session(
        parent=parent,
        read_session=request,
        max_stream_count=partitions,
    )
    return db.from_delayed([load_stream(s.name) for s in session.streams])


if __name__ == "__main__":
    bag = load_table(project, dataset, table)
    print(f"Loaded {bag.count().compute()} lines")
