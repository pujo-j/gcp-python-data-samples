import csv

import fastavro
import gcsfs
from dask.delayed import delayed
import dask.bag as db

source = "/jpujo-gcp-devzone-gcs-1/test/"
avro_path = "/jpujo-gcp-devzone-gcs-1/test/output/csv/dask/test"


# Read csv in a delayed function to parallelize load
@delayed
def read_csv(source, encoding="UTF-8", dialect="excel"):
    gcs = gcsfs.GCSFileSystem()
    with gcs.open(source, mode="r", newline='', encoding=encoding) as input_fd:
        reader = csv.DictReader(input_fd, dialect=dialect)
        for row in reader:
            yield row


def read_csvs(source):
    gcs = gcsfs.GCSFileSystem()
    return db.from_delayed([read_csv(x) for x in gcs.ls(source) if x.endswith(".csv")])


def csv_transcode(input):
    for row in input:
        yield {
            "Foo": int(row["Foo"]),
            "Bar": int(row["Bar"]),
            "Baz": row["Baz"],
        }


schema = {
    'name': 'record',
    'type': 'record',
    'fields': [
        {'name': 'Foo', 'type': 'long'},
        {'name': 'Bar', 'type': 'long'},
        {'name': 'Baz', 'type': 'string'},
    ],
}
parsed_schema = fastavro.parse_schema(schema)

if __name__ == '__main__':
    read_csvs(source).map_partitions(csv_transcode).to_avro("gcs:/" + avro_path + "_*.avro", parsed_schema)
