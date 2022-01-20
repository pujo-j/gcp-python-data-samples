import fastavro
import gcsfs
import csv

from google.cloud import bigquery

source = "/jpujo-gcp-devzone-gcs-1/test/"
avro_path = "/jpujo-gcp-devzone-gcs-1/test/output/csv/test"
destination_project = "jpujo-gcp-devzone"
destination_dataset = "ds1"
destination_table = "test"

gcs = gcsfs.GCSFileSystem()

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


def csv_transcode(input):
    for row in input:
        yield {
            "Foo": int(row["Foo"]),
            "Bar": int(row["Bar"]),
            "Baz": row["Baz"],
        }


def csv2avro():
    csv_files = [x for x in gcs.ls(source) if x.endswith(".csv")]
    for file_no, csv_file in enumerate(csv_files):
        with gcs.open(csv_file, mode="r", newline='', encoding="ISO-8859-1") as input_fd:
            reader = csv.DictReader(input_fd, dialect='excel')
            with gcs.open(avro_path + '_' + str(file_no).zfill(4) + ".avro", mode='wb') as output_fd:
                fastavro.writer(output_fd, parsed_schema, csv_transcode(reader))


def avro2bq():
    bq = bigquery.Client(project=destination_project)
    job_config = bigquery.LoadJobConfig()
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.source_format = bigquery.SourceFormat.AVRO
    job_config.use_avro_logical_types = True
    bq.load_table_from_uri(source_uris="gs:/" + avro_path + "*.avro",
                           destination=f"{destination_project}.{destination_dataset}.{destination_table}",
                           location="EU",
                           job_config=job_config).result()


csv2avro()
avro2bq()
