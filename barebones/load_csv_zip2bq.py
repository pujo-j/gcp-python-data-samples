import io
import zipfile

import fastavro
import gcsfs
import csv

from google.cloud import bigquery

source = "/jpujo-gcp-devzone-gcs-1/test/test.zip"
avro_path = "/jpujo-gcp-devzone-gcs-1/test/output/zip/test"
destination_project = "jpujo-gcp-devzone"
destination_dataset = "ds1"
destination_table = "test2"

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


def zip_csv2avro():
    with gcs.open(source, mode="rb") as zip_fd:
        with zipfile.ZipFile(zip_fd, mode="r") as zip:
            for file_no, csv_file_name in enumerate(zip.namelist()):
                input_fd = io.TextIOWrapper(zip.open(csv_file_name, mode="r"), newline='', encoding="ISO-8859-1")
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


zip_csv2avro()
avro2bq()
