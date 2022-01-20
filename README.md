# GCP Python data samples

## Sample init

The following [source](create-sample-files/create_csv.py) creates a bunch of GCS files for the following samples

## Barebones

Those code samples can be used in light python environments: cloud run, composer workers, cloud build, etc...
They sacrifice performance for simplicity (no multiprocessing, no threads, no async) and low requirements.

### Requirements

* google-cloud-bigquery
* gcsfs
* fastavro

### Sample source

* Read CSV with the python csv library (handles encoding issues and dialects where BQ does not) [source](barebones/load_csv2bq.py)
* Read CSVs inside an infozip file with end to end streaming [source](barebones/load_csv_zip2bq.py)

## Dask

Those code samples use dask (multiprocessing) to increase throughput, they can be scaled up with dask distributed 
but there is a point where using Google Cloud Dataflow would be simpler.

### Requirements

* google-cloud-bigquery
* gcsfs
* fastavro
* dask
* google-cloud-bigquery-storage\[fastavro\]

### Sample source

* Convert csv to avro for BQ load in parallel using dask multiprocessing [source](dask/csv2avro.py)

