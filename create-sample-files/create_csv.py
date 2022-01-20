import io

import gcsfs
import csv
import zipfile

path = "/jpujo-gcp-devzone-gcs-1/test/"

fs = gcsfs.GCSFileSystem()

with fs.open(path + "test.zip", mode="wb") as zipfd:
    with zipfile.ZipFile(zipfd, mode="w") as zipf:
        for i in range(1, 20):
            with zipf.open(f"test_{str(i)}.csv", mode="w") as bfd1:
                with fs.open(path + f"test_{str(i)}.csv", mode="w", encoding="ISO-8859-1", newline='') as fd2:
                    writer1 = csv.writer(io.TextIOWrapper(bfd1, encoding="ISO-8859-1", newline=''), dialect='excel',
                                         delimiter=',')
                    writer2 = csv.writer(fd2, dialect='excel', delimiter=',')
                    writer1.writerow(["Foo", "Bar", "Baz"])
                    writer2.writerow(["Foo", "Bar", "Baz"])
                    for j in range(1, 5000000):
                        writer1.writerow([i, j, f"Data_{i}_{j}"])
                        writer2.writerow([i, j, f"Data_{i}_{j}"])
