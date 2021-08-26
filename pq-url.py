import pyarrow.parquet as pq
from glob import glob

out = open("urls.txt", "a")
for fname in glob("part-*.parquet"):
    table = pq.read_table(fname)
    for url in table["URL"]:
        _ = out.write(str(url) + "\n")
