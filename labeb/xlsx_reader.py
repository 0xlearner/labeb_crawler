from itertools import islice
import multiprocessing as mp
import csv

csv.field_size_limit(100000000)

largefile = "6045_lulu_bahrain_items_final.csv"


import csv

reader = csv.DictReader(open(largefile, "r"))


def gen_chunks(reader, chunksize=100):
    """
    Chunk generator. Take a CSV `reader` and yield
    `chunksize` sized slices.
    """
    data = {}
    for index, line in enumerate(reader, 0):
        if index % chunksize == 0 and index > 0:
            yield data
            data.clear()
        data.setdefault((line["LabebStoreId"], line["catalog_uuid"]), []).append(line)
    yield data


# gen_chunks(reader, chunksize=3)
for chunk in gen_chunks(reader, chunksize=3):
    for row in chunk.values():
        print(row[1])


# def chunks(iterable, n):
#     """Yield successive n-sized chunks from iterable."""
#     it = iter(iterable)
#     while chunk := list(islice(it, n)):
#         yield chunk


# data = {}
# with open(largefile, "r") as f_in:
#     reader = csv.DictReader(f_in)
#     for row in chunks(reader, 10):
#         data.setdefault((row["LabebStoreId"], row["catalog_uuid"]), []).append(row)
