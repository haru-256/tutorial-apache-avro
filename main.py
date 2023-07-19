import json
import avro
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import pandas as pd


def prepare():
    data = json.load(open("data/test.json"))
    schema = avro.schema.parse(open("scores.avsc", "rb").read())
    writer = DataFileWriter(open("data/scores.avro", "wb"), DatumWriter(), schema)

    for d in data:
        writer.append(d)
    writer.close()


def main():
    reader = DataFileReader(open("data/scores.avro", "rb"), DatumReader())
    df = pd.DataFrame.from_records([row for row in reader])
    reader.close()
    breakpoint()


if __name__ == "__main__":
    prepare()
    main()
