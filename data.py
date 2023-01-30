"""
main: convert dataset format from csv to parquet.
part: prepare chunk dataset.
merge_result: merge prediction result.
"""

from datetime import datetime
from pathlib import Path

import numpy as np
from fire import Fire
import dask.dataframe as dd


def convert(val):
    if val == np.nan:
        return 0
    return int(val)


def read_csv_with_converter():
    df = dd.read_csv(
        "/mnt/data/ds/data/raw/pxmart__rec_dateset.csv/*",
        names=["user_id", "item_id", "txn_id", "txn_date"],
        dtype={
            "user_id": "category",
            "txn_date": "category",
            "item_id": "int",
            "txn_id": "int",
        },
        converters={"item_id": convert, "txn_id": convert},
        blocksize="4096MB",
    )
    return df


def clean(df):
    df["item_id"] = dd.to_numeric(df.item_id, errors="coerce")
    df = df[~df.item_id.isnull()]

    return df


def main():
    filepath = "/mnt/data/ds/data/raw/pxmart__rec_dateset.parquet"
    filepath_p = Path(filepath)
    if filepath_p.exists() and len(list(filepath_p.glob("*"))) > 0:
        print(f"{filepath} exists")
        return

    df = dd.read_csv(
        "/mnt/data/ds/data/raw/pxmart__rec_dateset.csv/*",
        names=["part_key", "user_id", "item_id", "txn_date"],
        dtype={
            "part_key": "category",
            "user_id": "object",
            "item_id": "object",
            "txn_date": "object",
        },
        converters={"item_id": convert},
        blocksize="250MB",
        usecols=[0, 1, 2, 4],
    )
    print("ddf.npartitions ", df.npartitions)

    print("clean start", datetime.today())
    df = clean(df)
    print("clean end", datetime.today())

    # 2022/2 cost time: 3 hours, cost memory 350GB (original csv 73GB)
    print("parquet start", datetime.today())
    df.to_parquet(
        filepath,
        schema="infer",
        write_index=False,
        engine="pyarrow",
        partition_on="part_key",
    )
    print("parquet end", datetime.today())


def main_parquet():
    filepath = "/mnt/data/ds/data/raw/pxmart__rec_dateset.parquet"
    filepath_p = Path(filepath)
    if filepath_p.exists() and len(list(filepath_p.glob("*"))) > 0:
        print(f"{filepath} exists")
        return

    df = dd.read_parquet(
        "/mnt/data/ds/data/raw/pxmart__rec_dateset.parquet/*",
        columns=["part_key", "user_id", "item_id", "txn_date"],
        converters={"item_id": convert},
        blocksize="4096MB",
    )
    print("ddf.npartitions ", df.npartitions)

    print("clean start", datetime.today())
    df = clean(df)
    print("clean end", datetime.today())

    # 2022/2 cost time: 3 hours, cost memory 350GB (original csv 73GB)
    print("parquet start", datetime.today())
    df.to_parquet(
        filepath,
        schema="infer",
        write_index=False,
        engine="pyarrow",
        partition_on="part_key",
    )
    print("parquet end", datetime.today())


def part(key):
    subfilep = f"/mnt/data/ds/data/raw/pxmart__rec_dateset_sorted_p{key}.parquet"
    if Path(subfilep).exists():
        print(f"{subfilep} exists.")
        return

    df = dd.read_parquet(
        "/mnt/data/ds/data/raw/pxmart__rec_dateset.parquet",
        # gather_statistics=False,
        filters=[("part_key", "==", str(key))],
    )

    print("ddf.npartitions ", df.npartitions)

    print("compute start", datetime.today())
    df = df.compute()
    print("compute end", datetime.today())

    df = df.drop(columns=["part_key"])

    print("sort start", datetime.today())
    df = df.sort_values(["user_id", "txn_date", "item_id"])
    print("sort end", datetime.today())

    print("parquet start", datetime.today())
    df.to_parquet(subfilep)
    print("parquet end", datetime.today())


def merge_result(end_date, format="csv"):
    filename = f"rec_result_{end_date}"
    df = dd.read_parquet(
        "/mnt/data/ds/data/final/rec_result_pkey*.parquet", engine="pyarrow"
    ).compute()
    out_df = df[~df.item_id.isnull()][["user_id", "item_id"]]
    out_df.loc[:, "item_id"] = out_df["item_id"].astype(int).astype(str)
    out_df.loc[:, "rk"] = out_df.groupby(["user_id"]).cumcount() + 1

    if format == "csv":
        out_df.to_csv(
            f"/mnt/data/ds/data/final/{filename}.csv", index=False, header=None
        )
    else:
        out_df.to_parquet(f"/mnt/data/ds/data/final/{filename}.parquet")


if __name__ == "__main__":
    Fire(
        {
            "main": main,
            "part": part,
            "merge_result": merge_result,
        }
    )
