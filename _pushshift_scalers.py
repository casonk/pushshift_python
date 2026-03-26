from zlib import crc32

import numpy as np
import pandas as pd


def numeric_scaler(col, df):
    col_max = df[col].max()
    col_min = df[col].min()
    col_range = col_max - col_min
    if pd.isna(col_range) or col_range == 0:
        return pd.Series(0.0, index=df.index)
    scaled = df[col].apply(lambda x: (x - col_min) / col_range)
    return scaled


def boolean_scaler(col, df):
    return df[col].replace({True: 1, False: 0})


def string_scaler(col, df):
    def string_hasher(string):
        s = str(string).encode("utf-8")
        try:
            unscaled_hash = float(crc32(s) & 0xFFFFFFFF)
        except:
            unscaled_hash = np.nan
        string_hash = unscaled_hash / 2**32
        return string_hash

    scaled = df[col].apply(lambda x: string_hasher(x))
    return scaled


def append_row(df, row):
    return pd.concat([df, pd.DataFrame([row])], ignore_index=True)


__all__ = [
    "append_row",
    "boolean_scaler",
    "numeric_scaler",
    "string_scaler",
]
