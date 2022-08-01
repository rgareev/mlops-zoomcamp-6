#!/usr/bin/env python
# coding: utf-8
import pickle
import pandas as pd
from pathlib import Path

DEFAULT_CATEGORICAL = ['PUlocationID', 'DOlocationID']


def prepare_data(df, categorical):
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    return df


def read_data(filename, categorical):
    df = pd.read_parquet(filename)
    df = prepare_data(df, categorical)
    return df


def main(year: int, month: int):
    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    input_file = f'/Users/rgareev/data/ny-tlc/src/fhv_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f'/Users/rgareev/data/ny-tlc/wrk/year_{year:04d}/month_{month:02d}/predictions.parquet'

    df = read_data(input_file, DEFAULT_CATEGORICAL)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    dicts = df[DEFAULT_CATEGORICAL].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)
    print('predicted mean duration:', y_pred.mean())
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred

    output_file = Path(output_file)
    output_file.parent.mkdir(exist_ok=True, parents=True)
    df_result.to_parquet(output_file, engine='pyarrow', index=False)


if __name__ == '__main__':
    import sys

    year = int(sys.argv[1])
    month = int(sys.argv[2])
    main(year, month)