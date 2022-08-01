#!/usr/bin/env python
# coding: utf-8
import pickle
import os
from pathlib import Path
import logging

import pandas as pd


DEFAULT_CATEGORICAL = ['PUlocationID', 'DOlocationID']


def _get_storage_options():
    log = logging.getLogger(__name__)
    s3_endpoint_url = os.getenv('S3_ENDPOINT_URL')
    if s3_endpoint_url is not None:
        log.info("Using S3 endpoint URL %s", s3_endpoint_url)
        return {
            'client_kwargs': {
                'endpoint_url': s3_endpoint_url
            }
        }
    return None


def get_input_path(year, month):
    default_input_pattern = 'https://raw.githubusercontent.com/alexeygrigorev/datasets/master/nyc-tlc/fhv/fhv_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)


def prepare_data(df, categorical):
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    return df


def read_data(filename, categorical):
    df = pd.read_parquet(filename, storage_options=_get_storage_options())
    df = prepare_data(df, categorical)
    return df


def main(year: int, month: int):
    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    df = read_data(input_file, DEFAULT_CATEGORICAL)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    dicts = df[DEFAULT_CATEGORICAL].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)
    print('predicted mean duration:', y_pred.mean())
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred

    df_result.to_parquet(
        output_file, 
        engine='pyarrow', 
        index=False,
        compression=None,
        storage_options=_get_storage_options()
    )


if __name__ == '__main__':
    import sys

    logging.basicConfig(level=logging.INFO)

    year = int(sys.argv[1])
    month = int(sys.argv[2])
    main(year, month)