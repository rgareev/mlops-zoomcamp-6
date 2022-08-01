from datetime import datetime
import logging

from batch import prepare_data, DEFAULT_CATEGORICAL

import pandas as pd


def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)


def test_prepare_data():
    data = [
        (None, None, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
        (1, 1, dt(1, 2, 0), dt(2, 2, 1)),        
    ]
    columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime']
    df = pd.DataFrame(data, columns=columns)

    actual_df = prepare_data(df, DEFAULT_CATEGORICAL)
    log = logging.getLogger(__name__)
    log.info("\n%s", actual_df)
    expected_df = pd.DataFrame(
        data=[
            ('-1', '-1', dt(1, 2), dt(1, 10), 8.),
            ('1', '1', dt(1, 2), dt(1, 10), 8.),
        ],
        columns=['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime', 'duration']
    )
    
    type_diff = actual_df.dtypes.compare(expected_df.dtypes)
    log.info("\n%s", type_diff)
    assert len(type_diff) == 0

    actual_df['duration'] = actual_df['duration'].round()
    expected_df['duration'] = expected_df['duration'].round()
    data_diff = actual_df.compare(expected_df)
    log.info("\n%s", data_diff)
    assert len(data_diff) == 0