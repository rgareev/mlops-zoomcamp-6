from datetime import datetime
import logging

from batch import get_storage_options, get_input_path

import pandas as pd


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)


data = [
    (None, None, dt(1, 2), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
    (1, 1, dt(1, 2, 0), dt(2, 2, 1)),        
]
columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime']
df = pd.DataFrame(data, columns=columns)

df_path = get_input_path(2021, 1)
log.info("Writing DF to %s", df_path)
df.to_parquet(
    df_path,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=get_storage_options()
)