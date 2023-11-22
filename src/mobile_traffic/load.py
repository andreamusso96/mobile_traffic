from datetime import date
from joblib import Parallel, delayed
from typing import List
import itertools

import pandas as pd
import xarray as xr
import numpy as np
from tqdm import tqdm

from .enums import City, Service, TrafficType, TrafficDataDimensions, TimeOptions
from . import config
from .utils import logger


def load_traffic_data(traffic_type: TrafficType, city: City, service: List[Service], day: List[date]) -> xr.DataArray:
    tuples = list(itertools.product(service, day))
    map_tuple_index = {t: i for i, t in enumerate(tuples)}
    data_vals = Parallel(n_jobs=-1)(delayed(load_traffic_data_base)(traffic_type=traffic_type, city=city, service=s, day=d) for s, d in tqdm(tuples))
    # data_vals = [load_traffic_data_base(traffic_type=traffic_type, geo_data_type=geo_data_type, city=city, service=s, day=d) for s, d in tqdm(tuples)]
    data_vals = np.stack([np.stack([data_vals[map_tuple_index[(s, d)]] for i, s in enumerate(service)], axis=-1) for j, d in enumerate(day)], axis=-1)
    coords = {TrafficDataDimensions.TILE.value: get_location_list(city=city),
              TrafficDataDimensions.TIME.value: TimeOptions.get_times(),
              TrafficDataDimensions.SERVICE.value: [s.value for s in service],
              TrafficDataDimensions.DAY.value: day}
    dims = [TrafficDataDimensions.TILE.value, TrafficDataDimensions.TIME.value, TrafficDataDimensions.SERVICE.value, TrafficDataDimensions.DAY.value]
    xar = xr.DataArray(data_vals, coords=coords, dims=dims)
    return xar


def get_location_list(city: City):
    traffic_data = load_traffic_data_file(traffic_type=TrafficType.UL, city=city, service=Service.WIKIPEDIA, day=date(2019, 4, 1))
    return traffic_data.index


def load_traffic_data_base(traffic_type: TrafficType, city: City, service: Service, day: date) -> pd.DataFrame:
    if traffic_type == TrafficType.UL_AND_DL:
        ul_data = load_traffic_data_file(traffic_type=TrafficType.UL, city=city, service=service, day=day)
        dl_data = load_traffic_data_file(traffic_type=TrafficType.DL, city=city, service=service, day=day)
        traffic = ul_data + dl_data
        return traffic
    else:
        return load_traffic_data_file(traffic_type=traffic_type, city=city, service=service, day=day)


def load_traffic_data_file(traffic_type: TrafficType, city: City, service: Service, day: date) -> pd.DataFrame:
    file_path = config.get_mobile_traffic_data_file_path(traffic_type=traffic_type, city=city, service=service, day=day)
    cols = [TrafficDataDimensions.TILE.value] + list(TimeOptions.get_times())
    traffic_data = pd.read_csv(file_path, sep=' ', names=cols)
    traffic_data.set_index(TrafficDataDimensions.TILE.value, inplace=True)

    nans = traffic_data.isna().sum().sum()

    if nans > 0:
        logger.debug(f'WARNING: file of traffic_type={traffic_type.value}, city={city.value}, service={service.value}, day={day} contains NaN values n={nans}, share={nans / (traffic_data.shape[0] * traffic_data.shape[1])}. Replacing them with 0.')
        traffic_data.fillna(0, inplace=True)

    return traffic_data
