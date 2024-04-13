from typing import List, Dict, Callable

import numpy as np
import xarray as xr
from datetime import time
from tqdm import tqdm

from .enums import TrafficDataDimensions, TrafficType, City, Service, TimeOptions
from .load import load_traffic_data
from .clean import remove_times_outside_range, remove_nights_when_traffic_data_is_noisy


class MobileTrafficDataset:
    def __init__(self, data: Dict[City, xr.DataArray]):
        self.data = data

    def save(self, folder_path: str):
        for city, data in self.data.items():
            time_as_str = [str(t) for t in data.time.values]
            data_ = data.assign_coords(time=time_as_str)
            data_.to_netcdf(f'{folder_path}/mobile_traffic_{city.value.lower()}_by_tile_service_and_time.nc')


def day_time_to_datetime_index(xar: xr.DataArray) -> xr.DataArray:
    new_index = np.add.outer(xar.indexes[TrafficDataDimensions.DAY.value], xar.indexes[TrafficDataDimensions.TIME.value]).flatten()
    datetime_xar = xar.stack(datetime=(TrafficDataDimensions.DAY.value, TrafficDataDimensions.TIME.value), create_index=False)
    datetime_xar = datetime_xar.reindex({TrafficDataDimensions.DATETIME.value: new_index})
    return datetime_xar


def get_night_traffic_by_tile_service_time_city(traffic_type: TrafficType, start_night: time, end_night: time, city: List[City] = None, service: List[Service] = None, remove_noisy_nights: bool = True) -> MobileTrafficDataset:
    city = city if city is not None else [c for c in City]
    service = service if service is not None else [s for s in Service]
    traffic_data = {c: get_night_traffic_city_by_tile_service_time(city=c, traffic_type=traffic_type, start_night=start_night, end_night=end_night, service=service, remove_noisy_nights=remove_noisy_nights) for c in tqdm(city)}
    return MobileTrafficDataset(data=traffic_data)


def get_night_traffic_city_by_tile_service_time(city: City, traffic_type: TrafficType, start_night: time, end_night: time, service: List[Service], remove_noisy_nights: bool = True):
    traffic_data_city = []
    for i in range(0, len(service), 5):
        service_ = service[i:i + 5]
        traffic_data_service = load_traffic_data(traffic_type=traffic_type, city=city, service=service_, day=TimeOptions.get_days())
        traffic_data_service = day_time_to_datetime_index(xar=traffic_data_service)
        if remove_noisy_nights:
            traffic_data_service = remove_nights_when_traffic_data_is_noisy(traffic_data=traffic_data_service, city=city)

        traffic_data_service = remove_times_outside_range(traffic_data=traffic_data_service, start=start_night, end=end_night)
        traffic_data_service = traffic_data_service.groupby(group=f'{TrafficDataDimensions.DATETIME.value}.time').sum()
        sorted_time_index = _sort_time_index(time_index=traffic_data_service.time.values, reference_time=start_night)
        traffic_data_service = traffic_data_service.reindex({TrafficDataDimensions.TIME.value: sorted_time_index})
        traffic_data_city.append(traffic_data_service)

    traffic_data_city = xr.concat(traffic_data_city, dim=TrafficDataDimensions.SERVICE.value)
    return traffic_data_city


def _sort_time_index(time_index: List[time], reference_time: time):
    auxiliary_day = datetime(2020, 2, 1)
    auxiliary_dates = []

    for t in time_index:
        if t < reference_time:
            auxiliary_dates.append(datetime.combine(date=auxiliary_day + timedelta(days=1), time=t))
        else:
            auxiliary_dates.append(datetime.combine(date=auxiliary_day, time=t))

    auxiliary_dates.sort()
    sorted_times = [d.time() for d in auxiliary_dates]
    return sorted_times
