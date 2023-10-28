from typing import List
from datetime import date, datetime, time

import xarray as xr

from . import preprocessing
from .enums import City, Service, TrafficType, GeoDataType, ServiceType, TimeOptions
from .city_traffic_data import CityTrafficData


def get_traffic_data(traffic_type: TrafficType, geo_data_type: GeoDataType, city: City, service: List[Service] = None, day: List[date] = None) -> CityTrafficData:
    service = Service.get_services(service_type=ServiceType.ENTERTAINMENT) if service is None else service
    day = list(TimeOptions.get_days()) if day is None else day
    data = preprocessing.load_traffic_data(traffic_type=traffic_type, geo_data_type=geo_data_type, city=city, service=service, day=day)
    return CityTrafficData(data=data, city=city, traffic_type=traffic_type, service=service, day=day, aggregation_level=geo_data_type.value)
