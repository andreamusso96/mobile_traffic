import os
from datetime import date

from .enums import City, Service, TrafficType, GeoDataType

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
data_dir = f'{base_dir}/data'


def get_matching_iris_tile_file_path():
    return f'{data_dir}/TileGeo/MatchingIrisTile.csv'


def get_mobile_traffic_data_file_path(traffic_type: TrafficType, geo_data_type: GeoDataType, city: City, service: Service, day: date):
    day_str = day.strftime('%Y%m%d')
    path = f'{data_dir}/{geo_data_type.value}/{city.value}/{service.value}/{day_str}/'
    file_name = f'{city.value}_{service.value}_{day_str}_{traffic_type.value}.txt'
    file_path = path + file_name
    return file_path