import os
from datetime import date

from .enums import City, Service, TrafficType

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
data_dir = f'{base_dir}/data'


def get_mobile_traffic_data_file_path(traffic_type: TrafficType, city: City, service: Service, day: date):
    day_str = day.strftime('%Y%m%d')
    path = f'{data_dir}/tile/{city.value}/{service.value}/{day_str}/'
    file_name = f'{city.value}_{service.value}_{day_str}_{traffic_type.value}.txt'
    file_path = path + file_name
    return file_path