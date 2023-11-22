from ..enums import City
import os

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
data_dir = f'{base_dir}/data/TileGeo'


def get_data_file_path(city: City) -> str:
    return f'{data_dir}/{city.value}.geojson'