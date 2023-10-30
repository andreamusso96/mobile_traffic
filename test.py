import mobile_traffic as mt
import numpy as np
from datetime import time


if __name__ == '__main__':
    traffic = mt.get_traffic_data(traffic_type=mt.TrafficType.USERS, geo_data_type=mt.GeoDataType.IRIS, city=mt.City.PARIS, service=[mt.Service.TWITTER, mt.Service.SNAPCHAT], day=list(mt.TimeOptions.get_days()[0:20]))
    print(traffic.data.coords)
    print(traffic.data.service)
    print(traffic.data.day)
    print(traffic.get_traffic_time_series_by_location())
    print(traffic.get_service_consumption_by_location(start=time(22), end=time(3)))
    print(traffic.get_service_consumption_by_time_of_day(start=time(22), end=time(3)))