from datetime import datetime, time, timedelta, date
from typing import List

import pandas as pd
import xarray as xr
import numpy as np

from .enums import City, Service, TrafficType, TrafficDataDimensions
from .utils import Calendar, Anomalies


class CityTrafficData:
    def __init__(self, data: xr.DataArray, city: City, traffic_type: TrafficType, aggregation_level: str):
        self.data = data
        self.city = city
        self.aggregation_level = aggregation_level
        self.traffic_type = traffic_type

    def print(self):
        print('I am a printer')

    def get_datetimes_to_keep(self, start: time, end: time, traffic_data: xr.DataArray) -> List[date]:
        datetime_ = pd.DatetimeIndex(traffic_data.datetime.values).to_pydatetime()
        dates = np.unique([d.date() for d in datetime_])
        datetime_intervals_to_keep = [(datetime.combine(day, start), datetime.combine(day, end) + timedelta(days=1)) for day in dates]
        datetime_to_keep = np.concatenate([np.where((datetime_ >= start) & (datetime_ < end))[0] for start, end in datetime_intervals_to_keep])
        return datetime_to_keep

    def get_service_consumption_by_location(self, start: time, end: time, remove_holidays: bool = True, remove_anomaly_periods: bool = True) -> pd.DataFrame:
        print('transforming to datetime index')
        traffic_data = CityTrafficData.day_time_to_datetime_index(xar=self.data)
        print('removing undesired dates and times')
        traffic_data = self._remove_nights_where_traffic_data_is_noisy(traffic_data=traffic_data, city=self.city, remove_nights_before_holidays=remove_holidays, remove_nights_of_anomaly_periods=remove_anomaly_periods)
        traffic_data = traffic_data.isel(datetime=self.get_datetimes_to_keep(start=start, end=end, traffic_data=traffic_data))
        print('removed nights')
        service_consumption_by_location__total_hours = traffic_data.sum(dim=TrafficDataDimensions.DATETIME.value).to_pandas() / 4
        print('summed')
        return service_consumption_by_location__total_hours

    @staticmethod
    def _remove_periods_where_service_consumption_data_is_noisy(traffic_data: xr.DataArray, city: City, remove_holidays: bool, remove_anomaly_periods: bool=True):
        traffic_data_ = traffic_data
        if remove_holidays:
            days_holiday = Calendar.holidays()
            days_to_remove = list(set(days_holiday).union(set(Calendar.fridays_and_saturdays())))
            traffic_data_ = CityTrafficData._remove_24h_periods(traffic_data=traffic_data_, dates=days_to_remove, time_start_period=time(0))
        if remove_anomaly_periods:
            anomaly_periods = Anomalies.get_anomaly_dates_by_city(city=city)
            traffic_data_ = CityTrafficData._remove_24h_periods(traffic_data=traffic_data_, dates=anomaly_periods, time_start_period=time(0))

        return traffic_data_

    def get_traffic_time_series_by_location(self, remove_nights_before_holidays: bool = True, remove_nights_of_anomaly_periods: bool = True, services: List[Service] = None) -> pd.DataFrame:
        traffic_data_with_selected_services = self.data if services is None else self.data.sel(service=[s.value for s in services])
        traffic_data_aggregated_services = traffic_data_with_selected_services.sum(dim=TrafficDataDimensions.SERVICE.value)
        traffic_time_series = CityTrafficData.day_time_to_datetime_index(xar=traffic_data_aggregated_services)
        traffic_time_series = self._remove_nights_where_traffic_data_is_noisy(traffic_data=traffic_time_series, city=self.city, remove_nights_before_holidays=remove_nights_before_holidays, remove_nights_of_anomaly_periods=remove_nights_of_anomaly_periods)
        traffic_time_series_by_location = traffic_time_series.T.to_pandas()
        return traffic_time_series_by_location

    @staticmethod
    def _remove_nights_where_traffic_data_is_noisy(traffic_data: xr.DataArray, city: City, remove_nights_before_holidays: bool, remove_nights_of_anomaly_periods: bool) -> xr.DataArray:
        traffic_data_ = traffic_data
        if remove_nights_before_holidays:
            days_holiday = Calendar.holidays()
            days_before_holiday = [holiday - timedelta(days=1) for holiday in days_holiday]
            days_to_remove = list(set(days_before_holiday).union(set(Calendar.fridays_and_saturdays())))
            traffic_data_ = CityTrafficData._remove_24h_periods(traffic_data=traffic_data_, dates=days_to_remove, time_start_period=time(15))
            traffic_data_ = CityTrafficData._remove_24h_periods(traffic_data=traffic_data_, dates=[pd.Timestamp(traffic_data_.datetime[0].values).to_pydatetime().date()], time_start_period=time(0))  # Since the first day is a saturday, we cut of its night. If we do not remove it, we have half a day detached from the rest of our series.
        if remove_nights_of_anomaly_periods:
            days_anomaly = Anomalies.get_anomaly_dates_by_city(city=city)
            days_before_anomaly = [day - timedelta(days=1) for day in days_anomaly]
            days_to_remove = list(set(days_anomaly).union(set(days_before_anomaly)))
            traffic_data_ = CityTrafficData._remove_24h_periods(traffic_data=traffic_data_, dates=days_to_remove, time_start_period=time(15))
        return traffic_data_

    @staticmethod
    def _remove_24h_periods(traffic_data: xr.DataArray, dates: List[date], time_start_period: time):
        datetime_ = pd.DatetimeIndex(traffic_data.datetime.values).to_pydatetime()
        datetime_intervals_to_remove = [(datetime.combine(day, time_start_period), datetime.combine(day, time_start_period) + timedelta(days=1)) for day in dates]
        datetime_to_remove = np.concatenate([np.where((datetime_ >= start) & (datetime_ < end))[0] for start, end in datetime_intervals_to_remove])
        datetime_to_keep = np.setdiff1d(np.arange(len(datetime_)), datetime_to_remove)
        return traffic_data.isel(datetime=datetime_to_keep)

    @staticmethod
    def day_time_to_datetime_index(xar: xr.DataArray) -> xr.DataArray:
        new_index = np.add.outer(xar.indexes[TrafficDataDimensions.DAY.value], xar.indexes[TrafficDataDimensions.TIME.value]).flatten()
        datetime_xar = xar.stack(datetime=(TrafficDataDimensions.DAY.value, TrafficDataDimensions.TIME.value), create_index=False)
        datetime_xar = datetime_xar.reindex({TrafficDataDimensions.DATETIME.value: new_index})
        return datetime_xar
