from datetime import datetime, time, timedelta, date
from typing import List

import pandas as pd
import xarray as xr
import numpy as np

from .enums import City, Service, TrafficType, TrafficDataDimensions
from .utils import Calendar, Anomalies


class CityTrafficData:
    def __init__(self, data: xr.DataArray, city: City, traffic_type: TrafficType, service: List[Service], day: List[date], aggregation_level: str):
        self.data = data
        self.city = city
        self.aggregation_level = aggregation_level
        self.traffic_type = traffic_type
        self.service = service
        self.day = day

    def get_service_consumption_by_location(self, start: time, end: time, remove_holidays: bool = True, remove_anomaly_periods: bool = True) -> pd.DataFrame:
        print('transforming to datetime index')
        traffic_data = CityTrafficData.day_time_to_datetime_index(xar=self.data)
        print('removing undesired days')
        traffic_data = self._remove_nights_where_traffic_data_is_noisy(traffic_data=traffic_data, city=self.city, remove_nights_before_holidays=remove_holidays, remove_nights_of_anomaly_periods=remove_anomaly_periods)
        print('removing undesired times')
        traffic_data = self._remove_times_outside_range(traffic_data=traffic_data, start=start, end=end)
        print('summing')
        service_consumption_by_location__total_hours = traffic_data.sum(dim=TrafficDataDimensions.DATETIME.value).to_pandas() / 4
        return service_consumption_by_location__total_hours

    @staticmethod
    def _remove_times_outside_range(traffic_data: xr.DataArray, start: time, end: time) -> xr.DataArray:
        auxiliary_date = date(2020, 1, 1)
        auxiliary_datetime_start, auxiliary_datetime_end = datetime.combine(auxiliary_date, start), datetime.combine(auxiliary_date, end)
        length_period = auxiliary_datetime_end - auxiliary_datetime_start if auxiliary_datetime_end > auxiliary_datetime_start else (auxiliary_datetime_end + timedelta(days=1)) - auxiliary_datetime_start
        dates = list(np.unique([d.date() for d in pd.DatetimeIndex(traffic_data.datetime.values).to_pydatetime()]))
        return CityTrafficData._remove_period_on_dates(traffic_data=traffic_data, time_start_period=start, length_period=length_period, dates=dates)

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
        return CityTrafficData._remove_period_on_dates(traffic_data=traffic_data, dates=dates, time_start_period=time_start_period, length_period=timedelta(days=1))

    @staticmethod
    def _remove_period_on_dates(traffic_data: xr.DataArray, dates: List[date], time_start_period: time, length_period: timedelta):
        datetime_ = pd.DatetimeIndex(traffic_data.datetime.values).to_pydatetime()
        datetime_intervals_to_remove = [(datetime.combine(day, time_start_period), datetime.combine(day, time_start_period) + length_period) for day in dates]
        datetime_to_remove = np.concatenate([np.where((datetime_ >= start) & (datetime_ < end))[0] for start, end in datetime_intervals_to_remove])
        datetime_to_keep = np.setdiff1d(np.arange(len(datetime_)), datetime_to_remove)
        return traffic_data.isel(datetime=datetime_to_keep)

    @staticmethod
    def day_time_to_datetime_index(xar: xr.DataArray) -> xr.DataArray:
        new_index = np.add.outer(xar.indexes[TrafficDataDimensions.DAY.value], xar.indexes[TrafficDataDimensions.TIME.value]).flatten()
        datetime_xar = xar.stack(datetime=(TrafficDataDimensions.DAY.value, TrafficDataDimensions.TIME.value), create_index=False)
        datetime_xar = datetime_xar.reindex({TrafficDataDimensions.DATETIME.value: new_index})
        return datetime_xar
