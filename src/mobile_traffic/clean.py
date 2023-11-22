from datetime import datetime, time, timedelta, date
from typing import List

import pandas as pd
import xarray as xr
import numpy as np

from .enums import City
from .utils import Calendar, Anomalies


def remove_time_period_on_dates(traffic_data: xr.DataArray, dates: List[date], time_start_period: time, length_period: timedelta):
    datetime_ = pd.DatetimeIndex(traffic_data.datetime.values).to_pydatetime()  # noqa
    datetime_intervals_to_remove = [(datetime.combine(day, time_start_period), datetime.combine(day, time_start_period) + length_period) for day in dates]
    datetime_to_remove = np.concatenate([np.where((datetime_ >= start) & (datetime_ < end))[0] for start, end in datetime_intervals_to_remove])
    datetime_to_keep = np.setdiff1d(np.arange(len(datetime_)), datetime_to_remove)
    return traffic_data.isel(datetime=datetime_to_keep)


def remove_nights_before_holidays(traffic_data: xr.DataArray) -> xr.DataArray:
    days_holiday = Calendar.holidays()
    days_before_holiday = [holiday - timedelta(days=1) for holiday in days_holiday]
    days_to_remove = list(set(days_before_holiday).union(set(Calendar.fridays_and_saturdays())))
    traffic_data = remove_time_period_on_dates(traffic_data=traffic_data, time_start_period=time(15), length_period=timedelta(days=1), dates=days_to_remove)
    # Since the first day is a saturday, we cut of its night. If we do not remove it, we have half a day detached from the rest of our series.
    traffic_data = remove_time_period_on_dates(traffic_data=traffic_data, time_start_period=time(0), length_period=timedelta(days=1), dates=[pd.Timestamp(traffic_data.datetime[0].values).to_pydatetime().date()])
    return traffic_data


def remove_nights_before_anomalies(traffic_data: xr.DataArray, city: City) -> xr.DataArray:
    days_anomaly = Anomalies.get_anomaly_dates_by_city(city=city)
    days_before_anomaly = [day - timedelta(days=1) for day in days_anomaly]
    days_to_remove = list(set(days_anomaly).union(set(days_before_anomaly)))
    return remove_time_period_on_dates(traffic_data=traffic_data, time_start_period=time(15), length_period=timedelta(days=1), dates=days_to_remove)


def remove_nights_when_traffic_data_is_noisy(traffic_data: xr.DataArray, city: City) -> xr.DataArray:
    traffic_data = remove_nights_before_holidays(traffic_data=traffic_data)
    traffic_data = remove_nights_before_anomalies(traffic_data=traffic_data, city=city)
    return traffic_data


def remove_times_outside_range(traffic_data: xr.DataArray, start: time, end: time) -> xr.DataArray:
    auxiliary_date = date(2020, 1, 1)
    auxiliary_datetime_start, auxiliary_datetime_end = datetime.combine(auxiliary_date, start), datetime.combine(auxiliary_date, end)
    length_period_keep = auxiliary_datetime_end - auxiliary_datetime_start if auxiliary_datetime_end > auxiliary_datetime_start else (auxiliary_datetime_end + timedelta(days=1)) - auxiliary_datetime_start
    length_period_remove = timedelta(days=1) - length_period_keep
    dates = list(np.unique([d.date() for d in pd.DatetimeIndex(traffic_data.datetime.values).to_pydatetime()]))  # noqa
    return remove_time_period_on_dates(traffic_data=traffic_data, time_start_period=end, length_period=length_period_remove, dates=dates)