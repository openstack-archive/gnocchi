# -*- encoding: utf-8 -*-
#
#
# Authors: Ana Malagon  <atmalagon@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import datetime

import numpy as np
import pandas as pd

from gnocchi import aggregates


def aggregate_result(data, func, window, center=False, min_periods=1):
    '''Performs aggregation on data.'''
    def rolling_window(x):
        if center:
            dslice = data[x - datetime.timedelta(seconds=window) / 2
                          + datetime.timedelta(milliseconds=1):
                          x + datetime.timedelta(seconds=window) / 2
                          - datetime.timedelta(milliseconds=1)]
            # NOTE(atmalagon): the millisecond adjustment is so that we do
            # not have inclusive endpoints.
        else:
            dslice = data[x - datetime.timedelta(seconds=window) + datetime.
                          timedelta(milliseconds=1):x]
        if dslice.size < min_periods:
            return np.nan
        else:
            return func(dslice)

    data = pd.Series(data)
    idx = pd.DatetimeIndex(data.index)
    data = pd.Series(data, index=idx).sort_index()
    idx = pd.Series(data.index, index=data.index)
    try:
        result = idx.apply(rolling_window).dropna()
        return result.to_dict()
    except Exception:
        raise aggregates.AggregationFailure(Exception.message)


class RollingMean(aggregates.CustomAggregator):

    def compute(self, storage_object, entity_id, start, stop, granularity,
                center=False):

        data = storage_object.get_measures(entity_id, start, stop,
                                           aggregation='mean',
                                           granularity=1)
        if center:
            center = (center.upper() == 'T')

        return aggregate_result(data, np.mean, granularity,
                                center, min_periods=1)


class RollingVariance(aggregates.CustomAggregator):

    def compute(self, storage_object, entity_id, start, stop, granularity,
                center=False):

        data = storage_object.get_measures(entity_id, start, stop,
                                           granularity=1)

        if center:
            center = (center.upper() == 'T')

        return aggregate_result(data, np.var, granularity,
                                center, min_periods=2)


class EWMA(aggregates.CustomAggregator):

    def compute(self, storage_object, entity_id, start, stop, granularity):

        data = storage_object.get_measures(entity_id, start, stop,
                                           granularity=1)

        data = pd.Series(data)
        idx = pd.DatetimeIndex(data.index)
        data = pd.Series(data, index=idx).sort_index()

        result = []
        result.append(data[0])
        for j in range(1, len(data)):
            delta = (data.index[j] - data.index[j - 1]).total_seconds()
            w = np.exp(- delta / granularity)
            result.append(result[-1] * w + data[j] * (1 - w))

        result = pd.Series(result, index=data.index).dropna()
        return result.to_dict()


class HoltWinters(aggregates.CustomAggregator):

    def compute(self, storage_object, entity_id, start, stop,
                granularity, alpha, beta):

        beta = float(beta)
        alpha = float(alpha)
        if (beta < 0 or beta > 1) or (alpha < 0 or alpha > 1):
            raise aggregates.AggregationFailure(
                'Aggregation parameters are not in range [0, 1]')

        data = storage_object.get_measures(entity_id, start, stop,
                                           granularity=1)

        data = pd.Series(data)
        idx = pd.DatetimeIndex(data.index)
        data = pd.Series(data, index=idx).sort_index()

        result = []

        # WIP. Right now does exponential smoothing for irregularly spaced
        # data with two smoothing coefficients; accounts for linear trends.
        # TODO(atmalagon): add smoothing coefficient for seasonality

        # initialize the first output value as the mean of an initial block
        # of observations (for now take the initial block to be within
        # granularity seconds of the start timestamp).
        start = data.index[0]
        t = datetime.timedelta(seconds=granularity)
        initial_obs = data[start:start + t]
        initial_times = [(data.index[i] - data.index[0]).total_seconds()
                         for i in range(len(initial_obs))]
        result.append(np.mean(initial_obs))
        slope_rec = np.polyfit(initial_times, initial_obs, 1)[1]

        # set the initial value of the smoothing coefficients.
        # q is the average time spacing of the data.
        delta = [(data.index[j] - data.index[j - 1]).total_seconds()
                 for j in range(1, len(data))]
        q = np.mean(delta)
        alpha_rec = 1 - np.power(1 - alpha, q)
        beta_rec = 1 - np.power(1 - beta, q)

        for j in range(1, len(data)):
            d = delta[j - 1]
            alpha_rec = alpha_rec / (np.power(1 - alpha, d) + alpha_rec)
            beta_rec = beta_rec / (np.power(1 - beta, d) + beta_rec)
            result.append((result[-1] + d * slope_rec) * (1 - alpha_rec) +
                          data[j] * alpha_rec)
            a = beta_rec * (result[-1] + result[-2]) / d
            b = (1 - beta_rec) * slope_rec
            slope_rec = a + b
        return pd.Series(result, index=data.index).dropna().to_dict()

        # NOTE(atmalagon): there is another method to do this smoothing
        # procedure called Brown's method that seems to have a more
        # rigorous derivation; not sure if there is any difference practically.
