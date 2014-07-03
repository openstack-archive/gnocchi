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
import numpy as np
import pandas as pd

from gnocchi import aggregates
from gnocchi.openstack.common import timeutils


def general_rolling_window(x, func, data, window, center):
    '''returns the value of func applied to data within
    window.
    '''
    if center:
        dslice = data[x - pd.datetools.to_offset(window).delta:
                          x + pd.datetools.to_offset(window)]

    else:
        dslice = data[x - pd.datetools.to_offset(window).delta / 2:
                          x + pd.datetools.to_offset(window).delta / 2]
    if dslice.size < 1:
        return np.nan

    else:
        return func(dslice)


class RollingMean(aggregates.CustomAggregator):

    def compute(self, storage_object, entity_id, start, stop, granularity,
                window, center=False):
        '''Function that computes a rolling mean
        Parameters
        ----------
        :param data : dict of utc/timestamp value pairs
        :param **params : dict for parameters center and window.
             center should be True/False.
             window must be a frequency string, e.g. '90S'.
             This is internally converted into a DateOffset object,
             representing the window size.
         Returns
         -------
         dict of string timestamp and aggregated value pairs
         '''
        data = storage_object.get_measures(entity_id, start, stop,
                                           aggregation='mean',
                                           granularity=1)
        # TODO(atmalagon): for RollingMean this can be optimized so that it
        # can compute the moving-average from rolled-up data.
        center = (center.upper() == 'T')

        def rolling_window(x):
            return general_rolling_window(x, np.mean, data, window, center)

        data = pd.Series(data)
        idx = pd.DatetimeIndex(data.index)
        data = pd.Series(data, index=idx).sort_index()
        idx = pd.Series(data.index, index=data.index)
        result = idx.apply(rolling_window).dropna()
        return dict((timeutils.strtime(id), val) for id, val in
                zip(result.index, result))


class RollingVariance(aggregates.CustomAggregator):

    def compute(self, storage_object, entity_id, start, stop, granularity,
                window, center=False):
        '''Function that computes a rolling variance
        Parameters
        ----------
        :param data : dict of utc timestamp/value pairs
        :param **params : dict for parameters center and window.
             center should be True or False.
             window must be a frequency string, e.g '90s' -
             this is internally converted into a DateOffset object,
             representing the window size.
         Returns
         -------
         dict of string timestamp and aggregated value pairs
         '''
        data = storage_object.get_measures(entity_id, start, stop,
                                           granularity=1)

        center = (center.upper() == 'T')

        def rolling_window(x):
            return general_rolling_window(x, np.std, data, window, center)

        data = pd.Series(data)
        idx = pd.DatetimeIndex(data.index)
        data = pd.Series(data, index=idx).sort_index()
        idx = pd.Series(data.index, index=data.index)
        result = idx.apply(rolling_window).dropna()
        return dict((timeutils.strtime(id), val) for id, val in
                    zip(result.index, result))


class EWMA(aggregates.CustomAggregator):

    def compute(self, storage_object, entity_id, start, stop, granularity,
                window):
        '''Computes the exponentially weighted average of a time series.
        Parameters
        ----------
        :param data: dict of utc timestamp/value pairs
        :param **params: dict for parameter window
        window must be frequency string, e.g. '90s'
        Returns
        -------
        dict of string timestamp/aggregated value pairs
        '''
        data = storage_object.get_measures(entity_id, start, stop,
                                           granularity=1)
        try:
            window = float(params['window'][:-1])
        except KeyError:
            raise KeyError('window must be specified for aggregate')

        data = pd.Series(data)
        idx = pd.DatetimeIndex(data.index)
        data = pd.Series(data, index=idx).sort_index()
        result = []
        result.append(data[0])
        for j in range(1, len(data)):
            delta = (data.index[j - 1] - data.index[j]).total_seconds()
            # TODO(atmalagon): generalize this so it doesn't just assume
            # window is in seconds
            w = np.exp(delta / window)
            result.append(result[-1] * w + data[j] * (1 - w))

            result = pd.Series(result, index=data.index).dropna()
            return dict((timeutils.strtime(id), val) for id, val in
                        zip(result.index, result))
