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
import six

from gnocchi import aggregates

from pytimeparse import timeparse


def aggregate_result(data, func, window, granularity, center=False,
                     min_size=1):
    '''Performs aggregation on data.'''
    def rolling_window(x):
        msec = datetime.timedelta(milliseconds=1)
        start = ts_sort.index[0]
        stop = ts_sort.index[-1]

        if center:
            left = datetime.timedelta(seconds=window / 2.)
            right = datetime.timedelta(seconds=window / 2.) - msec
        else:
            left = datetime.timedelta(seconds=0)
            right = datetime.timedelta(seconds=window) - msec

        if x - left >= start and x + right <= stop:
            if (window / granularity) % 2 == 1 and center:
                msg = """Window must be an even multiple of the granularity
                      when using center option."""
                raise aggregates.CustomAggregationFailure(msg)
            else:
                dslice = ts_sort[x - left:x + right]
        else:
            return np.nan

        if dslice.size < min_size:
            return np.nan
        else:
            return func(dslice)

    ts = pd.Series(data).dropna()
    ts_sort = pd.Series(ts, index=pd.DatetimeIndex(ts.index)).sort_index()

    try:
        result = pd.Series(ts_sort.index,
                           index=ts_sort.index).apply(rolling_window)
        return result.dropna().to_dict()
    except Exception as e:
        raise aggregates.CustomAggregationFailure(str(e))


def get_data(indexer_object, storage_object, entity_id, start=None,
             stop=None, window=None, resolution='high'):

        resource = indexer_object.get_resource('entity', entity_id)
        policy = indexer_object.get_archive_policy(resource['archive_policy'])

        if window is None:
            msg = 'Aggregation window must be specified.'
            raise aggregates.CustomAggregationFailure(msg)
        else:
            window = int(timeparse.timeparse(six.text_type(window)))

        list_of_granularities = [p['granularity'] for p in policy['definition']
                                 if (window % p['granularity'] == 0)]

        if list_of_granularities:
            if resolution == 'high':
                chosen_granularity = int(min(list_of_granularities))
            elif resolution == 'low':
                chosen_granularity = int(max(list_of_granularities))
            else:
                msg = """Resolution parameter not recognized. Must be
                         either 'high' or 'low'."""
                raise aggregates.CustomAggregationFailure(msg)
        else:
            msg = 'No archive granularity factors into window span'
            raise aggregates.CustomAggregationFailure(msg)

        data = storage_object.get_measures(entity_id, start, stop,
                                           aggregation='mean',
                                           granularity=chosen_granularity)
        return data, chosen_granularity, window


class RollingMean(aggregates.CustomAggregator):

    def compute(self, indexer_object, storage_object, entity_id, start, stop,
                window=None,
                center=False,
                resolution='high'):

        data, granularity, moving_window = get_data(indexer_object,
                                                    storage_object, entity_id,
                                                    start, stop, window,
                                                    resolution)

        if center:
            center = (center.upper()[0] == 'T')

        return aggregate_result(data, np.nanmean, moving_window, granularity,
                                center, min_size=1)


class RollingVariance(aggregates.CustomAggregator):

    def compute(self, indexer_object, storage_object, entity_id, start, stop,
                window=None,
                center=False,
                resolution='high'):

        data, granularity, moving_window = get_data(indexer_object,
                                                    storage_object, entity_id,
                                                    start, stop, window,
                                                    resolution)

        if center:
            center = (center.upper()[0] == 'T')

        return aggregate_result(data, np.nanvar, moving_window, granularity,
                                center, min_size=2)


class EWMA(aggregates.CustomAggregator):

    def compute(self, indexer_object, storage_object, entity_id, start, stop,
                window=None,
                resolution='high'):

        data, granularity, moving_window = get_data(indexer_object,
                                                    storage_object,
                                                    entity_id, start, stop,
                                                    window, resolution)

        ts = pd.Series(data).dropna()
        ts_sort = pd.Series(ts, index=pd.DatetimeIndex(ts.index)).sort_index()

        result = []
        result.append(ts_sort[0])
        for j in range(1, len(data)):
            delta = (ts_sort.index[j] - ts_sort.index[j - 1]).total_seconds()
            w = np.exp(- delta / moving_window)
            result.append(result[-1] * w + ts_sort[j] * (1 - w))

        return pd.Series(result, index=ts_sort.index).dropna().to_dict()
