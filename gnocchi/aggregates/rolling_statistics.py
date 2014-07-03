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


def aggregate_result(data, func, window, center=False, min_size=1):
    '''Performs aggregation on data.'''
    def rolling_window(x):
        offset = datetime.timedelta(seconds=window)
        mseconds = datetime.timedelta(milliseconds=1)
        dslice = pd.Series()

        if center:
            if (x - offset / 2 >= data.index[0] and
                    x + offset / 2 <= data.index[-1]):
                dslice = data[x - offset / 2 + mseconds:
                              x + offset / 2 - mseconds]
            # NOTE(atmalagon): the millisecond adjustment is so that we do
            # not have inclusive endpoints.
        elif (x >= data.index[0] and x + offset <= data.index[-1]):
            dslice = data[x:x + offset - mseconds]
        else:
            return np.nan

        if dslice.size < min_size:
            return np.nan
        else:
            return func(dslice)

    data = pd.Series(data)
    idx = pd.DatetimeIndex(data.index)
    data = pd.Series(data, index=idx).sort_index().dropna()
    idx = pd.Series(data.index, index=data.index)
    try:
        result = idx.apply(rolling_window).dropna()
        return result.to_dict()
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
            window = int(pd.datetools.to_offset(window).delta.total_seconds())

            list_of_granularities = []
        for pair in policy['definition']:
            try:
                granularity = pair['granularity']
            except KeyError:
                msg = 'policy definition not in terms of granularity.'
                raise aggregates.CustomAggregationFailure(msg)

            if (window % granularity == 0):
                list_of_granularities.append(granularity)

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
        return data, window


class RollingMean(aggregates.CustomAggregator):

    def compute(self, indexer_object, storage_object, entity_id, start, stop,
                window=None,
                center=False,
                resolution='high'):

        data, moving_window = get_data(indexer_object, storage_object,
                                       entity_id, start, stop, window,
                                       resolution)

        if center:
            center = (center.upper()[0] == 'T')

        return aggregate_result(data, np.mean, moving_window,
                                center, min_size=1)


class RollingVariance(aggregates.CustomAggregator):

    def compute(self, indexer_object, storage_object, entity_id, start, stop,
                window=None,
                center=False,
                resolution='high'):

        data, moving_window = get_data(indexer_object, storage_object,
                                       entity_id, start, stop, window,
                                       resolution)
        if center:
            center = (center.upper()[0] == 'T')

        return aggregate_result(data, np.var, moving_window,
                                center, min_size=2)


class EWMA(aggregates.CustomAggregator):

    def compute(self, indexer_object, storage_object, entity_id, start, stop,
                window=None,
                resolution='high'):

        data, moving_window = get_data(indexer_object, storage_object,
                                       entity_id, start, stop, window,
                                       resolution)

        data = pd.Series(data)
        idx = pd.DatetimeIndex(data.index)
        data = pd.Series(data, index=idx).sort_index().dropna()

        result = []
        result.append(data[0])
        for j in range(1, len(data)):
            delta = (data.index[j] - data.index[j - 1]).total_seconds()
            w = np.exp(- delta / moving_window)
            result.append(result[-1] * w + data[j] * (1 - w))

        return pd.Series(result, index=data.index).dropna().to_dict()
