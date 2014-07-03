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
        offset = datetime.timedelta(seconds=window)
        mseconds = datetime.timedelta(milliseconds=1)
        if (center and x - offset / 2. >= data.index[0] and
                x + offset / 2. <= data.index[-1]):
            dslice = data[x - offset / 2. + mseconds:
                          x + offset / 2. - mseconds]
            # NOTE(atmalagon): the millisecond adjustment is so that we do
            # not have inclusive endpoints.
        elif (x >= data.index[0] and x + offset <= data.index[-1]):
            dslice = data[x:x + offset - mseconds]
        else:
            return np.nan

        if dslice.size < min_periods:
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


def get_data(indexer_object, storage_object, entity_id, start, stop, window):

        resource = indexer_object.get_resource('entity', entity_id)
        policy = indexer_object.get_archive_policy(resource['archive_policy'])

        if (stop and start):
            span = (stop - start).total_seconds()
        else:
            span = 0

        if window is None:
            msg = 'Aggregation window must be specified.'
            raise aggregates.CustomAggregationFailure(msg)
        else:
            window = int(pd.datetools.to_offset(window).delta.total_seconds())

        list_of_granularities = []
        for pair in policy['definition']:
            granularity = pair['granularity']
            points = pair['points']
            if (granularity * points >= span and window % granularity == 0):
                list_of_granularities.append(granularity)

        if list_of_granularities:
            smallest_granularity = int(min(list_of_granularities))
        else:
            msg = """No archive granularity satisfies both timespan
                     and window requirements"""
            raise aggregates.CustomAggregationFailure(msg)

        data = storage_object.get_measures(entity_id, start, stop,
                                           aggregation='mean',
                                           granularity=smallest_granularity)
        return data, window


class RollingMean(aggregates.CustomAggregator):

    def compute(self, indexer_object, storage_object, entity_id, start, stop,
                window=None,
                center=False):

        data, moving_window = get_data(indexer_object, storage_object,
                                       entity_id, start, stop, window)

        if center:
            center = (center.upper() == 'T')

        return aggregate_result(data, np.mean, moving_window,
                                center, min_periods=1)


class RollingVariance(aggregates.CustomAggregator):

    def compute(self, indexer_object, storage_object, entity_id, start, stop,
                window=None,
                center=False):

        data, moving_window = get_data(indexer_object, storage_object,
                                       entity_id, start, stop, window)
        if center:
            center = (center.upper() == 'T')

        return aggregate_result(data, np.var, moving_window,
                                center, min_periods=2)


class EWMA(aggregates.CustomAggregator):

    def compute(self, indexer_object, storage_object, entity_id, start, stop,
                window=None):

        data, moving_window = get_data(indexer_object, storage_object,
                                       entity_id, start, stop, window)

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
