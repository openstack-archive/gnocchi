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


def aggregate_result(data, func, window, center=False, min_periods=1):
    '''Performs aggregation on data.'''
    def rolling_window(x):
        if center:
            dslice = data[x - pd.datetools.to_offset(window).delta / 2:
                          x + pd.datetools.to_offset(window).delta / 2]
        else:
            dslice = data[x - pd.datetools.to_offset(window).delta:x]

        # TODO(atmalagon): need to add min_periods argument so RollingVariance
        # doesn't do var of one value.
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
    except:
        raise AttributeError(
            'Granularity must be specified and a freq string.')


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

        try:
            granularity = pd.datetools.to_offset(
                granularity).delta.total_seconds()
        except:
            raise AttributeError(
                'granularity must be specified and a frequency string')

        data = pd.Series(data)
        idx = pd.DatetimeIndex(data.index)
        data = pd.Series(data, index=idx).sort_index()

        result = []
        result.append(data[0])
        for j in range(1, len(data)):
            delta = (data.index[j - 1] - data.index[j]).total_seconds()
            w = np.exp(delta / granularity)
            result.append(result[-1] * w + data[j] * (1 - w))

        result = pd.Series(result, index=data.index).dropna()
        return result.to_dict()
