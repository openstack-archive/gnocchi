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

import abc
import six


@six.add_metaclass(abc.ABCMeta)
class AggregationFailure(Exception):
    '''Error raised when aggregation fails.'''


@six.add_metaclass(abc.ABCMeta)
class CustomAggregator(object):

    @abc.abstractmethod
    def compute(storage_object, entity_id, start, stop, granularity, **params):
        '''Returns the custom aggregated data.

        :param storage_object: The storage object to call get_measures
        :param entity_id: Entity id
        :param start: start timestamp
        :param stop: stop timestamp
        :param granularity: time window
        :param **params: for moving aggregates, an optional paramter center
                         can be specified

        '''
