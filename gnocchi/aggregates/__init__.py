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
class CustomAggregationFailure(Exception):
    '''Error raised when aggregation fails.'''

    def __init__(self, msg):
        self.msg = msg
        super(CustomAggregationFailure, self).__init__(msg)


@six.add_metaclass(abc.ABCMeta)
class CustomAggregator(object):

    @abc.abstractmethod
    def compute(indexer_object, storage_object, entity_id, start, stop,
                window, **params):
        '''Returns the custom aggregated data.

        :param indexer_object: Indexer object for accessing the archive_policy
        :param storage_object: Storage object for retrieving the data
        :param entity_id: Entity id
        :param start: start timestamp
        :param stop: stop timestamp
        :param window: aggregation time window as frequency string
        :param **params: optional parameters center and resolution.
                         'center' indexes the aggregated data by
                         the central time value in each bin.
                         'resolution' allows the user to choose whether
                         to aggregate over the entire span
                         requested or only return the aggregation for times
                         where fine grained data was kept.

        '''
