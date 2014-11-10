# -*- encoding: utf-8 -*-
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
class CustomAggFailure(Exception):
    '''Error raised when custom aggregation functions fail for any reason.'''

    def __init__(self, msg):
        self.msg = msg
        super(CustomAggFailure, self).__init__(msg)


@six.add_metaclass(abc.ABCMeta)
class CustomAggregator(object):

    @abc.abstractmethod
    def compute(storage_obj, entity_id, start, stop, window, **param):
        '''Returns custom aggregated data in a dict of timestamp, value pairs.

       :param storage_obj: storage object for retrieving the data
       :param entity_id: entity id
       :param start: start timestamp
       :param stop: stop timestamp
       :param window: time window (implicit units of seconds) over which to do
           aggregation.
       :param **param: optional parameters are resolution and center.
           resolution='high' (default) means the custom aggregation is done
           on the finest-resolution data being stored. resolution='low'
           aggregates over the coarsest-grained data available.
           center='True' returns the aggregated data indexed by the central
           time in the sampling window, 'False' (default) indexes data
           by the oldest time in the window. center is not supported for EWMA.

       '''
