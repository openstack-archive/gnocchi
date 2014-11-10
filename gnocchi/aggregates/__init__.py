# -*- encoding: utf-8 -*-
#
# Copyright 2014 Openstack Foundation
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

from gnocchi import exceptions

from pytimeparse import timeparse


class CustomAggFailure(Exception):
    """Error raised when custom aggregation functions fail for any reason."""

    def __init__(self, msg):
        self.msg = msg
        super(CustomAggFailure, self).__init__(msg)


@six.add_metaclass(abc.ABCMeta)
class CustomAggregator(object):

    @staticmethod
    def check_window_valid(window):
        """Takes in the window parameter string, reformats as a float."""
        if window is None:
            msg = 'Moving aggregate must have window specified.'
            raise CustomAggFailure(msg)
        try:
            return float(timeparse.timeparse(six.text_type(window)))
        except Exception:
            raise CustomAggFailure('Invalid value for window')

    @abc.abstractmethod
    def compute(storage_obj, entity_id, start, stop, **param):
        """Returns custom aggregate in a dict of timestamp, value pairs.

       :param storage_obj: storage object for retrieving the data
       :param entity_id: entity id
       :param start: start timestamp
       :param stop: stop timestamp
       :param **param: parameters are window and optionally center.
           'window' is the granularity over which to compute the moving
           aggregate.
           'center=True' returns the aggregated data indexed by the central
           time in the sampling window, 'False' (default) indexes aggregates
           by the oldest time in the window. center is not supported for EWMA.

       """
        raise exceptions.NotImplementedError
