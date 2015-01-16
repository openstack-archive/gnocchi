# -*- encoding: utf-8 -*-
#
# Copyright (c) 2014 eNovance
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime

import six


class ArchivePolicy(object):

    # TODO(eglynn): figure out how to accommodate multi-valued aggregation
    #               methods, where there is no longer just a single aggregate
    #               value to be stored per-period (e.g. ohlc)
    VALID_AGGREGATION_METHODS = set(('mean', 'sum', 'last', 'max', 'min',
                                     'std', 'median', 'first', 'count'))

    # Set that contains all the above values + their minus equivalent (-mean)
    # and the "*" entry.
    VALID_AGGREGATION_METHODS_VALUES = VALID_AGGREGATION_METHODS.union(
        set(('*',)),
        set(map(lambda s: "-" + s,
                VALID_AGGREGATION_METHODS)))

    def __init__(self, name, back_window, definition,
                 aggregation_methods=None):
        self.name = name
        self.back_window = back_window
        self.definition = definition
        if aggregation_methods is None:
            self.aggregation_methods = set(("*",))
        else:
            self.aggregation_methods = aggregation_methods

    @property
    def aggregation_methods(self):
        if ('*' in self._aggregation_methods
           or all(map(lambda s: s.startswith('-'),
                      self._aggregation_methods))):
            agg_methods = self.VALID_AGGREGATION_METHODS.copy()
        else:
            agg_methods = self._aggregation_methods

        for entry in self._aggregation_methods:
            if entry and entry[0] == '-':
                agg_methods -= set((entry[1:],))

        return agg_methods

    @aggregation_methods.setter
    def aggregation_methods(self, value):
        value = set(value)
        rest = value - self.VALID_AGGREGATION_METHODS_VALUES
        if rest:
            raise ValueError("Invalid value for aggregation_methods: %s" %
                             rest)
        self._aggregation_methods = value

    @classmethod
    def from_dict(cls, d):
        return cls(d['name'],
                   d['back_window'],
                   [ArchivePolicyItem(**definition)
                    for definition in d['definition']],
                   d.get('aggregation_methods'))

    def to_dict(self):
        return {
            "name": self.name,
            "back_window": self.back_window,
            "definition": [d.to_dict()
                           for d in self.definition],
            "aggregation_methods": self.aggregation_methods,
        }

    def to_human_readable_dict(self):
        return {
            "name": self.name,
            "back_window": self.back_window,
            "definition": [d.to_human_readable_dict()
                           for d in self.definition],
            "aggregation_methods": self.aggregation_methods,
        }


class ArchivePolicyItem(object):
    def __init__(self, granularity=None, points=None, timespan=None):
        if (granularity is not None
           and points is not None
           and timespan is not None):
            if timespan != granularity * points:
                raise ValueError(
                    u"timespan ≠ granularity × points")

        if granularity is None:
            if points is None or timespan is None:
                raise ValueError(
                    "At least two of granularity/points/timespan "
                    "must be provided")
            granularity = round(timespan / float(points))

        if points is None:
            if timespan is None:
                self.timespan = None
            else:
                points = int(timespan / granularity)
                self.timespan = granularity * points
        else:
            self.timespan = granularity * points

        self.points = points
        self.granularity = granularity

    def to_dict(self):
        return {
            'timespan': self.timespan,
            'granularity': self.granularity,
            'points': self.points
        }

    def to_human_readable_dict(self):
        """Return a dict representation with human readable values."""
        return {
            'timespan': six.text_type(
                datetime.timedelta(seconds=self.timespan))
            if self.timespan is not None
            else None,
            'granularity': six.text_type(
                datetime.timedelta(seconds=self.granularity)),
            'points': self.points,
        }
