# -*- encoding: utf-8 -*-
#
# Copyright © 2014 eNovance
#
# Authors: Julien Danjou <julien@danjou.info>
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
import collections

from oslo.config import cfg
from stevedore import driver

from gnocchi import exceptions

# TODO(eglynn): figure out how to accommodate multi-valued aggregation
#               methods, where there is no longer just a single aggregate
#               value to be stored per-period (e.g. ohlc)
AGGREGATION_TYPES = ('mean', 'sum', 'last', 'max', 'min',
                     'std', 'median', 'first')


OPTS = [
    cfg.StrOpt('driver',
               default='swift',
               help='Storage driver to use'),
    cfg.StrOpt('coordination_url',
               help='Coordination driver URL',
               default='ipc://'),
]

cfg.CONF.register_opts(OPTS, group="storage")


Measure = collections.namedtuple('Measure', ['timestamp', 'value'])


class MetricDoesNotExist(Exception):
    """Error raised when this metric does not exist."""

    def __init__(self, metric):
        self.metric = metric
        super(MetricDoesNotExist, self).__init__(
            "Metric %s does not exist" % metric)


class MetricAlreadyExists(Exception):
    """Error raised when this metric already exists."""

    def __init__(self, metric):
        self.metric = metric
        super(MetricAlreadyExists, self).__init__(
            "Metric %s already exists" % metric)


class NoDeloreanAvailable(Exception):
    """Error raised when trying to insert a value that is too old."""

    def __init__(self, first_timestamp, bad_timestamp):
        self.first_timestamp = first_timestamp
        self.bad_timestamp = bad_timestamp
        super(NoDeloreanAvailable, self).__init__(
            "%s is before %s" % (bad_timestamp, first_timestamp))


class MetricUnaggregatable(Exception):
    """Error raised when metrics can't be aggregated."""

    def __init__(self, metrics, reason):
        self.metrics = metrics
        self.reason = reason
        super(MetricUnaggregatable, self).__init__(
            "Metrics %s can't be aggregated: %s" % (" ,".join(metrics),
                                                    reason))


def _get_driver(name, conf):
    """Return the driver named name.

    :param name: The name of the driver.
    :param conf: The conf to pass to the driver.
    """
    d = driver.DriverManager('gnocchi.storage',
                             name).driver
    return d(conf)


def get_driver(conf):
    """Return the configured driver."""
    return _get_driver(conf.storage.driver,
                       conf.storage)


class StorageDriver(object):
    @staticmethod
    def __init__(conf):
        pass

    @staticmethod
    def create_metric(metric, back_window, archive_policy):
        """Create an metric.

        :param metric: The metric key.
        :param back_window: Number of blocks to allow as back window.
        :param archive_policy: The archive policy to use.
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def add_measures(metric, measures):
        """Add a measure to an metric.

        :param metric: The metric measured.
        :param measures: The actual measures.
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def get_measures(metric, from_timestamp=None, to_timestamp=None,
                     aggregation='mean'):
        """Get a measure to an metric.

        :param metric: The metric measured.
        :param from timestamp: The timestamp to get the measure from.
        :param to timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def delete_metric(metric):
        raise exceptions.NotImplementedError

    @staticmethod
    def get_cross_metric_measures(metrics, from_timestamp=None,
                                  to_timestamp=None, aggregation='mean'):
        """Get aggregated measures of multiple entities.

        :param entities: The entities measured to aggregate.
        :param from timestamp: The timestamp to get the measure from.
        :param to timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        """
        raise exceptions.NotImplementedError
