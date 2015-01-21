#
# Copyright 2015 eNovance
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

from ceilometer.api.controllers import basetypes
from stevedore import extension
import wsme
from wsme import types as wtypes

from gnocchi import archive_policy


AGGREGATIONS = set()
AGGREGATIONS.update(archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS)
AGGREGATIONS.update(ext.name for ext in
                    extension.ExtensionManager(namespace='gnocchi.aggregates'))


class AlarmGnocchiThresholdRule(basetypes.AlarmRule):
    comparison_operator = basetypes.AdvEnum('comparison_operator', str,
                                            'lt', 'le', 'eq', 'ne', 'ge', 'gt',
                                            default='eq')
    "The comparison against the alarm threshold"

    threshold = wsme.wsattr(float, mandatory=True)
    "The threshold of the alarm"

    aggregation = basetypes.AdvEnum('aggregation', str, *AGGREGATIONS,
                                    default='mean')
    "The aggregation to compare to the threshold"

    evaluation_periods = wsme.wsattr(wtypes.IntegerType(minimum=1), default=1)
    "The number of historical periods to evaluate the threshold"

    granularity = wsme.wsattr(wtypes.IntegerType(minimum=1), default=60)
    "The time range in seconds over which query"


class AlarmGnocchiMetricOfResourcesThresholdRule(AlarmGnocchiThresholdRule):
    metric = wsme.wsattr(wtypes.text, mandatory=True)
    "The name of the metric"

    resource = wsme.wsattr(wtypes.text, mandatory=True)
    "The id of a resource or a expression to select multiple resources"

    resource_type = wsme.wsattr(wtypes.text, mandatory=True)
    "The resource type"

    def as_dict(self):
        rule = self.as_dict_from_keys(['granularity', 'comparison_operator',
                                       'threshold', 'aggregation',
                                       'evaluation_periods',
                                       'metric',
                                       'resource',
                                       'resource_type'])
        return rule


class AlarmGnocchiMetricsThresholdRule(AlarmGnocchiThresholdRule):
    metrics = wsme.wsattr([wtypes.text], mandatory=True)
    "A list of metric Ids"

    def as_dict(self):
        rule = self.as_dict_from_keys(['granularity', 'comparison_operator',
                                       'threshold', 'aggregation',
                                       'evaluation_periods',
                                       'metrics'])
        return rule
