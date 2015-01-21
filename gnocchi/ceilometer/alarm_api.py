#
# Copyright 2013 Red Hat, Inc
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
import wsme
from wsme import types as wtypes


class AlarmGnocchiThresholdRule(basetypes.AlarmRule):
    resource_type = wsme.wsattr(wtypes.text, mandatory=True)
    "The resource type"

    meter_name = wsme.wsattr(wtypes.text, mandatory=True)
    "The name of the meter"

    resource_id_or_selector = wsme.wsattr(wtypes.text, mandatory=True)
    "The id of a resource or a expression to select multiple resources"

    comparison_operator = basetypes.AdvEnum('comparison_operator', str,
                                            'lt', 'le', 'eq', 'ne', 'ge', 'gt',
                                            default='eq')
    "The comparison against the alarm threshold"

    threshold = wsme.wsattr(float, mandatory=True)
    "The threshold of the alarm"

    statistic = basetypes.AdvEnum('statistic', str, 'max', 'min', 'mean',
                                  'sum', 'count', default='mean')
    "The statistic to compare to the threshold"

    evaluation_periods = wsme.wsattr(wtypes.IntegerType(minimum=1), default=1)
    "The number of historical periods to evaluate the threshold"

    period = wsme.wsattr(wtypes.IntegerType(minimum=1), default=60)
    "The time range in seconds over which query"

    def as_dict(self):
        rule = self.as_dict_from_keys(['period', 'comparison_operator',
                                       'threshold', 'statistic',
                                       'evaluation_periods', 'meter_name',
                                       'resource_id_or_selector',
                                       'resource_type'])
        return rule
