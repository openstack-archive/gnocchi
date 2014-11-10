# -*- encoding: utf-8 -*-
#
# Copyright 2014 Openstack Foundation
#
# Authors: Ana Malagon <atmalagon@gmail.com>
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
from stevedore import extension
import testscenarios

from gnocchi.aggregates import null
from gnocchi.tests import base as tests_base


load_tests = testscenarios.load_tests_apply_scenarios


class TestAggregates(tests_base.TestCase):
    def _load_extensions(self):
        self.mgr = extension.ExtensionManager('gnocchi.aggregates',
                                              invoke_on_load=True)
        self.custom_agg = dict((x.name, x.obj) for x in self.mgr)

    def test_extension_dict(self):
        self._load_extensions()
        self.assertIsInstance(self.custom_agg['null'], null.NullAggregate)
