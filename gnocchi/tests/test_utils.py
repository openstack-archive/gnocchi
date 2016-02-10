# -*- encoding: utf-8 -*-
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
import datetime
import os

import iso8601
import mock

from gnocchi import utils
from gnocchi.tests import base as tests_base


class TestUtils(tests_base.TestCase):
    def test_datetime_to_unix_timezone_change(self):
        dt = datetime.datetime(2015, 1, 1, 23, 34, tzinfo=iso8601.iso8601.UTC)
        expected = 1420155240.0

        self.assertEqual(expected, utils.datetime_to_unix(dt))
        with mock.patch.dict(os.environ, {'TZ': 'UTC'}):
            self.assertEqual(expected, utils.datetime_to_unix(dt))
        with mock.patch.dict(os.environ, {'TZ': 'Europe/Paris'}):
            self.assertEqual(expected, utils.datetime_to_unix(dt))
        with mock.patch.dict(os.environ, {'TZ': 'US/Eastern'}):
            self.assertEqual(expected, utils.datetime_to_unix(dt))
