#
# Copyright 2015 Red Hat. All Rights Reserved.
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

"""A test module to exercise the Gnocchi API with gabbi."""

import os

from gabbi import driver

from gnocchi.tests.gabbi import fixtures


TESTS_DIR = 'gabbits'


def load_tests(loader, tests, pattern):
    """Provide a TestSuite to the discovery process."""
    test_dir = os.path.join(os.path.dirname(__file__), TESTS_DIR)
    host = os.getenv('GABBI_GNOCCHI_HOST')
    if host:
        port = os.getenv('GABBI_GNOCCHI_PORT', 8041)
        return driver.build_tests(test_dir, loader,
                                  host=host, port=port)
    else:
        return driver.build_tests(test_dir, loader, host=None,
                                  intercept=fixtures.setup_app,
                                  fixture_module=fixtures)
