# Copyright 2014: Mirantis Inc.
# All Rights Reserved.
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


class OpenTSDBError(Exception):
    def __init__(self, msg=None):
        if msg is None:
            msg = 'Unknown OpenTSDB error occurred. \n %s \n %s'
        super(OpenTSDBError, self).__init__(msg)


class InvalidOpenTSDBFormat(OpenTSDBError):
    """Error raised when data posted to OpenTSDB has the invalid format."""
    def __init__(self, actual, expected=None):
        msg = 'Data %s has the unexpected by openTSDB format.' % actual
        if expected:
            msg += ' Please provide data in %s format.'
        super(InvalidOpenTSDBFormat, self).__init__(msg)
        self.actual = actual
        self.expected = expected
