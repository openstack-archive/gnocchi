# -*- encoding: utf-8 -*-
#
# Copyright © 2016 Red Hat, Inc.
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
import os

# HACK(jd) Not sure why but Sphinx setup this multiple times, so we just avoid
# doing several times the requests by using this global variable :(
_RUN = False


def setup(app):
    global _RUN
    if _RUN:
        return
    # FIXME(jd) Do that in Python!
    os.system(
        "swg2rst doc/source/swagger.yaml --output doc/source/rest-api-def.rst")
    _RUN = True
