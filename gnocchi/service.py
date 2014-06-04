# Copyright (c) 2013 Mirantis Inc.
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

import logging

from oslo.config import cfg

from gnocchi.openstack.common import log

LOG = log.getLogger(__name__)


def prepare_service():
    cfg.CONF(project='gnocchi')
    log.setup('gnocchi')
    cfg.CONF.log_opt_values(LOG, logging.DEBUG)
