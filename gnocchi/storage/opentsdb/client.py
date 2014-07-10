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

from oslo.config import cfg
import requests

from gnocchi.openstack.common import jsonutils
from gnocchi.openstack.common import log
from gnocchi.storage import opentsdb

LOG = log.getLogger(__name__)

cfg.CONF.import_opt('debug', 'gnocchi.openstack.common.log')

OPENTSDB_OPTS = [
    cfg.IntOpt('opentsdb_port',
               default=4242,
               help='The port for the OpenTSDB server.',
               ),
    cfg.StrOpt('opentsdb_host',
               default='127.0.0.1',
               help='The listen IP for the OpenTSDB REST API interface.',
               ),
]

cfg.CONF.register_opts(OPENTSDB_OPTS, group="storage")

STATS_TEMPL = 'http://%(host)s:%(port)s/api/stats'
PUT_TEMPL = 'http://%(host)s:%(port)s/api/put?details'
META_TEMPL = 'http://%(host)s:%(port)s/api/uid/tsmeta?tsuid=%(tsuid)s'
CONF_TEMPL = 'http://%(host)s:%(oprt)s/api/config'
AGGR_TEMPL = 'http://%(host)s:%(port)s/api/aggregators'
VERSION_TEMPL = 'http://%(host)s:%(port)s/api/version'
QUERY_TEMPL = 'http://%(host)s:%(port)s/api/query?%(query)s'


def get_statistics():
    """Returns info about what metrics are registered and with what stats."""
    req = requests.get(STATS_TEMPL % {'host': cfg.CONF.storage.opentsdb_host,
                                      'port': cfg.CONF.storage.opentsdb_port})
    return req


def put_meter(meters):
    """Post new meter(s) to the database.

    Meter dictionary *should* contain the following four required fields:
      - metric: the name of the metric you are storing
      - timestamp: a Unix epoch style timestamp in seconds or milliseconds.
                   The timestamp must not contain non-numeric characters.
      - value: the value to record for this data point. It may be quoted or
               not quoted and must conform to the OpenTSDB value rules.
      - tags: a map of tag name/tag value pairs. At least one pair must be
              supplied.
    """
    res = []
    if type(meters) == dict:
        meters = [meters]
    for meter_dict in meters:
        if (set(meter_dict.keys())
                != set(['metric', 'timestamp', 'value', 'tags'])):
            raise opentsdb.InvalidOpenTSDBFormat(
                actual=meter_dict,
                expected="{'metric': <meter_name>, 'timestamp': <ts>, "
                         "'value': <value>, 'tags': <at least one pair>}")

        req = requests.post(PUT_TEMPL %
                            {'host': cfg.CONF.storage.opentsdb_host,
                             'port': cfg.CONF.storage.opentsdb_port},
                            data=jsonutils.dumps(meter_dict))
        res.append(req)
    return res


def define_retention(tsuid, retention_days):
    """Set retention days for the defined by ID timeseries.

    NOTE: currently not working directly through the REST API.

    :param tsuid: hexadecimal representation of the timeseries UID
    :param retention_days: number of days of data points to retain for the
                           given timeseries. When set to 0, the default, data
                           is retained indefinitely.
    """
    meta_data = {'tsuid': tsuid, 'retention': retention_days}
    req = requests.post(META_TEMPL % {'host': cfg.CONF.storage.opentsdb_host,
                                      'port': cfg.CONF.storage.opentsdb_port,
                                      'tsuid': tsuid},
                        data=jsonutils.dumps(meta_data))
    return req


def get_aggregators():
    """Used to get the list of default aggregation functions."""
    req = requests.get(AGGR_TEMPL % {'host': cfg.CONF.storage.opentsdb_host,
                                     'port': cfg.CONF.storage.opentsdb_port})
    return req


def get_version():
    """Used to check OpenTSDB version.

    That might be needed in case of unknown bugs - this code is written only
    for the 2.x REST API version, so some of the failures might refer to the
    wrong OpenTSDB version installed.
    """
    req = requests.get(VERSION_TEMPL % {
        'host': cfg.CONF.storage.opentsdb_host,
        'port': cfg.CONF.storage.opentsdb_port})
    return req


def _make_query(query, verb):
    meth = getattr(requests, verb.lower(), None)
    if meth is None:
        pass
    req = meth(QUERY_TEMPL % {'host': cfg.CONF.storage.opentsdb_host,
                              'port': cfg.CONF.storage.opentsdb_port,
                              'query': query})
    return req


def get_query(query):
    return _make_query(query, 'get')


def process_response(resp):
    try:
        res = jsonutils.loads(resp.text)
    except Exception:
        raise opentsdb.OpenTSDBError(resp.text)

    if 'errors' in res:
        raise opentsdb.OpenTSDBError(res['error'])

    return res
