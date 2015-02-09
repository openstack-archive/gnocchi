# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2015 eNovance
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

import multiprocessing
import os
import uuid

from flask import json as flask_json
from oslo.config import cfg
from oslo.utils import importutils
from oslo_log import log
from oslo_serialization import jsonutils
import pecan
from pecan import templating
import six
from werkzeug import serving
from werkzeug import wsgi

from gnocchi import indexer
from gnocchi import storage


LOG = log.getLogger(__name__)


OPTS = [
    cfg.IntOpt('port',
               default=8041,
               help='The port for the Gnocchi API server.',
               ),
    cfg.StrOpt('host',
               default='0.0.0.0',
               help='The listen IP for the Gnocchi API server.',
               ),
    cfg.BoolOpt('pecan_debug',
                default='$debug',
                help='Toggle Pecan Debug Middleware. '
                'Defaults to global debug value.'
                ),
    cfg.MultiStrOpt('middlewares',
                    default=['keystonemiddleware.auth_token.AuthProtocol'],
                    help='Middlewares to use',),
    cfg.IntOpt('workers', default=1,
               help='Number of workers for Gnocchi API server.'),
]

opt_group = cfg.OptGroup(name='api',
                         title='Options for the gnocchi-api service')
cfg.CONF.register_group(opt_group)
cfg.CONF.register_opts(OPTS, opt_group)


class DBHook(pecan.hooks.PecanHook):

    def __init__(self, storage, indexer):
        self.storage = storage
        self.indexer = indexer

    def on_route(self, state):
        state.request.storage = self.storage
        state.request.indexer = self.indexer


class OsloJSONRenderer(object):
    @staticmethod
    def __init__(path, extra_vars):
        pass

    @staticmethod
    def to_primitive(value, *args, **kwargs):
        # TODO(jd): Remove that once oslo.serialization is released with
        # https://review.openstack.org/#/c/147198/
        if isinstance(value, uuid.UUID):
            return six.text_type(value)
        return jsonutils.to_primitive(value, *args, **kwargs)

    def render(self, template_path, namespace):
        return jsonutils.dumps(namespace, default=self.to_primitive)


class GnocchiJinjaRenderer(templating.JinjaRenderer):
    def __init__(self, *args, **kwargs):
        super(GnocchiJinjaRenderer, self).__init__(*args, **kwargs)
        self.env.filters['tojson'] = flask_json.tojson_filter

    def render(self, template_path, namespace):
        if not isinstance(namespace, dict):
            namespace = dict(data=namespace)
        return super(GnocchiJinjaRenderer, self).render(
            template_path, namespace)


PECAN_CONFIG = {
    'app': {
        'root': 'gnocchi.rest.RootController',
        'modules': ['gnocchi.rest'],
    },
    'conf': cfg.CONF,
}


def setup_app(pecan_config=PECAN_CONFIG):
    conf = pecan_config['conf']
    s = pecan_config.get('storage')
    if not s:
        s = storage.get_driver(conf)
    i = pecan_config.get('indexer')
    if not i:
        i = indexer.get_driver(conf)
    i.connect()

    root_dir = os.path.dirname(os.path.abspath(__file__))

    app = pecan.make_app(
        pecan_config['app']['root'],
        debug=conf.api.pecan_debug,
        hooks=(DBHook(s, i),),
        guess_content_type_from_ext=False,
        custom_renderers={'json': OsloJSONRenderer,
                          'gnocchi_jinja': GnocchiJinjaRenderer},
        default_renderer='gnocchi_jinja',
        template_path=root_dir + "/templates",
    )

    app = wsgi.SharedDataMiddleware(
        app,
        {"/static": root_dir + "/static"},
        cache=not conf.api.pecan_debug)

    for middleware in reversed(pecan_config['conf'].api.middlewares):
        if not middleware:
            continue
        klass = importutils.import_class(middleware)
        app = klass(app, dict(conf))

    return app


def build_server():
    workers = cfg.CONF.api.get('workers')
    if not workers:
        try:
            workers = multiprocessing.cpu_count() or 1
        except NotImplementedError:
            workers = 1
    if workers and workers < 1:
        msg = (_("api.workers value of %(workers)s is invalid, "
                 "must be greater than 0") %
               {'workers': str(workers)})
        raise Exception(msg)

    serving.run_simple(cfg.CONF.api.host, cfg.CONF.api.port,
                       setup_app(), processes=workers)
