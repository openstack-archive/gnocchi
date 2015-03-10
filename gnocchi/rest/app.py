# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2015 eNovance
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
import uuid

from flask import json as flask_json
import keystonemiddleware.auth_token
from oslo.utils import importutils
from oslo_log import log
from oslo_serialization import jsonutils
import pecan
from pecan import templating
import six
from werkzeug import serving
from werkzeug import wsgi

from gnocchi import indexer
from gnocchi import service
from gnocchi import storage


LOG = log.getLogger(__name__)


class GnocchiHook(pecan.hooks.PecanHook):

    def __init__(self, storage, indexer, conf):
        self.storage = storage
        self.indexer = indexer
        self.conf = conf

    def on_route(self, state):
        state.request.storage = self.storage
        state.request.indexer = self.indexer
        state.request.conf = self.conf


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
        namespace = dict(data=namespace)
        return super(GnocchiJinjaRenderer, self).render(
            template_path, namespace)


PECAN_CONFIG = {
    'app': {
        'root': 'gnocchi.rest.RootController',
        'modules': ['gnocchi.rest'],
    },
}


def setup_app(config=PECAN_CONFIG, cfg=None):
    if cfg is None:
        # NOTE(jd) That sucks but pecan forces us to use kwargs :(
        raise RuntimeError("Config is actually mandatory")
    s = config.get('storage')
    if not s:
        s = storage.get_driver(cfg)
    i = config.get('indexer')
    if not i:
        i = indexer.get_driver(cfg)
    i.connect()

    root_dir = os.path.dirname(os.path.abspath(__file__))

    # NOTE(sileht): pecan debug won't work in multi-process environment
    pecan_debug = cfg.api.pecan_debug
    if cfg.api.workers != 1 and pecan_debug:
        pecan_debug = False
        LOG.warning('pecan_debug cannot be enabled, if workers is > 1, '
                    'the value is overrided with False')

    app = pecan.make_app(
        config['app']['root'],
        debug=pecan_debug,
        hooks=(GnocchiHook(s, i, cfg),),
        guess_content_type_from_ext=False,
        custom_renderers={'json': OsloJSONRenderer,
                          'gnocchi_jinja': GnocchiJinjaRenderer},
        default_renderer='gnocchi_jinja',
        template_path=root_dir + "/templates",
    )

    app = wsgi.SharedDataMiddleware(
        app,
        {"/static": root_dir + "/static"},
        cache=not cfg.api.pecan_debug)

    for middleware in reversed(cfg.api.middlewares):
        if not middleware:
            continue
        klass = importutils.import_class(middleware)
        # FIXME(jd) Find a way to remove that special handling…
        if klass == keystonemiddleware.auth_token.AuthProtocol:
            middleware_config = dict(cfg.keystone_authtoken)
        else:
            middleware_config = dict(cfg)
        app = klass(app, middleware_config)

    return app


def build_server():
    conf = service.prepare_service()
    serving.run_simple(conf.api.host, conf.api.port,
                       setup_app(cfg=conf),
                       processes=conf.api.workers)
