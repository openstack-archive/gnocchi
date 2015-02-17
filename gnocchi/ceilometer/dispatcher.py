#
# Copyright 2014 eNovance
#
# Authors: Julien Danjou <julien@danjou.info>
#          Mehdi Abaakouk <mehdi.abaakouk@enovance.com>
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
from __future__ import absolute_import
import fnmatch

import itertools
import json
import operator
import os
import yaml

from ceilometer import dispatcher
from ceilometer.i18n import _
from oslo.config import cfg
from oslo_log import log
import requests
import six
import stevedore.dispatch

from gnocchi.ceilometer import utils

LOG = log.getLogger(__name__)

dispatcher_opts = [
    cfg.BoolOpt('filter_service_activity',
                default=True,
                help='Filter out samples generated by Gnocchi '
                'service activity'),
    cfg.StrOpt('filter_user',
               default='gnocchi',
               help='Gnocchi user used to filter out samples '
               'generated by Gnocchi service activity'),
    cfg.StrOpt('url',
               default="http://localhost:8041",
               help='URL to Gnocchi.'),
    cfg.StrOpt('archive_policy',
               default="low",
               help='The archive policy to use when the dispatcher '
               'create a new metric.'),
    cfg.StrOpt('archive_policy_file',
               default='/etc/ceilometer/gnocchi_archive_policy_map.yaml',
               help=_('The Yaml file that defines per metric archive '
                      'policies.')),
]

cfg.CONF.register_opts(dispatcher_opts, group="dispatcher_gnocchi")


class UnexpectedWorkflowError(Exception):
    pass


class NoSuchMetric(Exception):
    pass


class MetricAlreadyExists(Exception):
    pass


class NoSuchResource(Exception):
    pass


class ResourceAlreadyExists(Exception):
    pass


def log_and_ignore_unexpected_workflow_error(func):
    def log_and_ignore(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except UnexpectedWorkflowError as e:
            LOG.error(six.text_type(e))
    return log_and_ignore


class GnocchiDispatcher(dispatcher.Base):
    def __init__(self, conf):
        super(GnocchiDispatcher, self).__init__(conf)
        self.filter_service_activity = (
            conf.dispatcher_gnocchi.filter_service_activity)

        self._ks_client = utils.get_keystone_client()
        if self.filter_service_activity:
            try:
                self.gnocchi_user_id = self._ks_client.users.find(
                    name=conf.dispatcher_gnocchi.filter_user)
            except Exception:
                LOG.exception('fail to retreive user of Gnocchi service')
                raise

        self.gnocchi_url = conf.dispatcher_gnocchi.url
        self.gnocchi_archive_policy_default = \
            conf.dispatcher_gnocchi.archive_policy
        self.gnocchi_archive_policy_data = self._load_archive_policy(conf)
        self.mgmr = stevedore.dispatch.DispatchExtensionManager(
            'gnocchi.ceilometer.resource', lambda x: True,
            invoke_on_load=True)

    def _get_headers(self, content_type="application/json"):
        return {
            'Content-Type': content_type,
            'X-Auth-Token': self._ks_client.auth_token,
        }

    def _load_archive_policy(self, conf):
        policy_config_file = self._get_config_file(conf)
        data = {}
        if policy_config_file is not None:
            with open(policy_config_file) \
                    as data_file:
                try:
                    data = yaml.safe_load(data_file)
                except ValueError:
                    data = {}
        return data

    def get_archive_policy(self, metric_name):

        archive_policy = {}
        if self.gnocchi_archive_policy_data is not None:
            policy_match = self._match_metric(metric_name)
            archive_policy['archive_policy_name'] = \
                policy_match or self.gnocchi_archive_policy_default
        else:
            LOG.debug(_("No archive policy file found!"
                      " Using default config."))
            archive_policy['archive_policy_name'] = \
                self.gnocchi_archive_policy_default

        return archive_policy

    @staticmethod
    def _get_config_file(conf):
        config_file = conf.dispatcher_gnocchi.archive_policy_file
        if not os.path.exists(config_file):
            config_file = cfg.CONF.find_file(config_file)
        return config_file

    def _match_metric(self, metric_name):
        for metric, policy in enumerate(self.gnocchi_archive_policy_data):
            # Support wild cards such as disk.*
            if fnmatch.fnmatch(metric_name, metric):
                return policy

    def _is_gnocchi_activity(self, sample):
        return (self.filter_service_activity
                and sample['user_id'] == self.gnocchi_user_id)

    def record_metering_data(self, data):
        # NOTE(sileht): skip sample generated by gnocchi itself
        data = [s for s in data if not self._is_gnocchi_activity(s)]

        # FIXME(sileht): This method bulk the processing of samples
        # grouped by resource_id and metric_name but this is not
        # efficient yet because the data received here doesn't often
        # contains a lot of different kind of samples
        # So perhaps the next step will be to pool the received data from
        # message bus.

        resource_grouped_samples = itertools.groupby(
            data, key=operator.itemgetter('resource_id'))

        for resource_id, samples_of_resource in resource_grouped_samples:
            resource_need_to_be_updated = True

            metric_grouped_samples = itertools.groupby(
                list(samples_of_resource),
                key=operator.itemgetter('counter_name'))
            for metric_name, samples in metric_grouped_samples:
                for ext in self.mgmr:
                    if metric_name in ext.obj.get_metrics_names():
                        self._process_samples(
                            ext, resource_id, metric_name, list(samples),
                            resource_need_to_be_updated)

                # FIXME(sileht): Does it reasonable to skip the resource
                # update here ? Does differents kind of counter_name
                # can have different metadata set ?
                # (ie: one have only flavor_id, and an other one have only
                # image_ref ?)
                #
                # resource_need_to_be_updated = False

    @log_and_ignore_unexpected_workflow_error
    def _process_samples(self, ext, resource_id, metric_name, samples,
                         resource_need_to_be_updated):

        resource_type = ext.name
        measure_attributes = [{'timestamp': sample['timestamp'],
                               'value': sample['counter_volume']}
                              for sample in samples]

        try:
            self._post_measure(resource_type, resource_id, metric_name,
                               measure_attributes)
        except NoSuchMetric:
            # NOTE(sileht): we try first to create the resource, because
            # they more chance that the resource doesn't exists than the metric
            # is missing, the should be reduce the number of resource API call
            resource_attributes = self._get_resource_attributes(
                ext, resource_id, metric_name, samples)
            try:
                self._create_resource(resource_type, resource_id,
                                      resource_attributes)
            except ResourceAlreadyExists:
                try:
                    self._create_metric(resource_type, resource_id,
                                        metric_name)
                except MetricAlreadyExists:
                    # NOTE(sileht): Just ignore the metric have been created in
                    # the meantime.
                    pass
            else:
                # No need to update it we just created it
                # with everything we need
                resource_need_to_be_updated = False

            # NOTE(sileht): we retry to post the measure but if it fail we
            # don't catch the exception to just log it and continue to process
            # other samples
            self._post_measure(resource_type, resource_id, metric_name,
                               measure_attributes)

        if resource_need_to_be_updated:
            resource_attributes = self._get_resource_attributes(
                ext, resource_id, metric_name, samples, for_update=True)
            self._update_resource(resource_type, resource_id,
                                  resource_attributes)

    def _get_resource_attributes(self, ext, resource_id, metric_name, samples,
                                 for_update=False):
        # FIXME(sileht): Should I merge attibutes of all samples ?
        # Or keep only the last one is sufficient ?
        attributes = ext.obj.get_resource_extra_attributes(
            samples[-1])
        if not for_update:
            attributes["id"] = resource_id
            attributes["user_id"] = samples[-1]['user_id']
            attributes["project_id"] = samples[-1]['project_id']
            attributes["metrics"] = dict(
                (metric_name, self.get_archive_policy(metric_name))
                for metric_name in ext.obj.get_metrics_names()
            )
        return attributes

    def _post_measure(self, resource_type, resource_id, metric_name,
                      measure_attributes):
        r = requests.post("%s/v1/resource/%s/%s/metric/%s/measures"
                          % (self.gnocchi_url, resource_type, resource_id,
                             metric_name),
                          headers=self._get_headers(),
                          data=json.dumps(measure_attributes))
        if r.status_code == 404:
            LOG.debug(_("The metric %(metric_name)s of "
                        "resource %(resource_id)s doesn't exists"
                        "%(status_code)d"),
                      {'metric_name': metric_name,
                       'resource_id': resource_id,
                       'status_code': r.status_code})
            raise NoSuchMetric
        elif int(r.status_code / 100) != 2:
            raise UnexpectedWorkflowError(
                _("Fail to post measure on metric %(metric_name)s of "
                  "resource %(resource_id)s with status: "
                  "%(status_code)d: %(msg)s") %
                {'metric_name': metric_name,
                 'resource_id': resource_id,
                 'status_code': r.status_code,
                 'msg': r.text})
        else:
            LOG.debug("Measure posted on metric %s of resource %s",
                      metric_name, resource_id)

    def _create_resource(self, resource_type, resource_id,
                         resource_attributes):
        r = requests.post("%s/v1/resource/%s"
                          % (self.gnocchi_url, resource_type),
                          headers=self._get_headers(),
                          data=json.dumps(resource_attributes))
        if r.status_code == 409:
            LOG.debug("Resource %s already exists", resource_id)
            raise ResourceAlreadyExists

        elif int(r.status_code / 100) != 2:
            raise UnexpectedWorkflowError(
                _("Resource %(resource_id)s creation failed with "
                  "status: %(status_code)d: %(msg)s") %
                {'resource_id': resource_id,
                 'status_code': r.status_code,
                 'msg': r.text})
        else:
            LOG.debug("Resource %s created", resource_id)

    def _update_resource(self, resource_type, resource_id,
                         resource_attributes):
        r = requests.patch(
            "%s/v1/resource/%s/%s"
            % (self.gnocchi_url, resource_type, resource_id),
            headers=self._get_headers(),
            data=json.dumps(resource_attributes))

        if int(r.status_code / 100) != 2:
            raise UnexpectedWorkflowError(
                _("Resource %(resource_id)s update failed with "
                  "status: %(status_code)d: %(msg)s") %
                {'resource_id': resource_id,
                 'status_code': r.status_code,
                 'msg': r.text})
        else:
            LOG.debug("Resource %s updated", resource_id)

    def _create_metric(self, resource_type, resource_id, metric_name):
        params = {metric_name: self.get_archive_policy(metric_name)}
        r = requests.post("%s/v1/resource/%s/%s/metric"
                          % (self.gnocchi_url, resource_type,
                             resource_id),
                          headers=self._get_headers(),
                          data=json.dumps(params))
        if r.status_code == 409:
            LOG.debug("Metric %s of resource %s already exists",
                      metric_name, resource_id)
            raise MetricAlreadyExists

        elif int(r.status_code / 100) != 2:
            raise UnexpectedWorkflowError(
                _("Fail to create metric %(metric_name)s of "
                  "resource %(resource_id)s with status: "
                  "%(status_code)d: %(msg)s") %
                {'metric_name': metric_name,
                 'resource_id': resource_id,
                 'status_code': r.status_code,
                 'msg': r.text})
        else:
            LOG.debug("Metric %s of resource %s created",
                      metric_name, resource_id)

    @staticmethod
    def record_events(events):
        raise NotImplementedError
