# -*- encoding: utf-8 -*-
#
# Copyright © 2014 eNovance
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
import datetime
import functools
import json
import uuid

import iso8601
from oslo.utils import strutils
from oslo.utils import timeutils
import pecan
from pecan import rest
from pytimeparse import timeparse
import six
from six.moves.urllib import parse as urllib_parse
from stevedore import extension
import voluptuous
import webob.exc
import werkzeug.http

from gnocchi import aggregates
from gnocchi import indexer
from gnocchi.openstack.common import policy
from gnocchi import storage


_ENFORCER = None


def enforce(rule, target):
    """Return the user and project the request should be limited to.

    :param rule: The rule name
    :param target: The target to enforce on.

    """
    global _ENFORCER
    if not _ENFORCER:
        _ENFORCER = policy.Enforcer()

    headers = pecan.request.headers
    creds = {
        'roles': headers.get("X-Roles", "").split(","),
        'user_id': headers.get("X-User-Id"),
        'project_id': headers.get("X-Project-Id"),
    }

    if not _ENFORCER.enforce(rule, target, creds):
        pecan.abort(403)


def set_resp_location_hdr(location):
    # NOTE(sileht): according the pep-3333 the headers must be
    # str in py2 and py3 even this is not the same thing in both
    # version
    # see: http://legacy.python.org/dev/peps/pep-3333/#unicode-issues
    if six.PY2 and isinstance(location, six.text_type):
        location = location.encode('utf-8')
    location = urllib_parse.quote(location)
    pecan.response.headers['Location'] = location


def get_user_and_project():
    return (pecan.request.headers.get('X-User-Id'),
            pecan.request.headers.get('X-Project-Id'))


def deserialize(schema):
    try:
        type, options = werkzeug.http.parse_options_header(
            pecan.request.headers.get('Content-Type'))
        params = json.loads(pecan.request.body.decode(
            options.get('charset', 'ascii')))
    except Exception as e:
        pecan.abort(400, "Unable to decode body" + str(e))
    try:
        return schema(params)
    except voluptuous.Error as e:
        pecan.abort(400, "Invalid input: %s" % e)


def vexpose(schema, *vargs, **vkwargs):
    def expose(f):
        f = pecan.expose(*vargs, **vkwargs)(f)

        @functools.wraps(f)
        def callfunction(*args, **kwargs):
            return f(*args, body=deserialize(schema), **kwargs)
        return callfunction
    return expose


def Timestamp(v):
    if v is None:
        return v
    try:
        v = float(v)
    except (ValueError, TypeError):
        return timeutils.normalize_time(iso8601.parse_date(v))
    return datetime.datetime.utcfromtimestamp(v)


def convert_metric_list(metrics, created_by_user_id, created_by_project_id):
    # Replace an archive policy as value for an metric by a brand
    # a new metric
    new_metrics = {}
    for k, v in six.iteritems(metrics):
        if isinstance(v, uuid.UUID):
            new_metrics[k] = v
        else:
            new_metrics[k] = str(MetricsController.create_metric(
                created_by_user_id, created_by_project_id,
                v['archive_policy']))
    return new_metrics


def PositiveOrNullInt(value):
    value = int(value)
    if value < 0:
        raise ValueError("Value must be positive")
    return value


def PositiveNotNullInt(value):
    value = int(value)
    if value <= 0:
        raise ValueError("Value must be positive and not null")
    return value


def Timespan(value):
    if value is None:
        raise ValueError("Invalid timespan")
    try:
        seconds = timeparse.timeparse(six.text_type(value))
    except Exception:
        raise ValueError("Unable to parse timespan")
    if seconds is None:
        raise ValueError("Unable to parse timespan")
    if seconds <= 0:
        raise ValueError("Timespan must be positive")
    return seconds


def get_details(params):
    type, options = werkzeug.http.parse_options_header(
        pecan.request.headers.get('Accept'))
    try:
        details = strutils.bool_from_string(
            options.get('details', params.pop('details', 'false')),
            strict=True)
    except ValueError as e:
        method = 'Accept' if 'details' in options else 'query'
        pecan.abort(
            400,
            "Unable to parse details value in %s: %s" % (method, str(e)))
    return details


class ArchivePolicyItem(object):
    def __init__(self, granularity=None, points=None, timespan=None):
        if (granularity is not None
           and points is not None
           and timespan is not None):
            if timespan != granularity * points:
                raise ValueError(
                    u"timespan ≠ granularity × points")

        if granularity is None:
            if points is None or timespan is None:
                raise ValueError(
                    "At least two of granularity/points/timespan "
                    "must be provided")
            granularity = round(timespan / float(points))

        if points is None:
            if timespan is None:
                self.timespan = None
            else:
                points = int(timespan / granularity)
                self.timespan = granularity * points
        else:
            self.timespan = granularity * points

        self.points = points
        self.granularity = granularity

    def to_dict(self):
        return {
            'timespan': self.timespan,
            'granularity': self.granularity,
            'points': self.points
        }

    def to_human_readable_dict(self):
        """Return a dict representation with human readable values."""
        return {
            'timespan': six.text_type(
                datetime.timedelta(seconds=self.timespan))
            if self.timespan is not None
            else None,
            'granularity': six.text_type(
                datetime.timedelta(seconds=self.granularity)),
            'points': self.points,
        }

    @classmethod
    def archive_policy_to_human_readable(cls, archive_policy):
        archive_policy['definition'] = [cls(**d).to_human_readable_dict()
                                        for d in archive_policy['definition']]
        return archive_policy


class ArchivePoliciesController(rest.RestController):
    ArchivePolicy = voluptuous.Schema({
        voluptuous.Required("name"): six.text_type,
        voluptuous.Required("back_window", default=0): PositiveOrNullInt,
        voluptuous.Required("definition"):
        voluptuous.All([{
            "granularity": Timespan,
            "points": PositiveNotNullInt,
            "timespan": Timespan,
            }], voluptuous.Length(min=1)),
        })

    @staticmethod
    @vexpose(ArchivePolicy, 'json')
    def post(body):
        # Validate the data
        for ap_def in body['definition']:
            try:
                ArchivePolicyItem(**ap_def)
            except ValueError as e:
                pecan.abort(400, e)
        enforce("create archive policy", body)
        try:
            ap = pecan.request.indexer.create_archive_policy(**body)
        except indexer.ArchivePolicyAlreadyExists as e:
            pecan.abort(409, e)

        location = "/v1/archive_policy/" + ap['name']
        set_resp_location_hdr(location)
        pecan.response.status = 201
        return ArchivePolicyItem.archive_policy_to_human_readable(ap)

    @pecan.expose('json')
    def get_one(self, id):
        ap = pecan.request.indexer.get_archive_policy(id)
        enforce("get archive policy", ap)
        if ap:
            return ArchivePolicyItem.archive_policy_to_human_readable(ap)
        pecan.abort(404)

    @pecan.expose('json')
    def get_all(self):
        enforce("list archive policy", {})
        return list(map(ArchivePolicyItem.archive_policy_to_human_readable,
                        pecan.request.indexer.list_archive_policies()))


class MetricController(rest.RestController):
    _custom_actions = {
        'measures': ['POST', 'GET']
    }

    def __init__(self, metric_id):
        self.metric_id = metric_id
        mgr = extension.ExtensionManager(namespace='gnocchi.aggregates',
                                         invoke_on_load=True)
        self.custom_agg = dict((x.name, x.obj) for x in mgr)

    Measures = voluptuous.Schema([{
        voluptuous.Required("timestamp"):
        Timestamp,
        voluptuous.Required("value"): voluptuous.Any(float, int),
    }])

    def enforce_metric(self, rule):
        metric = pecan.request.indexer.get_metric(self.metric_id)
        if metric is None:
            pecan.abort(404, storage.MetricDoesNotExist(self.metric_id))
        enforce(rule, metric)

    @pecan.expose('json')
    def get_all(self, **kwargs):
        self.enforce_metric("get metric")
        details = get_details(kwargs)
        metric = pecan.request.indexer.get_metric(self.metric_id,
                                                  details=details)
        if not metric:
            pecan.abort(404, storage.MetricDoesNotExist(self.metric_id))

        if details:
            metric['archive_policy'] = (
                ArchivePolicyItem.archive_policy_to_human_readable(
                    metric['archive_policy']))
        return metric

    @vexpose(Measures)
    def post_measures(self, body):
        self.enforce_metric("post measures")
        try:
            pecan.request.storage.add_measures(
                self.metric_id,
                (storage.Measure(
                    m['timestamp'],
                    m['value']) for m in body))
        except storage.MetricDoesNotExist as e:
            pecan.abort(404, str(e))
        except storage.NoDeloreanAvailable as e:
            pecan.abort(400,
                        "The measure for %s is too old considering the "
                        "archive policy used by this metric. "
                        "It can only go back to %s."
                        % (e.bad_timestamp, e.first_timestamp))

    @pecan.expose('json')
    def get_measures(self, start=None, stop=None, aggregation='mean', **param):
        self.enforce_metric("get measures")
        if not (aggregation in storage.AGGREGATION_TYPES or aggregation in
                self.custom_agg):
            msg = '''Invalid aggregation value %(agg)s, must be one of %(std)s
                     or %(custom)s'''
            pecan.abort(400, msg % dict(agg=aggregation,
                                        std=str(storage.AGGREGATION_TYPES),
                                        custom=str(self.custom_agg.keys())))

        if start is not None:
            try:
                start = Timestamp(start)
            except Exception:
                pecan.abort(400, "Invalid value for start")

        if stop is not None:
            try:
                stop = Timestamp(stop)
            except Exception:
                pecan.abort(400, "Invalid value for stop")

        try:
            if aggregation in self.custom_agg:
                measures = self.custom_agg[aggregation].compute(
                    pecan.request.storage, self.metric_id, start, stop,
                    **param)
            else:
                measures = pecan.request.storage.get_measures(self.metric_id,
                                                              start, stop,
                                                              aggregation)
            # Replace timestamp keys by their string versions
            return [(timeutils.strtime(timestamp), offset, v)
                    for timestamp, offset, v in measures]
        except storage.MetricDoesNotExist as e:
            pecan.abort(404, str(e))
        except aggregates.CustomAggFailure as e:
            pecan.abort(400, str(e))

    @pecan.expose()
    def delete(self):
        self.enforce_metric("delete metric")
        try:
            pecan.request.storage.delete_metric(self.metric_id)
        except storage.MetricDoesNotExist as e:
            pecan.abort(404, str(e))
        pecan.request.indexer.delete_metric(self.metric_id)


def UUID(value):
    try:
        return uuid.UUID(value)
    except Exception as e:
        raise ValueError(e)


MetricSchemaDefinition = {
    "user_id": UUID,
    "project_id": UUID,
    voluptuous.Required('archive_policy'): six.text_type,
}


class MetricsController(rest.RestController):
    @staticmethod
    @pecan.expose()
    def _lookup(id, *remainder):
        # That's triggered when accessing /v1/metric/
        if id is "":
            pecan.abort(404)
        return MetricController(id), remainder

    Metric = voluptuous.Schema(MetricSchemaDefinition)

    @staticmethod
    def create_metric(created_by_user_id, created_by_project_id,
                      archive_policy,
                      user_id=None, project_id=None):
        enforce("create metric", {
            "created_by_user_id": created_by_user_id,
            "created_by_project_id": created_by_project_id,
            "user_id": user_id,
            "project_id": project_id,
            "archive_policy": archive_policy,
        })
        id = uuid.uuid4()
        policy = pecan.request.indexer.get_archive_policy(archive_policy)
        if policy is None:
            pecan.abort(400, "Unknown archive policy %s" % archive_policy)
        pecan.request.indexer.create_metric(
            id,
            created_by_user_id, created_by_project_id,
            archive_policy=policy['name'])
        pecan.request.storage.create_metric(
            str(id),
            policy['back_window'],
            [ArchivePolicyItem(**d).to_dict()
             for d in policy['definition']],
        )
        return id

    @vexpose(Metric, 'json')
    def post(self, body):
        user, project = get_user_and_project()
        id = self.create_metric(user, project, **body)
        set_resp_location_hdr("/v1/metric/" + str(id))
        pecan.response.status = 201
        return {"id": str(id),
                "archive_policy": str(body['archive_policy'])}

    @pecan.expose('json')
    def get_all(self):
        try:
            enforce("list all metric", {})
        except webob.exc.HTTPForbidden:
            enforce("list resource", {})
            user_id, project_id = get_user_and_project()
        else:
            user_id, project_id = None, None
        return pecan.request.indexer.list_metrics(
            user_id, project_id)


def UUID(value):
    try:
        return uuid.UUID(value)
    except Exception as e:
        raise ValueError(e)


Metrics = voluptuous.Schema({
    six.text_type: voluptuous.Any(UUID,
                                  MetricsController.Metric),
})


class NamedMetricController(rest.RestController):
    def __init__(self, resource_id, resource_type):
        self.resource_id = resource_id
        self.resource_type = resource_type

    @pecan.expose()
    def _lookup(self, name, *remainder):
        # TODO(jd) There might be an slight optimization to do by using a
        # dedicated driver method rather than get_resource, which might be
        # heavier.
        resource = pecan.request.indexer.get_resource(
            'generic', self.resource_id)
        if name in resource['metrics']:
            return MetricController(resource['metrics'][name]), remainder
        pecan.abort(404)

    @vexpose(Metrics)
    def post(self, body):
        enforce("update resource", {
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
        })
        user, project = get_user_and_project()
        metrics = convert_metric_list(body, user, project)
        try:
            pecan.request.indexer.update_resource(
                self.resource_type, self.resource_id, metrics=metrics,
                append_metrics=True)
        except (indexer.NoSuchMetric, ValueError) as e:
            pecan.abort(400, e)
        except indexer.NamedMetricAlreadyExists as e:
            pecan.abort(409, e)
        except indexer.NoSuchResource as e:
            pecan.abort(404, e)


Metrics = voluptuous.Schema({
    six.text_type: voluptuous.Any(UUID,
                                  MetricsController.Metric),
})


def ResourceSchema(schema):
    base_schema = {
        voluptuous.Required("id"): UUID,
        'started_at': Timestamp,
        'ended_at': Timestamp,
        'user_id': UUID,
        'project_id': UUID,
        'metrics': Metrics,
    }
    base_schema.update(schema)
    return voluptuous.Schema(base_schema)


def ResourcePatchSchema(schema):
    base_schema = {
        'metrics': Metrics,
        'ended_at': Timestamp,
    }
    base_schema.update(schema)
    return voluptuous.Schema(base_schema)


class GenericResourceController(rest.RestController):
    _resource_type = 'generic'

    ResourcePatch = ResourcePatchSchema({})

    def __init__(self, id):
        self.id = id
        self.metric = NamedMetricController(id, self._resource_type)

    @pecan.expose('json')
    def get(self):
        enforce("get resource", {
            "resource_type": self._resource_type,
            "resource_id": self.id,
        })
        resource = pecan.request.indexer.get_resource(
            self._resource_type, self.id)
        if resource:
            return resource
        pecan.abort(404)

    @pecan.expose()
    def patch(self):
        enforce("update resource", {
            "resource_type": self._resource_type,
            "resource_id": self.id,
        })
        # NOTE(jd) Can't use vexpose because it does not take into account
        # inheritance
        body = deserialize(self.ResourcePatch)
        if len(body) == 0:
            # Empty update, just check if the resource exists
            if pecan.request.indexer.get_resource(
                    self._resource_type, self.id):
                return
            pecan.abort(404)

        try:
            if 'metrics' in body:
                user, project = get_user_and_project()
                body['metrics'] = convert_metric_list(
                    body['metrics'], user, project)
            pecan.request.indexer.update_resource(
                self._resource_type,
                self.id, **body)
        except (indexer.NoSuchMetric, ValueError) as e:
            pecan.abort(400, e)
        except indexer.NoSuchResource as e:
            pecan.abort(404, e)

    @pecan.expose()
    def delete(self):
        enforce("delete resource", {
            "resource_type": self._resource_type,
            "resource_id": self.id,
        })
        try:
            pecan.request.indexer.delete_resource(self.id)
        except indexer.NoSuchResource as e:
            pecan.abort(400, str(e))


class SwiftAccountResourceController(GenericResourceController):
    _resource_type = 'swift_account'


class InstanceResourceController(GenericResourceController):
    _resource_type = 'instance'

    ResourcePatch = ResourcePatchSchema({
        "flavor_id": int,
        "image_ref": six.text_type,
        "host": six.text_type,
        "display_name": six.text_type,
        "server_group": six.text_type,
    })


class GenericResourcesController(rest.RestController):
    _resource_type = 'generic'
    _resource_rest_class = GenericResourceController

    Resource = ResourceSchema({})

    @pecan.expose()
    def _lookup(self, id, *remainder):
        return self._resource_rest_class(id), remainder

    @pecan.expose('json')
    def post(self):
        enforce("create resource", {
            "resource_type": self._resource_type,
        })
        # NOTE(jd) Can't use vexpose because it does not take into account
        # inheritance
        body = deserialize(self.Resource)
        user, project = get_user_and_project()
        body['metrics'] = convert_metric_list(
            body.get('metrics', {}), user, project)
        rid = body['id']
        del body['id']
        try:
            resource = pecan.request.indexer.create_resource(
                self._resource_type, rid, user, project,
                **body)
        except (ValueError, indexer.NoSuchMetric) as e:
            pecan.abort(400, e)
        except indexer.ResourceAlreadyExists as e:
            pecan.abort(409, e)
        set_resp_location_hdr("/v1/resource/"
                              + self._resource_type + "/"
                              + resource['id'])
        pecan.response.status = 201
        return resource

    @pecan.expose('json')
    def get_all(self, **kwargs):
        try:
            enforce("list all resource", {
                "resource_type": self._resource_type,
            })
        except webob.exc.HTTPForbidden:
            enforce("list resource", {
                "resource_type": self._resource_type,
            })
            kwargs['user_id'], kwargs['project_id'] = get_user_and_project()

        started_after = kwargs.pop('started_after', None)
        ended_before = kwargs.pop('ended_before', None)
        details = get_details(kwargs)

        if started_after is not None:
            try:
                started_after = Timestamp(started_after)
            except Exception:
                pecan.abort(400, "Unable to parse started_after timestamp")
        if ended_before is not None:
            try:
                ended_before = Timestamp(ended_before)
            except Exception:
                pecan.abort(400, "Unable to parse ended_before timestamp")
        # Transform empty string to None (NULL)
        for k, v in six.iteritems(kwargs):
            if v == '':
                kwargs[k] = None
        try:
            return pecan.request.indexer.list_resources(
                self._resource_type,
                started_after=started_after,
                ended_before=ended_before,
                attributes_filter=kwargs,
                details=details)
        except indexer.ResourceAttributeError as e:
            pecan.abort(400, e)


class SwiftAccountsResourcesController(GenericResourcesController):
    _resource_type = 'swift_account'
    _resource_rest_class = SwiftAccountResourceController


class InstancesResourcesController(GenericResourcesController):
    _resource_type = 'instance'
    _resource_rest_class = InstanceResourceController

    Resource = ResourceSchema({
        voluptuous.Required("flavor_id"): int,
        voluptuous.Required("image_ref"): six.text_type,
        voluptuous.Required("host"): six.text_type,
        voluptuous.Required("display_name"): six.text_type,
        "server_group": six.text_type,
    })


class ResourcesController(rest.RestController):
    generic = GenericResourcesController()
    instance = InstancesResourcesController()
    swift_account = SwiftAccountsResourcesController()


class V1Controller(object):
    archive_policy = ArchivePoliciesController()
    metric = MetricsController()
    resource = ResourcesController()


class RootController(object):
    v1 = V1Controller()

    @staticmethod
    @pecan.expose(content_type="text/plain")
    def index():
        return "Nom nom nom."
