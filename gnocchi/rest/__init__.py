# -*- encoding: utf-8 -*-
#
# Copyright © 2016 Red Hat, Inc.
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
import itertools
import uuid

from oslo_utils import strutils
import pecan
from pecan import rest
import six
from six.moves.urllib import parse as urllib_parse
from stevedore import extension
import voluptuous
import webob.exc
import werkzeug.http

from gnocchi import aggregates
from gnocchi import archive_policy
from gnocchi import indexer
from gnocchi import json
from gnocchi import storage
from gnocchi import utils


def arg_to_list(value):
    if isinstance(value, list):
        return value
    elif value:
        return [value]
    return []


def abort(status_code, detail='', headers=None, comment=None, **kw):
    """Like pecan.abort, but make sure detail is a string."""
    if status_code == 404 and not detail:
        raise RuntimeError("http code 404 must have 'detail' set")
    return pecan.abort(status_code, six.text_type(detail),
                       headers, comment, **kw)


def get_user_and_project():
    headers = pecan.request.headers
    user_id = headers.get("X-User-Id")
    project_id = headers.get("X-Project-Id")
    return (user_id, project_id)


# TODO(jd) Move this to oslo.utils as I stole it from Ceilometer
def recursive_keypairs(d, separator='.'):
    """Generator that produces sequence of keypairs for nested dictionaries."""
    for name, value in sorted(six.iteritems(d)):
        if isinstance(value, dict):
            for subname, subvalue in recursive_keypairs(value, separator):
                yield ('%s%s%s' % (name, separator, subname), subvalue)
        else:
            yield name, value


def enforce(rule, target):
    """Return the user and project the request should be limited to.

    :param rule: The rule name
    :param target: The target to enforce on.

    """
    headers = pecan.request.headers
    user_id, project_id = get_user_and_project()
    creds = {
        'roles': headers.get("X-Roles", "").split(","),
        'user_id': user_id,
        'project_id': project_id
    }

    if not isinstance(target, dict):
        target = target.__dict__

    # Flatten dict
    target = dict(recursive_keypairs(target))

    if not pecan.request.policy_enforcer.enforce(rule, target, creds):
        abort(403)


def _get_list_resource_policy_filter(rule, resource_type, user, project):
    try:
        # Check if the policy allows the user to list any resource
        enforce(rule, {
            "resource_type": resource_type,
        })
    except webob.exc.HTTPForbidden:
        policy_filter = []
        try:
            # Check if the policy allows the user to list resources linked
            # to their project
            enforce(rule, {
                "resource_type": resource_type,
                "project_id": project,
            })
        except webob.exc.HTTPForbidden:
            pass
        else:
            policy_filter.append({"=": {"project_id": project}})
        try:
            # Check if the policy allows the user to list resources linked
            # to their created_by_project
            enforce(rule, {
                "resource_type": resource_type,
                "created_by_project_id": project,
            })
        except webob.exc.HTTPForbidden:
            pass
        else:
            policy_filter.append({"=": {"created_by_project_id": project}})

        if not policy_filter:
            # We need to have at least one policy filter in place
            abort(403, "Insufficient privileges")

        return {"or": policy_filter}


def set_resp_location_hdr(location):
    location = '%s%s' % (pecan.request.script_name, location)
    # NOTE(sileht): according the pep-3333 the headers must be
    # str in py2 and py3 even this is not the same thing in both
    # version
    # see: http://legacy.python.org/dev/peps/pep-3333/#unicode-issues
    if six.PY2 and isinstance(location, six.text_type):
        location = location.encode('utf-8')
    location = urllib_parse.quote(location)
    pecan.response.headers['Location'] = location


def deserialize():
    mime_type, options = werkzeug.http.parse_options_header(
        pecan.request.headers.get('Content-Type'))
    if mime_type != "application/json":
        abort(415)
    try:
        params = json.load(pecan.request.body_file_raw,
                           encoding=options.get('charset', 'ascii'))
    except Exception as e:
        abort(400, "Unable to decode body: " + six.text_type(e))
    return params


def deserialize_and_validate(schema, required=True):
    try:
        return voluptuous.Schema(schema, required=required)(
            deserialize())
    except voluptuous.Error as e:
        abort(400, "Invalid input: %s" % e)


def Timestamp(v):
    t = utils.to_timestamp(v)
    if t < utils.unix_universal_start:
        raise ValueError("Timestamp must be after Epoch")
    return t


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
    return utils.to_timespan(value).total_seconds()


def get_header_option(name, params):
    type, options = werkzeug.http.parse_options_header(
        pecan.request.headers.get('Accept'))
    try:
        return strutils.bool_from_string(
            options.get(name, params.pop(name, 'false')),
            strict=True)
    except ValueError as e:
        method = 'Accept' if name in options else 'query'
        abort(
            400,
            "Unable to parse %s value in %s: %s"
            % (name, method, six.text_type(e)))


def get_history(params):
    return get_header_option('history', params)


def get_details(params):
    return get_header_option('details', params)


RESOURCE_DEFAULT_PAGINATION = ['revision_start:asc',
                               'started_at:asc']


def get_pagination_options(params, default):
    max_limit = pecan.request.conf.api.max_limit
    limit = params.get('limit', max_limit)
    marker = params.get('marker')
    sorts = params.get('sort', default)
    if not isinstance(sorts, list):
        sorts = [sorts]

    try:
        limit = PositiveNotNullInt(limit)
    except ValueError:
        abort(400, "Invalid 'limit' value: %s" % params.get('limit'))

    limit = min(limit, max_limit)

    return {'limit': limit,
            'marker': marker,
            'sorts': sorts}


def ValidAggMethod(value):
    value = six.text_type(value)
    if value in archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS_VALUES:
        return value
    raise ValueError("Invalid aggregation method")


class ArchivePolicyController(rest.RestController):
    def __init__(self, archive_policy):
        self.archive_policy = archive_policy

    @pecan.expose('json')
    def get(self):
        ap = pecan.request.indexer.get_archive_policy(self.archive_policy)
        if ap:
            enforce("get archive policy", ap)
            return ap
        abort(404, indexer.NoSuchArchivePolicy(self.archive_policy))

    @pecan.expose()
    def delete(self):
        # NOTE(jd) I don't think there's any point in fetching and passing the
        # archive policy here, as the rule is probably checking the actual role
        # of the user, not the content of the AP.
        enforce("delete archive policy", {})
        try:
            pecan.request.indexer.delete_archive_policy(self.archive_policy)
        except indexer.NoSuchArchivePolicy as e:
            abort(404, e)
        except indexer.ArchivePolicyInUse as e:
            abort(400, e)


class ArchivePoliciesController(rest.RestController):
    @pecan.expose()
    def _lookup(self, archive_policy, *remainder):
        return ArchivePolicyController(archive_policy), remainder

    @pecan.expose('json')
    def post(self):
        # NOTE(jd): Initialize this one at run-time because we rely on conf
        conf = pecan.request.conf
        enforce("create archive policy", {})
        ArchivePolicySchema = voluptuous.Schema({
            voluptuous.Required("name"): six.text_type,
            voluptuous.Required("back_window", default=0): PositiveOrNullInt,
            voluptuous.Required(
                "aggregation_methods",
                default=set(conf.archive_policy.default_aggregation_methods)):
            [ValidAggMethod],
            voluptuous.Required("definition"):
            voluptuous.All([{
                "granularity": Timespan,
                "points": PositiveNotNullInt,
                "timespan": Timespan,
                }], voluptuous.Length(min=1)),
            })

        body = deserialize_and_validate(ArchivePolicySchema)
        # Validate the data
        try:
            ap = archive_policy.ArchivePolicy.from_dict(body)
        except ValueError as e:
            abort(400, e)
        enforce("create archive policy", ap)
        try:
            ap = pecan.request.indexer.create_archive_policy(ap)
        except indexer.ArchivePolicyAlreadyExists as e:
            abort(409, e)

        location = "/archive_policy/" + ap.name
        set_resp_location_hdr(location)
        pecan.response.status = 201
        return ap

    @pecan.expose('json')
    def get_all(self):
        enforce("list archive policy", {})
        return pecan.request.indexer.list_archive_policies()


class ArchivePolicyRulesController(rest.RestController):
    @pecan.expose('json')
    def post(self):
        enforce("create archive policy rule", {})
        ArchivePolicyRuleSchema = voluptuous.Schema({
            voluptuous.Required("name"): six.text_type,
            voluptuous.Required("metric_pattern"): six.text_type,
            voluptuous.Required("archive_policy_name"): six.text_type,
            })

        body = deserialize_and_validate(ArchivePolicyRuleSchema)
        enforce("create archive policy rule", body)
        try:
            ap = pecan.request.indexer.create_archive_policy_rule(
                body['name'], body['metric_pattern'],
                body['archive_policy_name']
            )
        except indexer.ArchivePolicyRuleAlreadyExists as e:
            abort(409, e)

        location = "/archive_policy_rule/" + ap.name
        set_resp_location_hdr(location)
        pecan.response.status = 201
        return ap

    @pecan.expose('json')
    def get_one(self, name):
        ap = pecan.request.indexer.get_archive_policy_rule(name)
        if ap:
            enforce("get archive policy rule", ap)
            return ap
        abort(404, indexer.NoSuchArchivePolicyRule(name))

    @pecan.expose('json')
    def get_all(self):
        enforce("list archive policy rule", {})
        return pecan.request.indexer.list_archive_policy_rules()

    @pecan.expose()
    def delete(self, name):
        # NOTE(jd) I don't think there's any point in fetching and passing the
        # archive policy rule here, as the rule is probably checking the actual
        # role of the user, not the content of the AP rule.
        enforce("delete archive policy rule", {})
        try:
            pecan.request.indexer.delete_archive_policy_rule(name)
        except indexer.NoSuchArchivePolicyRule as e:
            abort(404, e)
        except indexer.ArchivePolicyRuleInUse as e:
            abort(400, e)


class AggregatedMetricController(rest.RestController):
    _custom_actions = {
        'measures': ['GET']
    }

    def __init__(self, metric_ids):
        self.metric_ids = metric_ids

    @pecan.expose('json')
    def get_measures(self, start=None, stop=None, aggregation='mean',
                     granularity=None, needed_overlap=100.0):
        return self.get_cross_metric_measures_from_ids(
            self.metric_ids, start, stop,
            aggregation, granularity, needed_overlap)

    @classmethod
    def get_cross_metric_measures_from_ids(cls, metric_ids, start=None,
                                           stop=None, aggregation='mean',
                                           granularity=None,
                                           needed_overlap=100.0):
        # Check RBAC policy
        metrics = pecan.request.indexer.list_metrics(ids=metric_ids)
        missing_metric_ids = (set(metric_ids)
                              - set(six.text_type(m.id) for m in metrics))
        if missing_metric_ids:
            # Return one of the missing one in the error
            abort(404, storage.MetricDoesNotExist(
                missing_metric_ids.pop()))
        return cls.get_cross_metric_measures_from_objs(
            metrics, start, stop, aggregation, granularity, needed_overlap)

    @staticmethod
    def get_cross_metric_measures_from_objs(metrics, start=None, stop=None,
                                            aggregation='mean',
                                            granularity=None,
                                            needed_overlap=100.0):
        try:
            needed_overlap = float(needed_overlap)
        except ValueError:
            abort(400, 'needed_overlap must be a number')

        if start is not None:
            try:
                start = Timestamp(start)
            except Exception:
                abort(400, "Invalid value for start")

        if stop is not None:
            try:
                stop = Timestamp(stop)
            except Exception:
                abort(400, "Invalid value for stop")

        if (aggregation
           not in archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS):
            abort(
                400,
                'Invalid aggregation value %s, must be one of %s'
                % (aggregation,
                   archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS))

        for metric in metrics:
            enforce("get metric", metric)

        number_of_metrics = len(metrics)
        try:
            if number_of_metrics == 0:
                return []
            if granularity is not None:
                try:
                    granularity = float(granularity)
                except ValueError as e:
                    abort(400, "granularity must be a float: %s" % e)
            if number_of_metrics == 1:
                # NOTE(sileht): don't do the aggregation if we only have one
                # metric
                measures = pecan.request.storage.get_measures(
                    metrics[0], start, stop, aggregation,
                    granularity)
            else:
                measures = pecan.request.storage.get_cross_metric_measures(
                    metrics, start, stop, aggregation,
                    granularity,
                    needed_overlap)
            # Replace timestamp keys by their string versions
            return [(timestamp.isoformat(), offset, v)
                    for timestamp, offset, v in measures]
        except storage.MetricUnaggregatable as e:
            abort(400, ("One of the metrics being aggregated doesn't have "
                        "matching granularity: %s") % str(e))
        except storage.MetricDoesNotExist as e:
            abort(404, e)
        except storage.AggregationDoesNotExist as e:
            abort(404, e)


def MeasureSchema(m):
    # NOTE(sileht): don't use voluptuous for performance reasons
    try:
        value = float(m['value'])
    except Exception:
        abort(400, "Invalid input for a value")

    try:
        timestamp = Timestamp(m['timestamp'])
    except Exception as e:
        abort(400,
              "Invalid input for timestamp `%s': %s" % (m['timestamp'], e))

    return storage.Measure(timestamp, value)


class MetricController(rest.RestController):
    _custom_actions = {
        'measures': ['POST', 'GET']
    }

    def __init__(self, metric):
        self.metric = metric
        mgr = extension.ExtensionManager(namespace='gnocchi.aggregates',
                                         invoke_on_load=True)
        self.custom_agg = dict((x.name, x.obj) for x in mgr)

    def enforce_metric(self, rule):
        enforce(rule, json.to_primitive(self.metric))

    @pecan.expose('json')
    def get_all(self):
        self.enforce_metric("get metric")
        return self.metric

    @pecan.expose()
    def post_measures(self):
        self.enforce_metric("post measures")
        params = deserialize()
        if not isinstance(params, list):
            abort(400, "Invalid input for measures")
        if params:
            pecan.request.storage.add_measures(
                self.metric, six.moves.map(MeasureSchema, params))
        pecan.response.status = 202

    @pecan.expose('json')
    def get_measures(self, start=None, stop=None, aggregation='mean',
                     granularity=None, **param):
        self.enforce_metric("get measures")
        if not (aggregation
                in archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS
                or aggregation in self.custom_agg):
            msg = '''Invalid aggregation value %(agg)s, must be one of %(std)s
                     or %(custom)s'''
            abort(400, msg % dict(
                agg=aggregation,
                std=archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS,
                custom=str(self.custom_agg.keys())))

        if start is not None:
            try:
                start = Timestamp(start)
            except Exception:
                abort(400, "Invalid value for start")

        if stop is not None:
            try:
                stop = Timestamp(stop)
            except Exception:
                abort(400, "Invalid value for stop")

        try:
            if aggregation in self.custom_agg:
                measures = self.custom_agg[aggregation].compute(
                    pecan.request.storage, self.metric,
                    start, stop, **param)
            else:
                measures = pecan.request.storage.get_measures(
                    self.metric, start, stop, aggregation,
                    float(granularity) if granularity is not None else None)
            # Replace timestamp keys by their string versions
            return [(timestamp.isoformat(), offset, v)
                    for timestamp, offset, v in measures]
        except (storage.MetricDoesNotExist,
                storage.GranularityDoesNotExist,
                storage.AggregationDoesNotExist) as e:
            abort(404, e)
        except aggregates.CustomAggFailure as e:
            abort(400, e)

    @pecan.expose()
    def delete(self):
        self.enforce_metric("delete metric")
        try:
            pecan.request.indexer.delete_metric(self.metric.id)
        except indexer.NoSuchMetric as e:
            abort(404, e)


def UUID(value):
    try:
        return uuid.UUID(value)
    except Exception as e:
        raise ValueError(e)


class MetricsController(rest.RestController):

    @pecan.expose()
    def _lookup(self, id, *remainder):
        try:
            metric_id = uuid.UUID(id)
        except ValueError:
            abort(404, indexer.NoSuchMetric(id))
        metrics = pecan.request.indexer.list_metrics(
            id=metric_id, details=True)
        if not metrics:
            abort(404, indexer.NoSuchMetric(id))
        return MetricController(metrics[0]), remainder

    _MetricSchema = voluptuous.Schema({
        "user_id": six.text_type,
        "project_id": six.text_type,
        "archive_policy_name": six.text_type,
        "name": six.text_type,
    })

    # NOTE(jd) Define this method as it was a voluptuous schema – it's just a
    # smarter version of a voluptuous schema, no?
    @classmethod
    def MetricSchema(cls, definition):
        # First basic validation
        definition = cls._MetricSchema(definition)
        archive_policy_name = definition.get('archive_policy_name')

        name = definition.get('name')
        if archive_policy_name is None:
            try:
                ap = pecan.request.indexer.get_archive_policy_for_metric(name)
            except indexer.NoArchivePolicyRuleMatch:
                # NOTE(jd) Since this is a schema-like function, we
                # should/could raise ValueError, but if we do so, voluptuous
                # just returns a "invalid value" with no useful message – so we
                # prefer to use abort() to make sure the user has the right
                # error message
                abort(400, "No archive policy name specified "
                      "and no archive policy rule found matching "
                      "the metric name %s" % name)
            else:
                definition['archive_policy_name'] = ap.name

        user_id, project_id = get_user_and_project()

        enforce("create metric", {
            "created_by_user_id": user_id,
            "created_by_project_id": project_id,
            "user_id": definition.get('user_id'),
            "project_id": definition.get('project_id'),
            "archive_policy_name": archive_policy_name,
            "name": name,
        })

        return definition

    @pecan.expose('json')
    def post(self):
        user, project = get_user_and_project()
        body = deserialize_and_validate(self.MetricSchema)
        try:
            m = pecan.request.indexer.create_metric(
                uuid.uuid4(),
                user, project,
                name=body.get('name'),
                archive_policy_name=body['archive_policy_name'])
        except indexer.NoSuchArchivePolicy as e:
            abort(400, e)
        set_resp_location_hdr("/metric/" + str(m.id))
        pecan.response.status = 201
        return m

    @staticmethod
    @pecan.expose('json')
    def get_all(**kwargs):
        try:
            enforce("list all metric", {})
        except webob.exc.HTTPForbidden:
            enforce("list metric", {})
            user_id, project_id = get_user_and_project()
            provided_user_id = kwargs.get('user_id')
            provided_project_id = kwargs.get('project_id')
            if ((provided_user_id and user_id != provided_user_id)
               or (provided_project_id and project_id != provided_project_id)):
                abort(
                    403, "Insufficient privileges to filter by user/project")
        else:
            user_id = kwargs.get('user_id')
            project_id = kwargs.get('project_id')
        attr_filter = {}
        if user_id is not None:
            attr_filter['creater_by_user_id'] = user_id
        if project_id is not None:
            attr_filter['created_by_project_id'] = project_id
        return pecan.request.indexer.list_metrics(**attr_filter)


_MetricsSchema = voluptuous.Schema({
    six.text_type: voluptuous.Any(UUID,
                                  MetricsController.MetricSchema),
})


def MetricsSchema(data):
    # NOTE(jd) Before doing any kind of validation, copy the metric name
    # into the metric definition. This is required so we have the name
    # available when doing the metric validation with its own MetricSchema,
    # and so we can do things such as applying archive policy rules.
    if isinstance(data, dict):
        for metric_name, metric_def in six.iteritems(data):
            if isinstance(metric_def, dict):
                metric_def['name'] = metric_name
    return _MetricsSchema(data)


class NamedMetricController(rest.RestController):
    def __init__(self, resource_id, resource_type):
        self.resource_id = resource_id
        self.resource_type = resource_type

    @pecan.expose()
    def _lookup(self, name, *remainder):
        details = True if pecan.request.method == 'GET' else False
        m = pecan.request.indexer.list_metrics(details=details,
                                               name=name,
                                               resource_id=self.resource_id)
        if m:
            return MetricController(m[0]), remainder

        resource = pecan.request.indexer.get_resource(self.resource_type,
                                                      self.resource_id)
        if resource:
            abort(404, indexer.NoSuchMetric(name))
        else:
            abort(404, indexer.NoSuchResource(self.resource_id))

    @pecan.expose()
    def post(self):
        resource = pecan.request.indexer.get_resource(
            self.resource_type, self.resource_id)
        if not resource:
            abort(404, indexer.NoSuchResource(self.resource_id))
        enforce("update resource", resource)
        metrics = deserialize_and_validate(MetricsSchema)
        try:
            pecan.request.indexer.update_resource(
                self.resource_type, self.resource_id, metrics=metrics,
                append_metrics=True,
                create_revision=False)
        except (indexer.NoSuchMetric,
                indexer.NoSuchArchivePolicy,
                ValueError) as e:
            abort(400, e)
        except indexer.NamedMetricAlreadyExists as e:
            abort(409, e)
        except indexer.NoSuchResource as e:
            abort(404, e)

    @pecan.expose('json')
    def get_all(self):
        resource = pecan.request.indexer.get_resource(
            self.resource_type, self.resource_id)
        if not resource:
            abort(404, indexer.NoSuchResource(self.resource_id))
        enforce("get resource", resource)
        return pecan.request.indexer.list_metrics(resource_id=self.resource_id)


class ResourceHistoryController(rest.RestController):
    def __init__(self, resource_id, resource_type):
        self.resource_id = resource_id
        self.resource_type = resource_type

    @pecan.expose('json')
    def get(self, **kwargs):
        details = get_details(kwargs)
        pagination_opts = get_pagination_options(
            kwargs, RESOURCE_DEFAULT_PAGINATION)

        resource = pecan.request.indexer.get_resource(
            self.resource_type, self.resource_id)
        if not resource:
            abort(404, "foo")

        enforce("get resource", resource)

        try:
            # FIXME(sileht): next API version should returns
            # {'resources': [...], 'links': [ ... pagination rel ...]}
            return pecan.request.indexer.list_resources(
                self.resource_type,
                attribute_filter={"=": {"id": self.resource_id}},
                details=details,
                history=True,
                **pagination_opts
            )
        except indexer.IndexerException as e:
            abort(400, e)


def etag_precondition_check(obj):
    etag, lastmodified = obj.etag, obj.lastmodified
    # NOTE(sileht): Checks and order come from rfc7232
    # in webob, the '*' and the absent of the header is handled by
    # if_match.__contains__() and if_none_match.__contains__()
    # and are identique...
    if etag not in pecan.request.if_match:
        abort(412)
    elif (not pecan.request.environ.get("HTTP_IF_MATCH")
          and pecan.request.if_unmodified_since
          and pecan.request.if_unmodified_since < lastmodified):
        abort(412)

    if etag in pecan.request.if_none_match:
        if pecan.request.method in ['GET', 'HEAD']:
            abort(304)
        else:
            abort(412)
    elif (not pecan.request.environ.get("HTTP_IF_NONE_MATCH")
          and pecan.request.if_modified_since
          and (pecan.request.if_modified_since >=
               lastmodified)
          and pecan.request.method in ['GET', 'HEAD']):
        abort(304)


def etag_set_headers(obj):
    pecan.response.etag = obj.etag
    pecan.response.last_modified = obj.lastmodified


class ResourceTypeController(rest.RestController):
    def __init__(self, name):
        self._name = name

    @pecan.expose('json')
    def get(self):
        try:
            resource_type = pecan.request.indexer.get_resource_type(self._name)
        except indexer.NoSuchResourceType as e:
            abort(404, e)
        enforce("get resource type", resource_type)
        return resource_type

    @pecan.expose()
    def delete(self):
        try:
            resource_type = pecan.request.indexer.get_resource_type(self._name)
        except indexer.NoSuchResourceType as e:
            abort(404, e)
        enforce("delete resource type", resource_type)
        try:
            pecan.request.indexer.delete_resource_type(self._name)
        except (indexer.NoSuchResourceType,
                indexer.ResourceTypeInUse) as e:
            abort(400, e)


def ResourceTypeSchema(definition):
    # FIXME(sileht): Add resource type attributes from the indexer
    return voluptuous.Schema({
        "name": six.text_type,
    })(definition)


class ResourceTypesController(rest.RestController):

    @pecan.expose()
    def _lookup(self, name, *remainder):
        return ResourceTypeController(name), remainder

    @pecan.expose('json')
    def post(self):
        body = deserialize_and_validate(ResourceTypeSchema)
        enforce("create resource type", body)
        try:
            resource_type = pecan.request.indexer.create_resource_type(**body)
        except indexer.ResourceTypeAlreadyExists as e:
            abort(409, e)
        set_resp_location_hdr("/resource_type/" + resource_type.name)
        pecan.response.status = 201
        return resource_type

    @pecan.expose('json')
    def get_all(self, **kwargs):
        enforce("list resource type", {})
        try:
            return pecan.request.indexer.list_resource_types()
        except indexer.IndexerException as e:
            abort(400, e)


def ResourceSchema(schema):
    base_schema = {
        voluptuous.Optional('started_at'): Timestamp,
        voluptuous.Optional('ended_at'): Timestamp,
        voluptuous.Optional('user_id'): voluptuous.Any(None, six.text_type),
        voluptuous.Optional('project_id'): voluptuous.Any(None, six.text_type),
        voluptuous.Optional('metrics'): MetricsSchema,
    }
    base_schema.update(schema)
    return base_schema


class ResourceController(rest.RestController):

    def __init__(self, resource_type, id):
        self._resource_type = resource_type
        try:
            self.id = utils.ResourceUUID(id)
        except ValueError:
            abort(404, indexer.NoSuchResource(id))
        self.metric = NamedMetricController(str(self.id), self._resource_type)
        self.history = ResourceHistoryController(str(self.id),
                                                 self._resource_type)

    @pecan.expose('json')
    def get(self):
        resource = pecan.request.indexer.get_resource(
            self._resource_type, self.id, with_metrics=True)
        if resource:
            enforce("get resource", resource)
            etag_precondition_check(resource)
            etag_set_headers(resource)
            return resource
        abort(404, indexer.NoSuchResource(self.id))

    @pecan.expose('json')
    def patch(self):
        resource = pecan.request.indexer.get_resource(
            self._resource_type, self.id, with_metrics=True)
        if not resource:
            abort(404, indexer.NoSuchResource(self.id))
        enforce("update resource", resource)
        etag_precondition_check(resource)

        body = deserialize_and_validate(
            schema_for(self._resource_type),
            required=False)

        if len(body) == 0:
            etag_set_headers(resource)
            return resource

        for k, v in six.iteritems(body):
            if k != 'metrics' and getattr(resource, k) != v:
                create_revision = True
                break
        else:
            if 'metrics' not in body:
                # No need to go further, we assume the db resource
                # doesn't change between the get and update
                return resource
            create_revision = False

        try:
            resource = pecan.request.indexer.update_resource(
                self._resource_type,
                self.id,
                create_revision=create_revision,
                **body)
        except (indexer.NoSuchMetric,
                indexer.NoSuchArchivePolicy,
                ValueError) as e:
            abort(400, e)
        except indexer.NoSuchResource as e:
            abort(404, e)
        etag_set_headers(resource)
        return resource

    @pecan.expose()
    def delete(self):
        resource = pecan.request.indexer.get_resource(
            self._resource_type, self.id)
        if not resource:
            abort(404, indexer.NoSuchResource(self.id))
        enforce("delete resource", resource)
        etag_precondition_check(resource)
        try:
            pecan.request.indexer.delete_resource(self.id)
        except indexer.NoSuchResource as e:
            abort(404, e)


GenericSchema = ResourceSchema({})

InstanceDiskSchema = ResourceSchema({
    "name": six.text_type,
    "instance_id": UUID,
})

InstanceNetworkInterfaceSchema = ResourceSchema({
    "name": six.text_type,
    "instance_id": UUID,
})

InstanceSchema = ResourceSchema({
    "flavor_id": six.text_type,
    voluptuous.Optional("image_ref"): six.text_type,
    "host": six.text_type,
    "display_name": six.text_type,
    voluptuous.Optional("server_group"): six.text_type,
})

VolumeSchema = ResourceSchema({
    voluptuous.Optional("display_name"): voluptuous.Any(None,
                                                        six.text_type),
})

ImageSchema = ResourceSchema({
    "name": six.text_type,
    "container_format": six.text_type,
    "disk_format": six.text_type,
})

HostSchema = ResourceSchema({
    "host_name": six.text_type,
})

HostDiskSchema = ResourceSchema({
    "host_name": six.text_type,
    voluptuous.Optional("device_name"): voluptuous.Any(None,
                                                       six.text_type),
})

HostNetworkInterfaceSchema = ResourceSchema({
    "host_name": six.text_type,
    voluptuous.Optional("device_name"): voluptuous.Any(None,
                                                       six.text_type),
})

# NOTE(sileht): Must be loaded after all ResourceSchema
RESOURCE_SCHEMA_MANAGER = extension.ExtensionManager(
    'gnocchi.controller.schemas')


def schema_for(resource_type):
    if resource_type in RESOURCE_SCHEMA_MANAGER:
        # TODO(sileht): Remove this legacy resource schema loading
        return RESOURCE_SCHEMA_MANAGER[resource_type].plugin
    else:
        # TODO(sileht): Load schema from indexer
        return GenericSchema


def ResourceID(value):
    return (six.text_type(value), utils.ResourceUUID(value))


class ResourcesController(rest.RestController):
    def __init__(self, resource_type):
        self._resource_type = resource_type

    @pecan.expose()
    def _lookup(self, id, *remainder):
        return ResourceController(self._resource_type, id), remainder

    @pecan.expose('json')
    def post(self):
        # NOTE(sileht): we need to copy the dict because when change it
        # and we don't want that next patch call have the "id"
        schema = dict(schema_for(self._resource_type))
        schema["id"] = ResourceID

        body = deserialize_and_validate(schema)
        body["original_resource_id"], body["id"] = body["id"]

        target = {
            "resource_type": self._resource_type,
        }
        target.update(body)
        enforce("create resource", target)
        user, project = get_user_and_project()
        rid = body['id']
        del body['id']
        try:
            resource = pecan.request.indexer.create_resource(
                self._resource_type, rid, user, project,
                **body)
        except (ValueError,
                indexer.NoSuchMetric,
                indexer.NoSuchArchivePolicy) as e:
            abort(400, e)
        except indexer.ResourceAlreadyExists as e:
            abort(409, e)
        set_resp_location_hdr("/resource/"
                              + self._resource_type + "/"
                              + six.text_type(resource.id))
        etag_set_headers(resource)
        pecan.response.status = 201
        return resource

    @pecan.expose('json')
    def get_all(self, **kwargs):
        details = get_details(kwargs)
        history = get_history(kwargs)
        pagination_opts = get_pagination_options(
            kwargs, RESOURCE_DEFAULT_PAGINATION)
        user, project = get_user_and_project()
        policy_filter = _get_list_resource_policy_filter(
            "list resource", self._resource_type, user, project)

        try:
            # FIXME(sileht): next API version should returns
            # {'resources': [...], 'links': [ ... pagination rel ...]}
            return pecan.request.indexer.list_resources(
                self._resource_type,
                attribute_filter=policy_filter,
                details=details,
                history=history,
                **pagination_opts
            )
        except indexer.IndexerException as e:
            abort(400, e)


class ResourcesByTypeController(rest.RestController):
    @pecan.expose('json')
    def get_all(self):
        return dict(
            (rt.name,
             pecan.request.application_url + '/resource/' + rt.name)
            for rt in pecan.request.indexer.list_resource_types())

    @pecan.expose()
    def _lookup(self, resource_type, *remainder):
        try:
            pecan.request.indexer.get_resource_type(resource_type)
        except indexer.NoSuchResourceType as e:
            abort(404, e)
        return ResourcesController(resource_type), remainder


def _ResourceSearchSchema(v):
    """Helper method to indirect the recursivity of the search schema"""
    return SearchResourceTypeController.ResourceSearchSchema(v)


class SearchResourceTypeController(rest.RestController):
    def __init__(self, resource_type):
        self._resource_type = resource_type

    ResourceSearchSchema = voluptuous.Schema(
        voluptuous.All(
            voluptuous.Length(min=1, max=1),
            {
                voluptuous.Any(
                    u"=", u"==", u"eq",
                    u"<", u"lt",
                    u">", u"gt",
                    u"<=", u"≤", u"le",
                    u">=", u"≥", u"ge",
                    u"!=", u"≠", u"ne",
                    u"in",
                    u"like",
                ): voluptuous.All(voluptuous.Length(min=1, max=1), dict),
                voluptuous.Any(
                    u"and", u"∨",
                    u"or", u"∧",
                    u"not",
                ): [_ResourceSearchSchema],
            }
        )
    )

    def _search(self, **kwargs):
        if pecan.request.body:
            attr_filter = deserialize_and_validate(self.ResourceSearchSchema)
        else:
            attr_filter = None

        details = get_details(kwargs)
        history = get_history(kwargs)
        pagination_opts = get_pagination_options(
            kwargs, RESOURCE_DEFAULT_PAGINATION)

        user, project = get_user_and_project()
        policy_filter = _get_list_resource_policy_filter(
            "search resource", self._resource_type, user, project)
        if policy_filter:
            if attr_filter:
                attr_filter = {"and": [
                    policy_filter,
                    attr_filter
                ]}
            else:
                attr_filter = policy_filter

        return pecan.request.indexer.list_resources(
            self._resource_type,
            attribute_filter=attr_filter,
            details=details,
            history=history,
            **pagination_opts)

    @pecan.expose('json')
    def post(self, **kwargs):
        try:
            return self._search(**kwargs)
        except indexer.IndexerException as e:
            abort(400, e)


class SearchResourceController(rest.RestController):
    @pecan.expose()
    def _lookup(self, resource_type, *remainder):
        try:
            pecan.request.indexer.get_resource_type(resource_type)
        except indexer.NoSuchResourceType as e:
            abort(404, e)
        return SearchResourceTypeController(resource_type), remainder


def _MetricSearchSchema(v):
    """Helper method to indirect the recursivity of the search schema"""
    return SearchMetricController.MetricSearchSchema(v)


def _MetricSearchOperationSchema(v):
    """Helper method to indirect the recursivity of the search schema"""
    return SearchMetricController.MetricSearchOperationSchema(v)


class SearchMetricController(rest.RestController):

    MetricSearchOperationSchema = voluptuous.Schema(
        voluptuous.All(
            voluptuous.Length(min=1, max=1),
            {
                voluptuous.Any(
                    u"=", u"==", u"eq",
                    u"<", u"lt",
                    u">", u"gt",
                    u"<=", u"≤", u"le",
                    u">=", u"≥", u"ge",
                    u"!=", u"≠", u"ne",
                    u"%", u"mod",
                    u"+", u"add",
                    u"-", u"sub",
                    u"*", u"×", u"mul",
                    u"/", u"÷", u"div",
                    u"**", u"^", u"pow",
                ): voluptuous.Any(
                    float, int,
                    voluptuous.All(
                        [float, int,
                         voluptuous.Any(_MetricSearchOperationSchema)],
                        voluptuous.Length(min=2, max=2),
                    ),
                ),
            },
        )
    )

    MetricSearchSchema = voluptuous.Schema(
        voluptuous.Any(
            MetricSearchOperationSchema,
            voluptuous.All(
                voluptuous.Length(min=1, max=1),
                {
                    voluptuous.Any(
                        u"and", u"∨",
                        u"or", u"∧",
                        u"not",
                    ): [_MetricSearchSchema],
                }
            )
        )
    )

    @pecan.expose('json')
    def post(self, metric_id, start=None, stop=None, aggregation='mean'):
        metrics = pecan.request.indexer.list_metrics(
            ids=arg_to_list(metric_id))

        for metric in metrics:
            enforce("search metric", metric)

        if not pecan.request.body:
            abort(400, "No query specified in body")

        query = deserialize_and_validate(self.MetricSearchSchema)

        if start is not None:
            try:
                start = Timestamp(start)
            except Exception:
                abort(400, "Invalid value for start")

        if stop is not None:
            try:
                stop = Timestamp(stop)
            except Exception:
                abort(400, "Invalid value for stop")

        try:
            return {
                str(metric.id): values
                for metric, values in six.iteritems(
                    pecan.request.storage.search_value(
                        metrics, query, start, stop, aggregation)
                )
            }
        except storage.InvalidQuery as e:
            abort(400, e)


class ResourcesMetricsMeasuresBatchController(rest.RestController):
    MeasuresBatchSchema = voluptuous.Schema(
        {utils.ResourceUUID: {six.text_type: [MeasureSchema]}}
    )

    @pecan.expose()
    def post(self):
        body = deserialize_and_validate(self.MeasuresBatchSchema)

        known_metrics = []
        unknown_metrics = []
        for resource_id in body:
            names = body[resource_id].keys()
            metrics = pecan.request.indexer.list_metrics(
                names=names, resource_id=resource_id)

            if len(names) != len(metrics):
                known_names = [m.name for m in metrics]
                unknown_metrics.extend(
                    ["%s/%s" % (six.text_type(resource_id), m)
                     for m in names if m not in known_names])

            known_metrics.extend(metrics)

        if unknown_metrics:
            abort(400, "Unknown metrics: %s" % ", ".join(
                sorted(unknown_metrics)))

        for metric in known_metrics:
            enforce("post measures", metric)

        for metric in known_metrics:
            measures = body[metric.resource_id][metric.name]
            pecan.request.storage.add_measures(metric, measures)

        pecan.response.status = 202


class MetricsMeasuresBatchController(rest.RestController):
    # NOTE(sileht): we don't allow to mix both formats
    # to not have to deal with id collision that can
    # occurs between a metric_id and a resource_id.
    # Because while json allow duplicate keys in dict payload
    # only the last key will be retain by json python module to
    # build the python dict.
    MeasuresBatchSchema = voluptuous.Schema(
        {UUID: [MeasureSchema]}
    )

    @pecan.expose()
    def post(self):
        body = deserialize_and_validate(self.MeasuresBatchSchema)
        metrics = pecan.request.indexer.list_metrics(ids=body.keys())

        if len(metrics) != len(body):
            missing_metrics = sorted(set(body) - set(m.id for m in metrics))
            abort(400, "Unknown metrics: %s" % ", ".join(
                six.moves.map(str, missing_metrics)))

        for metric in metrics:
            enforce("post measures", metric)

        for metric in metrics:
            pecan.request.storage.add_measures(metric, body[metric.id])

        pecan.response.status = 202


class SearchController(object):
    resource = SearchResourceController()
    metric = SearchMetricController()


class AggregationResourceController(rest.RestController):
    def __init__(self, resource_type, metric_name):
        self.resource_type = resource_type
        self.metric_name = metric_name

    @pecan.expose('json')
    def post(self, start=None, stop=None, aggregation='mean',
             granularity=None, needed_overlap=100.0,
             groupby=None):
        # First, set groupby in the right format: a sorted list of unique
        # strings.
        groupby = sorted(set(arg_to_list(groupby)))

        # NOTE(jd) Sort by groupby so we are sure we do not return multiple
        # groups when using itertools.groupby later.
        try:
            resources = SearchResourceTypeController(
                self.resource_type)._search(sort=groupby)
        except indexer.InvalidPagination:
            abort(400, "Invalid groupby attribute")
        except indexer.IndexerException as e:
            abort(400, e)

        if resources is None:
            return []

        if not groupby:
            metrics = list(filter(None,
                                  (r.get_metric(self.metric_name)
                                   for r in resources)))
            return AggregatedMetricController.get_cross_metric_measures_from_objs(  # noqa
                metrics, start, stop, aggregation, granularity, needed_overlap)

        def groupper(r):
            return tuple((attr, r[attr]) for attr in groupby)

        results = []
        for key, resources in itertools.groupby(resources, groupper):
            metrics = list(filter(None,
                                  (r.get_metric(self.metric_name)
                                   for r in resources)))
            results.append({
                "group": dict(key),
                "measures": AggregatedMetricController.get_cross_metric_measures_from_objs(  # noqa
                    metrics, start, stop, aggregation,
                    granularity, needed_overlap)
            })

        return results


class AggregationController(rest.RestController):
    _custom_actions = {
        'metric': ['GET'],
    }

    @pecan.expose()
    def _lookup(self, object_type, resource_type, key, metric_name,
                *remainder):
        if object_type != "resource" or key != "metric":
            # NOTE(sileht): we want the raw 404 message here
            # so use directly pecan
            pecan.abort(404)
        elif resource_type not in RESOURCE_SCHEMA_MANAGER:
            abort(404, indexer.NoSuchResourceType(resource_type))
        return AggregationResourceController(resource_type,
                                             metric_name), remainder

    @pecan.expose('json')
    def get_metric(self, metric=None, start=None,
                   stop=None, aggregation='mean',
                   granularity=None, needed_overlap=100.0):
        return AggregatedMetricController.get_cross_metric_measures_from_ids(
            arg_to_list(metric), start, stop, aggregation,
            granularity, needed_overlap)


class CapabilityController(rest.RestController):
    @staticmethod
    @pecan.expose('json')
    def get():
        aggregation_methods = set(
            archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS)
        aggregation_methods.update(
            ext.name for ext in extension.ExtensionManager(
                namespace='gnocchi.aggregates'))
        return dict(aggregation_methods=aggregation_methods)


class StatusController(rest.RestController):
    @staticmethod
    @pecan.expose('json')
    def get(details=True):
        enforce("get status", {})
        report = pecan.request.storage.measures_report(details)
        report_dict = {"storage": {"summary": report['summary']}}
        if 'details' in report:
            report_dict["storage"]["measures_to_process"] = report['details']
        return report_dict


class MetricsBatchController(object):
    measures = MetricsMeasuresBatchController()


class ResourcesMetricsBatchController(object):
    measures = ResourcesMetricsMeasuresBatchController()


class ResourcesBatchController(object):
    metrics = ResourcesMetricsBatchController()


class BatchController(object):
    metrics = MetricsBatchController()
    resources = ResourcesBatchController()


class V1Controller(object):

    def __init__(self):
        self.sub_controllers = {
            "search": SearchController(),
            "archive_policy": ArchivePoliciesController(),
            "archive_policy_rule": ArchivePolicyRulesController(),
            "metric": MetricsController(),
            "batch": BatchController(),
            "resource": ResourcesByTypeController(),
            "resource_type": ResourceTypesController(),
            "aggregation": AggregationController(),
            "capabilities": CapabilityController(),
            "status": StatusController(),
        }
        for name, ctrl in self.sub_controllers.items():
            setattr(self, name, ctrl)

    @pecan.expose('json')
    def index(self):
        return {
            "version": "1.0",
            "links": [
                {"rel": "self",
                 "href": pecan.request.application_url}
            ] + [
                {"rel": name,
                 "href": pecan.request.application_url + "/" + name}
                for name in sorted(self.sub_controllers)
            ]
        }


class VersionsController(object):
    @staticmethod
    @pecan.expose('json')
    def index():
        return {
            "versions": [
                {
                    "status": "CURRENT",
                    "links": [
                        {
                            "rel": "self",
                            "href": pecan.request.application_url + "/v1/"
                            }
                        ],
                    "id": "v1.0",
                    "updated": "2015-03-19"
                    }
                ]
            }
