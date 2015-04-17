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
import hashlib

from oslo_config import cfg
from oslo_utils import netutils
from oslo_utils import timeutils
import pytz
import six
from stevedore import driver

from gnocchi import exceptions

OPTS = [
    cfg.StrOpt('url',
               default="null://",
               help='Indexer driver to use'),
]


_marker = object()


class Resource(object):
    def get_metric(self, metric_name):
        for m in self.metrics:
            if m.name == metric_name:
                return m

    def __eq__(self, other):
        return (self.id == other.id
                and self.type == other.type
                and self.revision == other.revision
                and self.revision_start == other.revision_start
                and self.revision_end == other.revision_end
                and self.created_by_user_id == other.created_by_user_id
                and self.created_by_project_id == other.created_by_project_id
                and self.user_id == other.user_id
                and self.project_id == other.project_id
                and self.started_at == other.started_at
                and self.ended_at == other.ended_at)

    @property
    def etag(self):
        etag = hashlib.sha1()
        etag.update(six.text_type(self.id).encode('utf-8'))
        etag.update(six.text_type(timeutils.isotime(
            self.revision_start, subsecond=True)).encode('utf-8'))
        return etag.hexdigest()

    @property
    def lastmodified(self):
        # less precise revision start for Last-Modified http header
        return self.revision_start.replace(microsecond=0,
                                           tzinfo=pytz.UTC)


def get_driver(conf):
    """Return the configured driver."""
    split = netutils.urlsplit(conf.indexer.url)
    d = driver.DriverManager('gnocchi.indexer',
                             split.scheme).driver
    return d(conf)


class IndexerException(Exception):
    """Base class for all exceptions raised by an indexer."""


class UnknownResourceType(IndexerException):
    """Error raised when the resource type is unknown."""
    def __init__(self, type):
        super(UnknownResourceType, self).__init__(
            "Resource type %s is unknown" % type)
        self.type = type


class NoSuchMetric(IndexerException):
    """Error raised when a metric does not exist."""
    def __init__(self, metric):
        super(NoSuchMetric, self).__init__("Metric %s does not exist" %
                                           str(metric))
        self.metric = metric


class NoSuchResource(IndexerException):
    """Error raised when a resource does not exist."""
    def __init__(self, resource):
        super(NoSuchResource, self).__init__("Resource %s does not exist" %
                                             str(resource))
        self.resource = resource


class NoSuchArchivePolicy(IndexerException):
    """Error raised when an archive policy does not exist."""
    def __init__(self, archive_policy):
        super(NoSuchArchivePolicy, self).__init__(
            "Archive policy %s does not exist" %
            str(archive_policy))
        self.archive_policy = archive_policy


class ArchivePolicyInUse(IndexerException):
    """Error raised when an archive policy is still being used."""
    def __init__(self, archive_policy):
        super(ArchivePolicyInUse, self).__init__(
            "Archive policy %s is still in use" % archive_policy)
        self.archive_policy = archive_policy


class NoSuchArchivePolicyRule(IndexerException):
    """Error raised when an archive policy rule does not exist."""
    def __init__(self, archive_policy_rule):
        super(NoSuchArchivePolicyRule, self).__init__(
            "Archive policy Rule %s does not exist" %
            str(archive_policy_rule))
        self.archive_policy_rule = archive_policy_rule


class ArchivePolicyRuleInUse(IndexerException):
    """Error raised when an archive policy rule is still being used."""
    def __init__(self, archive_policy_rule):
        super(ArchivePolicyRuleInUse, self).__init__(
            "Archive policy Rule %s is still in use" % archive_policy_rule)
        self.archive_policy_rule = archive_policy_rule


class NamedMetricAlreadyExists(IndexerException):
    """Error raised when a named metric already exists."""
    def __init__(self, metric):
        super(NamedMetricAlreadyExists, self).__init__(
            "Named metric %s already exists" % metric)
        self.metric = metric


class ResourceAlreadyExists(IndexerException):
    """Error raised when a resource already exists."""
    def __init__(self, resource):
        super(ResourceAlreadyExists, self).__init__(
            "Resource %s already exists" % resource)
        self.resource = resource


class ResourceAttributeError(IndexerException, AttributeError):
    """Error raised when an attribute does not exist for a resource type."""
    def __init__(self, resource, attribute):
        super(ResourceAttributeError, self).__init__(
            "Resource %s has no %s attribute" % (resource, attribute))
        self.resource = resource,
        self.attribute = attribute


class ResourceValueError(IndexerException, ValueError):
    """Error raised when an attribute value is invalid for a resource type."""
    def __init__(self, resource_type, attribute, value):
        super(ResourceValueError, self).__init__(
            "Value %s for attribute %s on resource type %s is invalid"
            % (value, attribute, resource_type))
        self.resource_type = resource_type
        self.attribute = attribute
        self.value = value


class ArchivePolicyAlreadyExists(IndexerException):
    """Error raised when an archive policy already exists."""
    def __init__(self, name):
        super(ArchivePolicyAlreadyExists, self).__init__(
            "Archive policy %s already exists" % name)
        self.name = name


class ArchivePolicyRuleAlreadyExists(IndexerException):
    """Error raised when an archive policy rule already exists."""
    def __init__(self, name):
        super(ArchivePolicyRuleAlreadyExists, self).__init__(
            "Archive policy %s already exists" % name)
        self.name = name


class QueryError(IndexerException):
    def __init__(self):
        super(QueryError, self).__init__("Unable to parse this query")


class QueryInvalidOperator(QueryError):
    def __init__(self, op):
        self.op = op
        super(QueryError, self).__init__("Unknown operator `%s'" % op)


class QueryAttributeError(QueryError, ResourceAttributeError):
    def __init__(self, resource, attribute):
        ResourceAttributeError.__init__(self, resource, attribute)


class IndexerDriver(object):
    @staticmethod
    def __init__(conf):
        pass

    @staticmethod
    def connect():
        pass

    @staticmethod
    def disconnect():
        pass

    @staticmethod
    def upgrade():
        pass

    @staticmethod
    def get_resource(resource_type, resource_id, with_metrics=False):
        """Get a resource from the indexer.

        :param resource_type: The type of the resource to look for.
        :param resource_id: The UUID of the resource.
        :param with_metrics: Whether to include metrics information.
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def list_resources(resource_type='generic',
                       attribute_filter=None,
                       details=False,
                       history=False):
        raise exceptions.NotImplementedError

    @staticmethod
    def list_archive_policies():
        raise exceptions.NotImplementedError

    @staticmethod
    def get_archive_policy(name):
        raise exceptions.NotImplementedError

    @staticmethod
    def delete_archive_policy(name):
        raise exceptions.NotImplementedError

    @staticmethod
    def get_archive_policy_rule(name):
        raise exceptions.NotImplementedError

    @staticmethod
    def list_archive_policy_rules():
        raise exceptions.NotImplementedError

    @staticmethod
    def create_archive_policy_rule(name, metric_pattern, archive_policy_name):
        raise exceptions.NotImplementedError

    @staticmethod
    def delete_archive_policy_rule(name):
        raise exceptions.NotImplementedError

    @staticmethod
    def get_metrics(uuids):
        """Get metrics informations from the indexer.

        :param uuids: A list of metric UUID.
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def create_metric(id, created_by_user_id, created_by_project_id,
                      archive_policy_name, name=None, resource_id=None,
                      details=False):
        raise exceptions.NotImplementedError

    @staticmethod
    def list_metrics(user_id=None, project_id=None):
        raise exceptions.NotImplementedError

    @staticmethod
    def create_archive_policy(archive_policy):
        raise exceptions.NotImplementedError

    @staticmethod
    def create_resource(resource_type, id, user_id, project_id,
                        started_at=None, ended_at=None, metrics=None,
                        **kwargs):
        raise exceptions.NotImplementedError

    @staticmethod
    def update_resource(resource_type, resource_id, ended_at=_marker,
                        metrics=_marker,
                        append_metrics=False,
                        **kwargs):
        raise exceptions.NotImplementedError

    @staticmethod
    def delete_resource(uuid):
        raise exceptions.NotImplementedError

    @staticmethod
    def delete_metric(id):
        raise exceptions.NotImplementedError
