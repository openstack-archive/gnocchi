# -*- encoding: utf-8 -*-
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

import six
import stevedore
import voluptuous

from gnocchi import utils


INVALID_NAMES = [
    "id", "type", "metrics",
    "revision", "revision_start", "revision_end",
    "started_at", "ended_at",
    "user_id", "project_id",
    "created_by_user_id", "created_by_project_id", "get_metric"
]


class InvalidResourceAttributeName(Exception):
    """Error raised when the resource attribute name is invalid."""
    def __init__(self, name):
        super(InvalidResourceAttributeName, self).__init__(
            "Resource attribute name %s is invalid" % str(name))
        self.name = name


class CommonAttributeSchema(object):
    meta_schema_ext = {}
    schema_ext = None

    def __init__(self, type, name, required):
        # TODO(sileht): Perhaps we need to be more restrictive ?
        # max len ? only ascii char ? what does sql backends support ?
        if name.startswith("_") or name in INVALID_NAMES:
            raise InvalidResourceAttributeName(name)

        self.name = name
        self.required = required

    @classmethod
    def meta_schema(cls):
        d = {
            voluptuous.Required('type'): cls.typename,
            voluptuous.Required('required', default=True): bool
        }
        if callable(cls.meta_schema_ext):
            d.update(cls.meta_schema_ext())
        else:
            d.update(cls.meta_schema_ext)
        return d

    def schema(self):
        if self.required:
            return {self.name: self.schema_ext}
        else:
            return {voluptuous.Optional(self.name): self.schema_ext}

    def jsonify(self):
        return {"type": self.typename,
                "required": self.required}


class StringSchema(CommonAttributeSchema):
    typename = "string"

    def __init__(self, length, *args, **kwargs):
        super(StringSchema, self).__init__(*args, **kwargs)
        self.length = length

    meta_schema_ext = {
        voluptuous.Required('length', default=255):
        voluptuous.All(int, voluptuous.Range(min=1, max=255))
    }

    @property
    def schema_ext(self):
        return voluptuous.All(six.text_type,
                              voluptuous.Length(max=self.length))

    def jsonify(self):
        d = super(StringSchema, self).jsonify()
        d.update({"length": self.length})
        return d


class UUIDSchema(CommonAttributeSchema):
    typename = "uuid"
    schema_ext = staticmethod(utils.UUID)


class NumberSchema(CommonAttributeSchema):
    typename = "number"

    def __init__(self, min, max, *args, **kwargs):
        super(NumberSchema, self).__init__(*args, **kwargs)
        self.min = min
        self.max = max

    meta_schema_ext = {
        voluptuous.Required('min', default=None): voluptuous.Any(
            None, voluptuous.All(voluptuous.Any(float, int),
                                 voluptuous.Range(min=0))),
        voluptuous.Required('max', default=None): voluptuous.Any(
            None, voluptuous.All(voluptuous.Any(float, int),
                                 voluptuous.Range(min=0)))
    }

    @property
    def schema_ext(self):
        return voluptuous.All(voluptuous.Any(float, int),
                              voluptuous.Range(min=self.min,
                                               max=self.max))

    def jsonify(self):
        d = super(NumberSchema, self).jsonify()
        d.update({"min": self.min, "max": self.max})
        return d


class BoolSchema(CommonAttributeSchema):
    typename = "bool"
    schema_ext = bool


class ResourceTypeAttributes(list):
    def jsonify(self):
        d = {}
        for attr in self:
            d[attr.name] = attr.jsonify()
        return d


class ResourceTypeSchemaManager(stevedore.ExtensionManager):
    def __init__(self, *args, **kwargs):
        super(ResourceTypeSchemaManager, self).__init__(*args, **kwargs)
        type_schemas = tuple([ext.plugin.meta_schema()
                              for ext in self.extensions])
        self._schema = voluptuous.Schema({
            "name": six.text_type,
            voluptuous.Required("attributes", default={}): {
                six.text_type: voluptuous.Any(*tuple(type_schemas))
            }
        })

    def __call__(self, definition):
        return self._schema(definition)

    def attributes_from_dict(self, attributes):
        return ResourceTypeAttributes(
            self[attr["type"]].plugin(name=name, **attr)
            for name, attr in attributes.items())

    def resource_type_from_dict(self, name, attributes):
        return ResourceType(name, self.attributes_from_dict(attributes))


class ResourceType(object):
    def __init__(self, name, attributes):
        self.name = name
        self.attributes = attributes

    @property
    def schema(self):
        schema = {}
        for attr in self.attributes:
            schema.update(attr.schema())
        return schema

    def __eq__(self, other):
        return self.name == other.name

    def jsonify(self):
        return {"name": self.name,
                "attributes": self.attributes.jsonify()}
