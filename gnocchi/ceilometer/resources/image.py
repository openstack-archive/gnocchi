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
import sqlalchemy
import sqlalchemy_utils

from gnocchi.ceilometer.resources import base
from gnocchi.indexer import sqlalchemy_base


class Image(base.ResourceBase):
    @staticmethod
    def get_resource_extra_attributes(sample):
        metadata = sample['resource_metadata']
        params = {
            "name": metadata['name'],
            "container_format": metadata["container_format"],
            "disk_format": metadata["disk_format"]
        }
        return params

    @staticmethod
    def get_metrics_names():
        return ['image',
                'image.size']


class ImageSQLAlchemy(sqlalchemy_base.Resource):
    __tablename__ = 'image'
    __table_args__ = (
        sqlalchemy.Index('ix_image_seq', 'seq'),
        sqlalchemy.Index('ix_image_eid', 'eid'),
        sqlalchemy_base.COMMON_TABLES_ARGS,
    )

    seq = sqlalchemy.Column(sqlalchemy.Integer,
                            sqlalchemy.ForeignKey('resource.seq',
                                                  ondelete="CASCADE"),
                            primary_key=True)
    eid = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                            sqlalchemy.ForeignKey('resourceid.id',
                                                  ondelete="CASCADE"))
    name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    container_format = sqlalchemy.Column(sqlalchemy.String(255),
                                         nullable=False)
    disk_format = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
