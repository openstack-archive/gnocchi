# -*- encoding: utf-8 -*-
#
# Copyright © 2016 Red Hat, Inc.
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
from collections import defaultdict
import contextlib
import datetime
import uuid

import six

from gnocchi.storage.common import s3
from gnocchi.storage.incoming import _carbonara

boto3 = s3.boto3
botocore = s3.botocore


class S3Storage(_carbonara.CarbonaraBasedStorage):

    # FIXME: s3 and s3rver has a bucket restriction
    # https://bugs.launchpad.net/gnocchi/+bug/1671925
    NUM_SACKS = 1

    def __init__(self, conf):
        super(S3Storage, self).__init__(conf)
        self.s3, self._region_name, self._bucket_prefix = (
            s3.get_connection(conf)
        )

    def _sack_name(self, sack):
        return ('%s-' + self.SACK_PREFIX) % (self._bucket_prefix, sack)

    def upgrade(self, index):
        super(S3Storage, self).upgrade(index)
        for i in six.moves.range(self.NUM_SACKS):
            try:
                s3.create_bucket(self.s3, self._sack_name(i),
                                 self._region_name)
            except botocore.exceptions.ClientError as e:
                if e.response['Error'].get('Code') not in (
                    "BucketAlreadyExists", "BucketAlreadyOwnedByYou"
                ):
                    raise

    def _store_new_measures(self, metric, data):
        now = datetime.datetime.utcnow().strftime("_%Y%m%d_%H:%M:%S")
        self.s3.put_object(
            Bucket=self._sack_name(self.compute_sack(metric.id)),
            Key=(six.text_type(metric.id)
                 + "/"
                 + six.text_type(uuid.uuid4())
                 + now),
            Body=data)

    def _build_report(self, details):
        metric_details = defaultdict(int)
        for i in six.moves.range(self.NUM_SACKS):
            response = {}
            while response.get('IsTruncated', True):
                if 'NextContinuationToken' in response:
                    kwargs = {
                        'ContinuationToken': response['NextContinuationToken']
                    }
                else:
                    kwargs = {}
                response = self.s3.list_objects_v2(
                    Bucket=self._sack_name(i),
                    **kwargs)
                for c in response.get('Contents', ()):
                    metric, metric_file = c['Key'].split("/", 1)
                    metric_details[metric] += 1
        return (len(metric_details), sum(metric_details.values()),
                metric_details if details else None)

    def list_metric_with_measures_to_process(self, sack):
        metrics = set()
        response = {}
        # Handle pagination
        while response.get('IsTruncated', True):
            if 'NextContinuationToken' in response:
                kwargs = {
                    'ContinuationToken': response['NextContinuationToken']
                }
            else:
                kwargs = {}
            response = self.s3.list_objects_v2(
                Bucket=self._sack_name(sack),
                Delimiter="/",
                **kwargs)
            for p in response.get('CommonPrefixes', ()):
                metrics.add(p['Prefix'].rstrip('/'))

        return metrics

    def _list_measure_files_for_metric_id(self, sack, metric_id):
        files = set()
        response = {}
        while response.get('IsTruncated', True):
            if 'NextContinuationToken' in response:
                kwargs = {
                    'ContinuationToken': response['NextContinuationToken']
                }
            else:
                kwargs = {}
            response = self.s3.list_objects_v2(
                Bucket=self._sack_name(sack),
                Prefix=six.text_type(metric_id) + "/",
                **kwargs)

            for c in response.get('Contents', ()):
                files.add(c['Key'])

        return files

    def delete_unprocessed_measures_for_metric_id(self, metric_id):
        sack = self.compute_sack(metric_id)
        files = self._list_measure_files_for_metric_id(sack, metric_id)
        s3.bulk_delete(self.s3, self._sack_name(sack), files)

    @contextlib.contextmanager
    def process_measure_for_metric(self, metric):
        sack = self.compute_sack(metric.id)
        sack_name = self._sack_name(sack)
        files = self._list_measure_files_for_metric_id(sack, metric.id)

        measures = []
        for f in files:
            response = self.s3.get_object(Bucket=sack_name, Key=f)
            measures.extend(
                self._unserialize_measures(f, response['Body'].read()))

        yield measures

        # Now clean objects
        s3.bulk_delete(self.s3, sack_name, files)
