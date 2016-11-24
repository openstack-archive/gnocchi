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
import errno
import os
import tempfile


class FileStorageBase(object):
    def __init__(self, conf):
        super(FileStorageBase, self).__init__(conf)
        self.basepath = conf.file_basepath
        self.basepath_tmp = os.path.join(conf.file_basepath,
                                         'tmp')
        try:
            os.mkdir(self.basepath)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        self.measure_path = os.path.join(self.basepath, 'measure')
        try:
            os.mkdir(self.measure_path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        try:
            os.mkdir(self.basepath_tmp)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    def _get_tempfile(self):
        return tempfile.NamedTemporaryFile(prefix='gnocchi',
                                           dir=self.basepath_tmp,
                                           delete=False)
