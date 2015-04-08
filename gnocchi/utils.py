# -*- encoding: utf-8 -*-
#
# Copyright © 2015 eNovance
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

from oslo_utils import timeutils
from pytimeparse import timeparse
import six


def to_timestamp(v):
    if isinstance(v, datetime.datetime):
        return v
    try:
        v = float(v)
    except (ValueError, TypeError):
        v = six.text_type(v)
        try:
            return timeutils.normalize_time(timeutils.parse_isotime(v))
        except ValueError:
            delta = timeparse.timeparse(v)
            if delta is None:
                raise ValueError("Unable to parse timestamp %s" % v)
            return timeutils.utcnow() + datetime.timedelta(seconds=delta)
    return datetime.datetime.utcfromtimestamp(v)


def to_timespan(value):
    if value is None:
        raise ValueError("Invalid timespan")
    try:
        seconds = int(value)
    except Exception:
        try:
            seconds = timeparse.timeparse(six.text_type(value))
        except Exception:
            raise ValueError("Unable to parse timespan")
    if seconds is None:
        raise ValueError("Unable to parse timespan")
    if seconds <= 0:
        raise ValueError("Timespan must be positive")
    return datetime.timedelta(seconds=seconds)
