# Copyright (c) 2013 Mirantis Inc.
# Copyright (c) 2015 Red Hat
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import futures
import multiprocessing
import threading
import time
import uuid

import cotyledon
from futurist import periodics
from oslo_config import cfg
from oslo_log import log
from oslo_utils import timeutils
import retrying
import six
from tooz import coordination

from gnocchi import archive_policy
from gnocchi import indexer
from gnocchi import service
from gnocchi import statsd as statsd_service
from gnocchi import storage


LOG = log.getLogger(__name__)


def upgrade():
    conf = cfg.ConfigOpts()
    conf.register_cli_opts([
        cfg.BoolOpt("skip-index", default=False,
                    help="Skip index upgrade."),
        cfg.BoolOpt("skip-storage", default=False,
                    help="Skip storage upgrade."),
        cfg.BoolOpt("skip-archive-policies-creation", default=False,
                    help="Skip default archive policies creation."),
        cfg.BoolOpt("create-legacy-resource-types", default=False,
                    help="Creation of Ceilometer legacy resource types.")
    ])
    conf = service.prepare_service(conf=conf)
    index = indexer.get_driver(conf)
    index.connect()
    if not conf.skip_index:
        LOG.info("Upgrading indexer %s" % index)
        index.upgrade(
            create_legacy_resource_types=conf.create_legacy_resource_types)
    if not conf.skip_storage:
        s = storage.get_driver(conf)
        LOG.info("Upgrading storage %s" % s)
        s.upgrade(index)

    if (not conf.skip_archive_policies_creation
            and not index.list_archive_policies()
            and not index.list_archive_policy_rules()):
        for name, ap in six.iteritems(archive_policy.DEFAULT_ARCHIVE_POLICIES):
            index.create_archive_policy(ap)
        index.create_archive_policy_rule("default", "*", "low")


def statsd():
    statsd_service.start()


class Retry(Exception):
    pass


def retry_if_retry_is_raised(exception):
    return isinstance(exception, Retry)


class MetricProcessBase(cotyledon.Service):
    def __init__(self, worker_id, conf, interval_delay=0):
        super(MetricProcessBase, self).__init__(worker_id)
        self.conf = conf
        self.startup_delay = worker_id
        self.interval_delay = interval_delay
        self._shutdown = threading.Event()
        self._shutdown_done = threading.Event()

    # Retry with exponential backoff for up to 1 minute
    @retrying.retry(wait_exponential_multiplier=500,
                    wait_exponential_max=60000,
                    retry_on_exception=retry_if_retry_is_raised)
    def _configure(self):
        try:
            self.store = storage.get_driver(self.conf)
        except storage.StorageError as e:
            LOG.error("Unable to initialize storage: %s" % e)
            raise Retry(e)
        try:
            self.index = indexer.get_driver(self.conf)
            self.index.connect()
        except indexer.IndexerException as e:
            LOG.error("Unable to initialize indexer: %s" % e)
            raise Retry(e)

    def run(self):
        self._configure()
        # Delay startup so workers are jittered.
        time.sleep(self.startup_delay)

        while not self._shutdown.is_set():
            with timeutils.StopWatch() as timer:
                self._run_job()
                self._shutdown.wait(max(0, self.interval_delay -
                                        timer.elapsed()))
        self._shutdown_done.set()

    def terminate(self):
        self._shutdown.set()
        self.close_services()
        LOG.info("Waiting ongoing metric processing to finish")
        self._shutdown_done.wait()

    @staticmethod
    def close_services():
        raise NotImplementedError

    @staticmethod
    def _run_job():
        raise NotImplementedError


class MetricReporting(MetricProcessBase):
    name = "reporting"

    def __init__(self, worker_id, conf, queue):
        super(MetricReporting, self).__init__(
            worker_id, conf, conf.storage.metric_reporting_delay)

    def _run_job(self):
        try:
            report = self.store.measures_report(details=False)
            LOG.info("Metricd reporting: %d measurements bundles across %d "
                     "metrics wait to be processed.",
                     report['summary']['measures'],
                     report['summary']['metrics'])
        except Exception:
            LOG.error("Unexpected error during pending measures reporting",
                      exc_info=True)


class MetricScheduler(MetricProcessBase):
    name = "scheduler"
    group_id = "gnocchi-scheduler"

    def _enable_coordination(self, conf):
        self._coord = coordination.get_coordinator(
            conf.cordination_url, self._my_id)
        self._coord.start(start_heart=True)

    def __init__(self, worker_id, conf, queue):
        super(MetricScheduler, self).__init__(
            worker_id, conf, conf.storage.metric_processing_delay)
        self._my_id = str(uuid.uuid4())
        self._enable_coordination(conf)
        self.queue = queue
        self.prev_set = set()
        self.block = 0

    def set_block(self, event):
        get_members_req = self._coord.get_members(self.group_id)
        try:
            members = sorted(get_members_req.get())
            self.block = members.index(self._my_id)
        except Exception:
            LOG.warning('Error getting block to work on, default to first')
            self.block = 0

    # Retry with exponential backoff for up to 1 minute
    @retrying.retry(wait_exponential_multiplier=500,
                    wait_exponential_max=60000,
                    retry_on_exception=retry_if_retry_is_raised)
    def _configure(self):
        super(MetricScheduler, self)._configure()
        try:
            join_req = self._coord.join_group(self.group_id)
            join_req.get()
            LOG.info('Joined coordination group: %s', self.group_id)
            self.set_block(None)

            @periodics.periodic(spacing=cfg.CONF.coordination.check_watchers,
                                run_immediately=True)
            def run_watchers():
                self._coord.run_watchers()

            self.periodic = periodics.PeriodicWorker.create(
                [], executor_factory=lambda:
                futures.ThreadPoolExecutor(max_workers=10))
            self.periodic.add(run_watchers)

            self._coord.watch_join_group(self.group_id, self.set_block)
            self._coord.watch_leave_group(self.group_id, self.set_block)
        except coordination.GroupNotCreated as e:
            create_group_req = self._coord.create_group(self.group_id)
            try:
                create_group_req.get()
            except coordination.GroupAlreadyExist:
                pass
            raise Retry(e)
        except Exception:
            LOG.warning('Failed to join group. Not part of coordination.')

    def _run_job(self):
        try:
            # TODO(gordc): add support to detect other agents to enable
            #              partitioning
            metrics = self.store._list_metric_with_measures_to_process(
                500, self.block)
            if not self.queue.empty():
                # NOTE(gordc): drop metrics we previously process to avoid
                #              handling twice
                orig = len(metrics)
                metrics = metrics - self.prev_set
                if 500 - (orig - len(metrics)) > 150:
                    LOG.warning('Large overlap between processing intervals. '
                                'It is recommended to increase the number of '
                                'workers or to increase processing interval.')
            for m_id in metrics:
                self.queue.put(m_id)
            self.prev_set = metrics
            LOG.debug("Metricd distribution: %d metrics queued.", len(metrics))
        except Exception:
            LOG.error("Unexpected error scheduling metrics for processing",
                      exc_info=True)

    def close_services(self):
        self._coord.leave_group(self.group_id)
        self._coord.stop()
        self.queue.close()


class MetricJanitor(MetricProcessBase):
    name = "janitor"

    def __init__(self,  worker_id, conf):
        super(MetricJanitor, self).__init__(
            worker_id, conf, conf.storage.metric_cleanup_delay)

    def _run_job(self):
        try:
            self.store.expunge_metrics(self.index)
            LOG.debug("Metrics marked for deletion removed from backend")
        except Exception:
            LOG.error("Unexpected error during metric cleanup", exc_info=True)


class MetricProcessor(MetricProcessBase):
    name = "processing"

    def __init__(self, worker_id, conf, queue):
        super(MetricProcessor, self).__init__(
            worker_id, conf, 1)
        self.queue = queue
        self.block_size = 4

    def _run_job(self):
        try:
            metrics = []
            while not self.queue.empty() and len(metrics) < self.block_size:
                metrics.append(self.queue.get())
            LOG.debug("Got %s metrics to process", len(metrics))
            if metrics:
                self.store.process_background_tasks(self.index, metrics)
        except Exception:
            LOG.error("Unexpected error during measures processing",
                      exc_info=True)

    def close_services(self):
        self.queue.close()


class MetricdServiceManager(cotyledon.ServiceManager):
    def __init__(self, conf):
        super(MetricdServiceManager, self).__init__()
        self.conf = conf
        self.queue = multiprocessing.Manager().Queue()

        self.add(MetricScheduler, args=(self.conf, self.queue))
        self.add(MetricProcessor, args=(self.conf, self.queue),
                 workers=conf.metricd.workers)
        self.add(MetricReporting, args=(self.conf,))
        self.add(MetricJanitor, args=(self.conf,))


def metricd():
    conf = service.prepare_service()
    MetricdServiceManager(conf).run()
