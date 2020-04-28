import toolz

from ..utils import RequestHandler
from ...scheduler import ALL_TASK_STATES


class _PrometheusCollector:
    def __init__(self, dask_server):
        self.server = dask_server

    def collect(self):
        from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily

        yield GaugeMetricFamily(
            "dask_scheduler_clients",
            "Number of clients connected.",
            value=len(self.server.clients),
        )

        yield GaugeMetricFamily(
            "dask_scheduler_desired_workers",
            "Number of workers scheduler needs for task graph.",
            value=self.server.adaptive_target(),
        )

        worker_states = GaugeMetricFamily(
            "dask_scheduler_workers",
            "Number of workers known by scheduler.",
            labels=["state"],
        )
        worker_states.add_metric(["connected"], len(self.server.workers))
        worker_states.add_metric(["saturated"], len(self.server.saturated))
        worker_states.add_metric(["idle"], len(self.server.idle))
        yield worker_states

        tasks = GaugeMetricFamily(
            "dask_scheduler_tasks",
            "Number of tasks known by scheduler.",
            labels=["state"],
        )

        task_counter = toolz.merge_with(
            sum, (tp.states for tp in self.server.task_prefixes.values())
        )

        suspicious_tasks = CounterMetricFamily(
            "dask_scheduler_tasks_suspicious",
            "Total number of times a task has been marked suspicious",
            labels=["task_prefix_name"],
        )

        for tp in self.server.task_prefixes.values():
            suspicious_tasks.add_metric([tp.name], tp.suspicious)
        yield suspicious_tasks

        yield CounterMetricFamily(
            "dask_scheduler_tasks_forgotten",
            (
                "Total number of processed tasks no longer in memory and already "
                "removed from the scheduler job queue. Note task groups on the "
                "scheduler which have all tasks in the forgotten state are not included."
            ),
            value=task_counter.get("forgotten", 0.0),
        )

        for state in ALL_TASK_STATES:
            tasks.add_metric([state], task_counter.get(state, 0.0))
        yield tasks

        sem_ext = self.server.extensions["semaphores"]

        # FIXME: move these to write path??
        semaphore_active_leases_family = GaugeMetricFamily(
            "semaphore_active_leases",
            "Total number of currently active leases per semaphore.",
            labels=["name"],
        )
        for semaphore_name, semaphore_leases in sem_ext.leases.items():
            semaphore_active_leases_family.add_metric(
                [semaphore_name], len(semaphore_leases)
            )
        yield semaphore_active_leases_family

        semaphore_max_leases_family = GaugeMetricFamily(
            "semaphore_max_leases",
            "Maximum leases allowed per semaphore, this will be constant for each semaphore",
            labels=["name"],
        )
        for semaphore_name, max_leases in sem_ext.max_leases.items():
            semaphore_max_leases_family.add_metric([semaphore_name], max_leases)
        yield semaphore_max_leases_family

        semaphore_pending_leases = GaugeMetricFamily(
            "semaphore_pending_leases", "Leases currently pending", labels=["name"]
        )
        for semaphore_name, lease_ids in sem_ext.pending_leases.items():
            semaphore_pending_leases.add_metric([semaphore_name], len(lease_ids))


class PrometheusHandler(RequestHandler):
    _collector = None

    def __init__(self, *args, dask_server=None, **kwargs):
        import prometheus_client

        super(PrometheusHandler, self).__init__(
            *args, dask_server=dask_server, **kwargs
        )

        if PrometheusHandler._collector:
            # Especially during testing, multiple schedulers are started
            # sequentially in the same python process
            PrometheusHandler._collector.server = self.server
            return

        PrometheusHandler._collector = _PrometheusCollector(self.server)
        prometheus_client.REGISTRY.register(PrometheusHandler._collector)

    def get(self):
        import prometheus_client

        self.write(prometheus_client.generate_latest())
        self.set_header("Content-Type", "text/plain; version=0.0.4")


routes = [("/metrics", PrometheusHandler, {})]
