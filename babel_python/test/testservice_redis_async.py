from threathunter_common.metrics.metricsagent import MetricsAgent
from threathunter_common.redis.redisctx import RedisCtx
from babel_python.servicemeta import ServiceMeta
from babel_python.serviceserver_async import *
from babel_python.serviceclient_async import *
from threathunter_common.event import Event
import time

__author__ = 'luwen'


def sleep(timeout):
    time.sleep(timeout)


def get_echo_service():
    results = list()

    def echo_service(request):
        results.append(request)
        return request
    return echo_service, results


class TestService:
    EVENT = Event("testapp", "testname", "testkey", 0, {})

    MetricsAgent.get_instance().initialize_by_dict({
        "server": "redis",
        "redis": {
            "type": "redis",
            "host": "127.0.0.1",
            "port": 6379
        }
    })

    def setup_method(self, method):
        RedisCtx.get_instance().host = "127.0.0.1"
        RedisCtx.get_instance().port = 6379

    def test_notify_queue_service(self):
        service_data = [
            {
                "callmode": "notify",
                "delivermode": "queue",
            }, {
                "callmode": "notify",
                "delivermode": "queue",
            }
        ]
        client_result, server_result = self._common_test_process(service_data)
        assert client_result[0] == (True, None)
        sleep(1)
        assert server_result[0] == [TestService.EVENT]

    def test_notify_topic_service(self):
        service_data = [
            {
                "callmode": "notify",
                "delivermode": "topic",
                "serversubname": "consumer1"
            }, {
                "callmode": "notify",
                "delivermode": "topic",
                "serversubname": "consumer2"
            }
        ]
        client_result, server_result = self._common_test_process(service_data)
        assert client_result[0] == (True, None)
        assert server_result[0] == [TestService.EVENT]
        assert server_result[1] == [TestService.EVENT]

    def test_notify_sharding_service(self):
        service_data = [
            {
                "callmode": "notify",
                "delivermode": "sharding",
                "serverseq": 1
            }, {
                "callmode": "notify",
                "delivermode": "sharding",
                "serverseq": 2
            }
        ]

        events = list()
        for i in range(10):
            e = TestService.EVENT.copy()
            e.key = str(i)
            events.append(e)

        client_result, server_result = self._common_test_process(service_data, events)
        print events
        print server_result
        assert client_result == [(True, None)] * 10
        assert len(server_result[0])+len(server_result[1]) == 10
        assert set(events) == set(server_result[0]+server_result[1])

    def test_batch_notify_service(self):
        events = list()
        for i in range(10):
            e = TestService.EVENT.copy()
            e.key = str(i)
            events.append(e)

        s = ServiceMeta.from_dict(
            {
                "name": "test",
                "callmode": "notify",
                "delivermode": "queue",
                "serverimpl": "redis",
                "coder": "mail",
                "options": {
                    "cdc": "sh",
                    "sdc": "sh"
                }
            })

        client = ServiceClient(s)
        client.start()

        echo_service, results = get_echo_service()
        server = ServiceServer(s, echo_service)
        server.start()

        if events is None:
            events = [TestService.EVENT]

        client_response_list = list()
        for event in events:
            response = client.batch_notify(event, event.key, limit=5)
            client_response_list.append(response)

            sleep(0.1)
            print len(results)

        sleep(1)
        client.close()
        server.close()
        print client_response_list
        print results

    def test_rpc_queue_service(self):
        service_data = [
            {
                "callmode": "rpc",
                "delivermode": "queue",
            }
        ]
        client_result, server_result = self._common_test_process(service_data)
        print client_result, server_result
        assert client_result[0] == (True, TestService.EVENT)
        assert server_result[0] == [TestService.EVENT]

    def test_mrpc_topic_service(self):
        service_data = [
            {
                "callmode": "polling",
                "delivermode": "topic",
                "servercardinality": 2,
                "serversubname": "consumer1",
                "serverseq": 1
            },
            {
                "callmode": "polling",
                "delivermode": "topic",
                "servercardinality":2,
                "serversubname": "consumer2",
                "serverseq": 2
            }
        ]
        client_result, server_result = self._common_test_process(service_data)
        print client_result, server_result
        assert client_result[0] == (True, [TestService.EVENT, TestService.EVENT])
        assert server_result[0] == [TestService.EVENT]
        assert server_result[1] == [TestService.EVENT]

    def _common_test_process(self, service_data, events = None):
        if not isinstance(service_data, list):
            service_data = [service_data]
        services = list()
        for d in service_data:
            s = ServiceMeta.from_dict(
            {
                "name": "test",
                "callmode": d.get("callmode", "notify"),
                "delivermode": d.get("delivermode", "queue"),
                "serverimpl": "redis",
                "coder": "mail",
                "options": {
                    "cdc": "sh",
                    "sdc": "sh",
                    "serversubname": d.get("serversubname", ""),
                    "serverseq": d.get("serverseq", ""),
                    "servercardinality": d.get("servercardinality", 1)
                }
            })
            services.append(s)

        client = ServiceClient(services[0])
        client.start()

        servers = list()
        server_results_list = list()
        for s in services:
            echo_service, results = get_echo_service()
            server = ServiceServer(s, echo_service)
            servers.append(server)
            server.start()
            server_results_list.append(results)

        if events is None:
            events = [TestService.EVENT]

        client_response_list = list()
        for event in events:
            response = client.send(event, event.key, timeout=5)
            client_response_list.append(response)

        sleep(1)
        client.close()
        map(lambda s: s.close(), servers)
        return client_response_list, server_results_list

