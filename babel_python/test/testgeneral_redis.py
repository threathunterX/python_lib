#!/usr/bin/env python
# -*- coding: utf-8 -*-
from threathunter_common.redis.redisctx import RedisCtx
import pytest
from babel_python.serviceclient import ServiceClient
from babel_python.servicemeta import ServiceMeta
from babel_python.serviceserver import ServiceServer
from babel_python.unsafeserviceclient import UnsafeServiceClient
from babel_python.util import *
from threathunter_common.event import Event


__author__ = 'lw'


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
    MODE = "clientserver"
    CLIENT = "client" in MODE
    SERVER = "server" in MODE
    server_repo = dict()

    def setup_class(self):
        RedisCtx.get_instance().host = "127.0.0.1"
        RedisCtx.get_instance().port = 6379
        print "start to init service"
        TestService.init_services()
        print "successfully started the services"

    def setup_method(self, method):
        if not TestService.SERVER:
            return
        for v in TestService.server_repo.values():
            server = v["server"]
            results = v["results"]
            results[:] = []

    @staticmethod
    def build_service(id, sname, callmode, delivermode, subname="", sequence="", cardinality=0):
        s = ServiceMeta.from_dict(
            {
                "name": sname,
                "callmode": callmode,
                "delivermode": delivermode,
                "serverimpl": "redis",
                "coder": "mail",
                "options": {
                    "cdc": "sh",
                    "sdc": "sh",
                    "serversubname": subname,
                    "serverseq": sequence,
                    "servercardinality": cardinality
                }
            })

        # start the relative server in server mode
        if "server" in TestService.MODE:
            echo_service, results = get_echo_service()
            server = ServiceServer(s, echo_service)
            server.start()
        else:
            server = None
            results = None

        TestService.server_repo[id] = {"service": s, "server": server, "results": results}

    @staticmethod
    def init_services():
        # queue notify
        TestService.build_service("service_notify_queue", "service_notify_queue", "notify", "queue")

        # 3 services use topic to get data
        TestService.build_service("service_notify_topic1", "service_notify_topic", "notify", "topic", "consumer1")
        TestService.build_service("service_notify_topic2", "service_notify_topic", "notify", "topic", "consumer2")
        TestService.build_service("service_notify_topic3", "service_notify_topic", "notify", "topic", "consumer3")

        # 3 services use sharding
        TestService.build_service("service_notify_sharding1", "service_notify_sharding", "notify", "sharding", "", 1, 3)
        TestService.build_service("service_notify_sharding2", "service_notify_sharding", "notify", "sharding", "", 2, 3)
        TestService.build_service("service_notify_sharding3", "service_notify_sharding", "notify", "sharding", "", 3, 3)

        #  3 services use shuffle
        TestService.build_service("service_notify_shuffle1", "service_notify_shuffle", "notify", "shuffle", "", 1, 3)
        TestService.build_service("service_notify_shuffle2", "service_notify_shuffle", "notify", "shuffle", "", 2, 3)
        TestService.build_service("service_notify_shuffle3", "service_notify_shuffle", "notify", "shuffle", "", 3, 3)

        #  rpc queue
        TestService.build_service("service_rpc_queue", "service_rpc_queue", "rpc", "queue", "", 1, 1)

        #  2 sharding services for rpc
        TestService.build_service("service_rpc_sharding1", "service_rpc_sharding", "rpc", "sharding", "", 1, 2)
        TestService.build_service("service_rpc_sharding2", "service_rpc_sharding", "rpc", "sharding", "", 2, 2)
        #
        #  2 shuffle services for rpc
        TestService.build_service("service_rpc_shuffle1", "service_rpc_shuffle", "rpc", "shuffle", "", 1, 2)
        TestService.build_service("service_rpc_shuffle2", "service_rpc_shuffle", "rpc", "shuffle", "", 2, 2)
        #
        #  2 topic services for polling
        TestService.build_service("service_polling_topic1", "service_polling_topic", "polling", "topic", "consumer1", 1, 2)
        TestService.build_service("service_polling_topic2", "service_polling_topic", "polling", "topic", "consumer2", 2, 2)
        #

    @pytest.mark.skipif("'client' not in TestService.MODE")
    def test_notify_queue(self):
        if TestService.CLIENT:
            self.client = ServiceClient(TestService.server_repo["service_notify_queue"]["service"])
            self.client.start()
            self.client.send(TestService.EVENT, "", True, 1)

        if TestService.CLIENT and TestService.SERVER:
            sleep(2)
            results = TestService.server_repo["service_notify_queue"]["results"]
            assert len(results) == 1
            assert results[0] == TestService.EVENT

    @pytest.mark.skipif("'client' not in TestService.MODE")
    def test_notify_topic(self):
        if TestService.CLIENT:
            self.client = ServiceClient(TestService.server_repo["service_notify_topic1"]["service"])
            self.client.start()
            self.client.send(TestService.EVENT, "", True, 1)

        if TestService.CLIENT and TestService.SERVER:
            sleep(1)
            results = TestService.server_repo["service_notify_topic1"]["results"]
            assert len(results) == 1
            assert results[0] == TestService.EVENT
            results = TestService.server_repo["service_notify_topic2"]["results"]
            assert len(results) == 1
            assert results[0] == TestService.EVENT

    @pytest.mark.skipif("'client' not in TestService.MODE")
    def test_notify_sharding(self):
        if TestService.CLIENT:
            self.client = ServiceClient(TestService.server_repo["service_notify_sharding1"]["service"])
            self.client.start()
            for i in range(20):
                ev = TestService.EVENT.copy()
                ev.key = str(i)
                self.client.send(ev, ev.key, True, 1)

        if TestService.CLIENT and TestService.SERVER:
            sleep(1)
            results1 = TestService.server_repo["service_notify_sharding1"]["results"]
            results2 = TestService.server_repo["service_notify_sharding2"]["results"]
            results3 = TestService.server_repo["service_notify_sharding3"]["results"]
            assert len(results1) + len(results2) + len(results3) == 20

            old_size1 = len(results1)
            old_size2 = len(results2)
            old_size3 = len(results3)
            results1[:] = []
            results2[:] = []
            results3[:] = []

            for i in range(100):
                ev = TestService.EVENT.copy()
                ev.key = str(i%20)
                self.client.send(ev, ev.key, True, 1)

            sleep(1)
            results1 = TestService.server_repo["service_notify_sharding1"]["results"]
            results2 = TestService.server_repo["service_notify_sharding2"]["results"]
            results3 = TestService.server_repo["service_notify_sharding3"]["results"]
            new_size1 = len(results1)
            new_size2 = len(results2)
            new_size3 = len(results3)
            print old_size1
            print old_size2
            print old_size3
            print new_size1
            print new_size2
            print new_size3

            assert old_size1 * 5 == new_size1
            assert old_size2 * 5 == new_size2
            assert old_size3 * 5 == new_size3

    @pytest.mark.skipif("'client' not in TestService.MODE")
    def test_notify_shuffle(self):
        if TestService.CLIENT:
            self.client = ServiceClient(TestService.server_repo["service_notify_shuffle1"]["service"])
            self.client.start()
            for i in range(20):
                ev = TestService.EVENT.copy()
                ev.key = str(i)
                self.client.send(ev, ev.key, True, 1)

        if TestService.CLIENT and TestService.SERVER:
            sleep(1)
            results1 = TestService.server_repo["service_notify_shuffle1"]["results"]
            results2 = TestService.server_repo["service_notify_shuffle2"]["results"]
            results3 = TestService.server_repo["service_notify_shuffle3"]["results"]
            assert len(results1) + len(results2) + len(results3) == 20
            assert len(results1) > 0
            assert len(results2) > 0
            assert len(results3) > 0

    @pytest.mark.skipif("'client' not in TestService.MODE")
    def test_rpc_queue(self):
        if TestService.CLIENT:
            self.client = ServiceClient(TestService.server_repo["service_rpc_queue"]["service"])
            self.client.start()
            result = self.client.send(TestService.EVENT, "", True, 3)
            assert result == (True, TestService.EVENT)

        if TestService.CLIENT and TestService.SERVER:
            results = TestService.server_repo["service_rpc_queue"]["results"]
            assert len(results) == 1
            assert results[0] == TestService.EVENT

    @pytest.mark.skipif("'client' not in TestService.MODE")
    def test_rpc_topic(self):
        if TestService.CLIENT:
            self.client = ServiceClient(TestService.server_repo["service_polling_topic1"]["service"])
            self.client.start()
            result = self.client.send(TestService.EVENT, "", True, 1)
            assert result == (True, [TestService.EVENT, TestService.EVENT])

        if TestService.CLIENT and TestService.SERVER:
            results1 = TestService.server_repo["service_polling_topic1"]["results"]
            results2 = TestService.server_repo["service_polling_topic2"]["results"]
            assert len(results1) == 1
            assert len(results2) == 1

    @pytest.mark.skipif("'client' not in TestService.MODE")
    def test_rpc_sharding(self):
        if TestService.CLIENT:
            self.client = ServiceClient(TestService.server_repo["service_rpc_sharding1"]["service"])
            self.client.start()
            result = self.client.send(TestService.EVENT, "", True, 1)
            assert result == (True, TestService.EVENT)

        if TestService.CLIENT and TestService.SERVER:
            results1 = TestService.server_repo["service_rpc_sharding1"]["results"]
            results2 = TestService.server_repo["service_rpc_sharding2"]["results"]
            assert len(results1) + len(results2) == 1

    @pytest.mark.skipif("'client' not in TestService.MODE")
    def test_rpc_shuffle(self):
        if TestService.CLIENT:
            self.client = ServiceClient(TestService.server_repo["service_rpc_shuffle1"]["service"])
            self.client.start()
            result = self.client.send(TestService.EVENT, "", True, 1)
            assert result == (True, TestService.EVENT)

        if TestService.CLIENT and TestService.SERVER:
            results1 = TestService.server_repo["service_rpc_shuffle1"]["results"]
            results2 = TestService.server_repo["service_rpc_shuffle2"]["results"]
            assert len(results1) + len(results2) == 1

    def _performance_test_one_thread(self, client, round, service):
        if TestService.CLIENT:
            for i in xrange(round):
                result = client.send(TestService.EVENT, "", True, 5)
                if not result[0]:
                    assert result[0] == True

    @pytest.mark.skipif("'client' not in TestService.MODE")
    def test_notify_queue_performance_single_thread(self):
        round = 10000
        if TestService.CLIENT:
            service = TestService.server_repo["service_notify_queue"]["service"]
            client = ServiceClient(service)
            start = millis_now()
            self._performance_test_one_thread(client, round, service)
            end = millis_now()
            client.close()
            print "spent {} milliseconds on sending {} records, {} per second on average".format((end-start), round, round*1000.0/(end-start))

            if TestService.CLIENT and TestService.SERVER:
                results = TestService.server_repo["service_notify_queue"]["results"]
                current = millis_now()
                while len(results) != round and (current - start) <= 40000:
                    sleep(0.1)
                    current = millis_now()

                print "spent {} milliseconds on sending {} records, {} per second on average".format((current-start),
                                                                                                     len(results),
                                                                                                     len(results)*1000.0/(current-start))
            assert False

    @pytest.mark.skipif("'client' not in TestService.MODE")
    def test_notify_queue_performance_multiple_thread(self):
        round = 10000
        numOfThreads = 10
        if TestService.CLIENT:
            service = TestService.server_repo["service_notify_queue"]["service"]
            threads = []
            clients = [ServiceClient(service) for i in range(numOfThreads)]
            for client in clients:
                client.start()

            start = millis_now()
            for i in range(numOfThreads):
                threads.append(run_in_thread(self._performance_test_one_thread, clients[i], round/numOfThreads, service))
            for i in range(numOfThreads):
                threads[i].join()
            end = millis_now()

            print "spent {} milliseconds on sending {} records, {} per second on average".format((end-start), round, round*1000.0/(end-start))
            if TestService.CLIENT and TestService.SERVER:
                results = TestService.server_repo["service_notify_queue"]["results"]
                current = millis_now()
                while len(results) != round and (current - start) <= 40000:
                    sleep(0.1)
                    current = millis_now()

                print "spent {} milliseconds on receiving {} records, {} per second on average".format((current-start),
                                                                                                     len(results),
                                                                                                     len(results)*1000.0/(current-start))
            for client in clients:
                client.close()
            assert False

    @pytest.mark.skipif("'client' not in TestService.MODE")
    def test_rpc_queue_performance_single_thread(self):
        round = 5000
        if TestService.CLIENT:
            service = TestService.server_repo["service_rpc_queue"]["service"]
            client = ServiceClient(service)
            client.start()
            start = millis_now()
            self._performance_test_one_thread(client, round, service)
            end = millis_now()
            client.close()
            print "spent {} milliseconds on sending {} records, {} per second on average".format((end-start), round, round*1000.0/(end-start))
            assert False

    @pytest.mark.skipif("'client' not in TestService.MODE")
    def test_rpc_queue_performance_multiple_thread(self):
        round = 5000
        numOfThreads = 10
        if TestService.CLIENT:
            service = TestService.server_repo["service_rpc_queue"]["service"]
            self.client = ServiceClient(service)
            self.client.start()
            start = millis_now()
            threads = []
            for i in range(numOfThreads):
                threads.append(run_in_thread(self._performance_test_one_thread, self.client, round/numOfThreads, service))
            for i in range(numOfThreads):
                threads[i].join()
            end = millis_now()

            print "spent {} milliseconds on sending {} records, {} per second on average".format((end-start), round, round*1000.0/(end-start))
            self.client.close()
            assert False
            return

    # @pytest.mark.skipif("'client' not in TestService.MODE")
    # def test_rpc_queue_performance_multiple_thread_multiple_client(self):
    #     round = 4000
    #     numOfThreads = 10
    #     if TestService.CLIENT:
    #         service = TestService.server_repo["service_rpc_queue"]["service"]
    #         clients = [ServiceClient(service) for i in range(numOfThreads)]
    #         for client in clients:
    #             client.start()
    #         start = millis_now()
    #         threads = []
    #         for i in range(numOfThreads):
    #             threads.append(run_in_thread(self._performance_test_one_thread, clients[i], round/numOfThreads, service))
    #         for i in range(numOfThreads):
    #             threads[i].join()
    #         end = millis_now()
    #         for client in clients:
    #             client.close()
    #         print "spent {} milliseconds on sending {} records, {} per second on average".format((end-start), round, round*1000.0/(end-start))
    #         assert False
    #         return

    @pytest.mark.skipif("'server' != TestService.MODE")
    def test_server(self):
        sleep(200)


# if __name__ == "__main__":
#     import cProfile
#     ts = TestService()
#     ts.setup_class()
#     ts.test_rpc_queue_performance_multiple_thread()
#     pass