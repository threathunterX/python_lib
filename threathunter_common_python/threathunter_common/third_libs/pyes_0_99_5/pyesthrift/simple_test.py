#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import print_function

import pprint

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from .Rest import Client
from .ttypes import *


pp = pprint.PrettyPrinter(indent = 4)
host = '127.0.0.1'
port = 9500
uri = ''
framed = False
http = False
argi = 1

socket = TSocket.TSocket(host, port)
transport = TTransport.TBufferedTransport(socket)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Client(protocol)
transport.open()

res = RestRequest(0, "/test-index/test-type/1", {}, {})
print(client.execute(res))

transport.close()
