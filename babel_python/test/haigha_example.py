#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys, os
sys.path.append(os.path.abspath("."))
sys.path.append(os.path.abspath(".."))

import logging
import random
import socket
from optparse import OptionParser

from haigha.connections.rabbit_connection import Connection
from haigha.message import Message
import signal
import time
import gevent
from gevent import monkey

def sigint_cb(*args):
  logger.info("stopping test")
  print_stats()

  # Have to iterate on a copy because list will be modified on close()
  for channel in channels[:]:
    channel.close()

  while channels:
    gevent.sleep(1)

  connection.close()

def print_stats():
  t_end = time.time()
  total = 0
  duration = t_end - t_start

  for channel in channels:
    total += channel._count

  logger.info("completed %d in %.06f", total, duration)
  logger.info("%0.6f msg/s", float(total) / duration )
  logger.info("frames read: %d, %f/s",
    connection.frames_read, float(connection.frames_read)/duration )
  logger.info("frames written: %d, %f/s",
    connection.frames_written, float(connection.frames_written)/duration )


def channel_closed(channel):
  channels.remove( channel )

def connection_close_cb():
  pass


def open_channel():
  channels.append( ChannelTest(connection, random.choice(exchanges)) )

class ChannelTest:
  def __init__(self, connection, exchange):
    self._ch = connection.channel()
    self._exchange = exchange
    self._queue = '%s'%(self._ch.channel_id)
    self._count = 0

    self._ch.exchange.declare( self._exchange, 'direct', auto_delete=True )
    self._ch.queue.declare( self._queue, auto_delete=True )
    self._ch.queue.bind( self._queue, self._exchange, self._queue )
    self._ch.basic.consume( self._queue, self._consume )

    self._publish()

  @property
  def _transactions(self):
    return options.tx

  def close_and_reopen(self):
    if not self._ch.closed:
      self.close()
      open_channel()

  def close(self):
    self._ch.close()
    # HACK: Without a proper callback chain, need to delay this so that rabbit
    # 2.4.0 can handle the handshake of channel close before we handshake the
    # connection close. Otherwise, it gets both close requests in rapid
    # succession and doesn't ack either of them, resulting in a force quit
    #event.timeout( 0.3, channel_closed, self )
    channel_closed(self)

  def _publish(self):
    self._count += 1
    if not self._ch.closed:
      msg = Message( body='hello world' )

      if self._transactions:
        self._ch.publish_synchronous( msg, exchange=self._exchange, routing_key=self._queue, cb=self._published )
      else:
        self._ch.publish( msg, exchange=self._exchange, routing_key=self._queue )

  def _published(self):
    self._publish()

  def _consume(self, msg):
    if not self._transactions:
      self._publish()

###################################################################

parser = OptionParser(
  usage='Usage: stress_test [options]'
)
parser.add_option('--user', default='guest', type='string')
parser.add_option('--pass', default='guest', dest='password', type='string')
parser.add_option('--vhost', default='/', type='string')
parser.add_option('--host', default='localhost', type='string')
parser.add_option('--tx', default=False, action='store_true' )
parser.add_option('--profile', default=False, action='store_true' )
parser.add_option('--channels', default=500, type='int')
parser.add_option('--debug', default=0, action='count')
parser.add_option('--time', default=0, type='int')
parser.add_option('--transport', default='gevent', choices=['event', 'gevent', 'gevent_pool'])

(options,args) = parser.parse_args()

debug = options.debug
level = logging.DEBUG if debug else logging.INFO

# Setup logging
logging.basicConfig(level=level, format="[%(levelname)s %(asctime)s] %(message)s" )
logger = logging.getLogger('haigha')

channels = []

logger.info( 'connecting with transport %s ...', options.transport )

# Need to monkey patch before the connection is made
if options.transport in ('gevent', 'gevent_pool'):
  monkey.patch_all()

sock_opts = {
  (socket.IPPROTO_TCP, socket.TCP_NODELAY) : 1,
}
import haigha
connection = haigha.connections.rabbit_connection.RabbitConnection(logger=logger, debug=debug,
  user=options.user, password=options.password,
  vhost=options.vhost, host=options.host,
  heartbeat=None, close_cb=connection_close_cb,
  sock_opts=sock_opts,
  transport=options.transport)

exchanges = ['publish-%d'%(x) for x in xrange(0,10)]

for x in xrange(0,options.channels):
  open_channel()

if options.transport=='gevent':
  def frame_loop():
    while True:
      connection.read_frames()
      gevent.sleep(0)

  t_start = time.time()
  greenlet = gevent.spawn( frame_loop )
  greenlet.join( options.time )

  print_stats()

elif options.transport=='gevent_pool':
  def frame_loop():
    while True:
      connection.read_frames()
      gevent.sleep(0)

  t_start = time.time()
  connection.transport.pool.spawn( frame_loop )
  connection.transport.pool.join( options.time )

  print_stats()
