# -*- coding: utf-8 -*-
# Open Source Initiative OSI - The MIT License (MIT):Licensing
#
# The MIT License (MIT)
# Copyright (c) 2015 François-Xavier Bourlet (bombela+zerorpc@gmail.com)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Based on https://github.com/traviscline/gevent-zeromq/blob/master/gevent_zeromq/core.py


from __future__ import print_function

import zmq
from zmq.constants import *

import eventlet
import eventlet.hubs
import greenlet
from collections import deque

STOP_EVERYTHING = False

class LockReleaseError(Exception):
    pass

class _QueueLock(object):
    """A Lock that can be acquired by at most one thread. Any other
    thread calling acquire will be blocked in a queue. When release
    is called, the threads are awoken in the order they blocked,
    one at a time. This lock can be required recursively by the same
    thread."""

    def __init__(self):
        self._waiters = deque()
        self._count = 0
        self._holder = None
        self._hub = eventlet.hubs.get_hub()

    def __nonzero__(self):
        return bool(self._count)

    __bool__ = __nonzero__

    def __enter__(self):
        self.acquire()

    def __exit__(self, type, value, traceback):
        self.release()

    def acquire(self):
        current = greenlet.getcurrent()
        if (self._waiters or self._count > 0) and self._holder is not current:
            # block until lock is free
            self._waiters.append(current)
            self._hub.switch()
            w = self._waiters.popleft()

            assert w is current, 'Waiting threads woken out of order'
            assert self._count == 0, 'After waking a thread, the lock must be unacquired'

        self._holder = current
        self._count += 1

    def release(self):
        if self._count <= 0:
            raise LockReleaseError("Cannot release unacquired lock")

        self._count -= 1
        if self._count == 0:
            self._holder = None
            if self._waiters:
                # wake next
                self._hub.schedule_call_global(0, self._waiters[0].switch)


class _BlockedThread(object):
    """Is either empty, or represents a single blocked thread that
    blocked itself by calling the block() method. The thread can be
    awoken by calling wake(). Wake() can be called multiple times and
    all but the first call will have no effect."""

    def __init__(self):
        self._blocked_thread = None
        self._wakeupper = None
        self._hub = eventlet.hubs.get_hub()

    def __nonzero__(self):
        return self._blocked_thread is not None

    __bool__ = __nonzero__

    def block(self, deadline=None):
        if self._blocked_thread is not None:
            raise Exception("Cannot block more than one thread on one BlockedThread")
        self._blocked_thread = greenlet.getcurrent()

        if deadline is not None:
            self._hub.schedule_call_local(deadline - self._hub.clock(), self.wake)

        try:
            self._hub.switch()
        finally:
            self._blocked_thread = None
            # cleanup the wakeup task
            if self._wakeupper is not None:
                # Important to cancel the wakeup task so it doesn't
                # spuriously wake this greenthread later on.
                self._wakeupper.cancel()
                self._wakeupper = None

    def wake(self):
        """Schedules the blocked thread to be awoken and return
        True. If wake has already been called or if there is no
        blocked thread, then this call has no effect and returns
        False."""
        if self._blocked_thread is not None and self._wakeupper is None:
            self._wakeupper = self._hub.schedule_call_global(0, self._blocked_thread.switch)
            return True
        return False


class ZMQSocket(zmq.Socket):

    def __init__(self, context, socket_type):
        super(ZMQSocket, self).__init__(context, socket_type)
        on_state_changed_fd = self.getsockopt(zmq.FD)
        self.__dict__['_eventlet_send_event'] = _BlockedThread()
        self.__dict__['_eventlet_recv_event'] = _BlockedThread()
        self.__dict__['_eventlet_send_lock'] = _QueueLock()
        self.__dict__['_eventlet_recv_lock'] = _QueueLock()

        def event(fd):
            # Some events arrived at the zmq socket. This may mean
            # there's a message that can be read or there's space for
            # a message to be written.
            send_wake = self._eventlet_send_event.wake()
            recv_wake = self._eventlet_recv_event.wake()
            if not send_wake and not recv_wake:
                # if no waiting send or recv thread was woken up, then
                # force the zmq socket's events to be processed to
                # avoid repeated wakeups
                events = self.getsockopt(zmq.EVENTS)
                if events & zmq.POLLOUT:
                    self._eventlet_send_event.wake()
                if events & zmq.POLLIN:
                    self._eventlet_recv_event.wake()

        hub = eventlet.hubs.get_hub()
        self.__dict__['_eventlet_listener'] = hub.add(hub.READ,
                                                      self.getsockopt(FD),
                                                      event,
                                                      lambda _: None,
                                                      lambda: None)
        self.__dict__['_eventlet_clock'] = hub.clock

    def _on_state_changed(self, event=None, _evtype=None):
        if self.closed:
            self._writable.set()
            self._readable.set()
            return

        events = self.getsockopt(zmq.EVENTS)
        if events & zmq.POLLOUT:
            self._writable.set()
        if events & zmq.POLLIN:
            self._readable.set()

    def close(self, linger=None):
        super(ZMQSocket, self).close(linger)
        if self._eventlet_listener is not None:
            eventlet.hubs.get_hub().remove(self._state_event)
            self.__dict__['_eventlet_listener'] = None
            self._eventlet_send_event.wake()
            self._eventlet_recv_event.wake()

    def send(self, data, flags=0, copy=True, track=False):
        if flags & zmq.NOBLOCK:
            result = super(ZMQSocket, self).send(data, flags, copy, track)
            # Instead of calling both wake methods, could call
            # self.getsockopt(EVENTS) which would trigger wakeups if
            # needed.
            self._eventlet_send_event.wake()
            self._eventlet_recv_event.wake()
            return result

        # TODO: pyzmq will copy the message buffer and create Message
        # objects under some circumstances. We could do that work here
        # once to avoid doing it every time the send is retried.
        flags |= zmq.NOBLOCK
        with self._eventlet_send_lock:
            while True:
                try:
                    return super(ZMQSocket, self).send(data, flags, copy, track)
                except zmq.ZMQError as e:
                    if e.errno == zmq.EAGAIN:
                        self._eventlet_send_event.block()
                    else:
                        raise
                finally:
                    # The call to send processes 0mq events and may
                    # make the socket ready to recv. Wake the next
                    # receiver. (Could check EVENTS for POLLIN here)
                    self._eventlet_recv_event.wake()

    def recv(self, flags=0, copy=True, track=False):
        if flags & zmq.NOBLOCK:
            msg = super(ZMQSocket, self).recv(flags, copy, track)
            # Instead of calling both wake methods, could call
            # self.getsockopt(EVENTS) which would trigger wakeups if
            # needed.
            self._eventlet_send_event.wake()
            self._eventlet_recv_event.wake()
            return msg

        deadline = None
        if hasattr(zmq, 'RCVTIMEO'):
            sock_timeout = self.getsockopt(zmq.RCVTIMEO)
            if sock_timeout == -1:
                pass
            elif sock_timeout > 0:
                deadline = self._eventlet_clock() + sock_timeout / 1000.0
            else:
                raise ValueError(sock_timeout)

        flags |= zmq.NOBLOCK
        with self._eventlet_recv_lock:
            while True:
                try:
                    return super(ZMQSocket, self).recv(flags, copy, track)
                except zmq.ZMQError as e:
                    if e.errno == zmq.EAGAIN:
                        # zmq in its wisdom decided to reuse EAGAIN for timeouts
                        if deadline is not None and self._eventlet_clock() > deadline:
                            e.is_timeout = True
                            raise

                        self._eventlet_recv_event.block(deadline=deadline)
                    else:
                        raise
                finally:
                    # The call to recv processes 0mq events and may
                    # make the socket ready to send. Wake the next
                    # receiver. (Could check EVENTS for POLLOUT here)
                    while self._eventlet_send_event.wake():
                        events = self.getsockopt(zmq.EVENTS)
                        if bool(events & zmq.POLLIN):
                            print("here we go, nobody told me about new messages!")
                            global STOP_EVERYTHING
                            STOP_EVERYTHING = True
                            raise greenlet.GreenletExit()

zmq_context = zmq.Context()


def server():
    socket = ZMQSocket(zmq_context, zmq.REP)
    socket.bind('ipc://zmqbug')

    class Cnt(object):
        responded = 0

    cnt = Cnt()

    def responder():
        while not STOP_EVERYTHING:
            msg = socket.recv()
            socket.send(msg)
            cnt.responded += 1

    eventlet.spawn(responder)

    while not STOP_EVERYTHING:
        print("cnt.responded=", cnt.responded)
        eventlet.sleep(0.5)


def client():
    socket = ZMQSocket(zmq_context, zmq.DEALER)
    socket.connect('ipc://zmqbug')

    class Cnt(object):
        recv = 0
        send = 0

    cnt = Cnt()

    def recvmsg():
        while not STOP_EVERYTHING:
            socket.recv()
            socket.recv()
            cnt.recv += 1

    def sendmsg():
        while not STOP_EVERYTHING:
            socket.send(b'', flags=zmq.SNDMORE)
            socket.send(b'hello')
            cnt.send += 1
            eventlet.sleep(0)

    eventlet.spawn(recvmsg)
    eventlet.spawn(sendmsg)

    while not STOP_EVERYTHING:
        print("cnt.recv=", cnt.recv, "cnt.send=", cnt.send)
        eventlet.sleep(0.5)

eventlet.spawn(server)
client()
