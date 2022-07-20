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
# Based on https://github.com/eventlet/eventlet/blob/37fca06d7466a698cf53a1ae6e4b3d840a3ced7a/eventlet/green/zmq.py

# We want to act like zmq
from zmq import *  # noqa
from zmq.constants import *  # noqa

# Explicit import to please flake8
from zmq import ZMQError

# A way to access original zmq
import zmq as _zmq

import eventlet
import eventlet.hubs
from eventlet.support import greenlets as greenlet
import errno
from logging import getLogger
from collections import deque

logger = getLogger(__name__)

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


class Context(_zmq.Context):

    def socket(self, socket_type):
        if self.closed:
            raise _zmq.ZMQError(_zmq.ENOTSUP)
        return Socket(self, socket_type)


class Socket(_zmq.Socket):

    def __init__(self, context, socket_type):
        super(Socket, self).__init__(context, socket_type)
        on_state_changed_fd = self.getsockopt(_zmq.FD)
        # NOTE: pyzmq 13.0.0 messed up with setattr (they turned it into a
        # non-op) and you can't assign attributes normally anymore, hence the
        # tricks with self.__dict__ here

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
                events = self.getsockopt(_zmq.EVENTS)
                if events & _zmq.POLLOUT:
                    self._eventlet_send_event.wake()
                if events & _zmq.POLLIN:
                    self._eventlet_recv_event.wake()

        hub = eventlet.hubs.get_hub()
        self.__dict__['_eventlet_listener'] = hub.add(hub.READ,
                                                      self.getsockopt(_zmq.FD),
                                                      event,
                                                      lambda _: None,
                                                      lambda: None)
        self.__dict__['_eventlet_clock'] = hub.clock

    def close(self, linger=None):
        super(Socket, self).close(linger)
        if self._eventlet_listener is not None:
            eventlet.hubs.get_hub().remove(self._state_event)
            self.__dict__['_eventlet_listener'] = None
            self._eventlet_send_event.wake()
            self._eventlet_recv_event.wake()

    def connect(self, *args, **kwargs):
        while True:
            try:
                return super(Socket, self).connect(*args, **kwargs)
            except _zmq.ZMQError as e:
                if e.errno not in (_zmq.EAGAIN, errno.EINTR):
                    raise

    def send(self, data, flags=0, copy=True, track=False):
        if flags & _zmq.NOBLOCK:
            result = super(Socket, self).send(data, flags, copy, track)
            # Instead of calling both wake methods, could call
            # self.getsockopt(EVENTS) which would trigger wakeups if
            # needed.
            self._eventlet_send_event.wake()
            self._eventlet_recv_event.wake()
            return result

        # TODO: pyzmq will copy the message buffer and create Message
        # objects under some circumstances. We could do that work here
        # once to avoid doing it every time the send is retried.
        flags |= _zmq.NOBLOCK
        with self._eventlet_send_lock:
            while True:
                try:
                    return super(Socket, self).send(data, flags, copy, track)
                except ZMQError as e:
                    if e.errno == _zmq.EAGAIN:
                        self._eventlet_send_event.block()
                    else:
                        raise
                finally:
                    # The call to send processes 0mq events and may
                    # make the socket ready to recv. Wake the next
                    # receiver. (Could check EVENTS for POLLIN here)
                    self._eventlet_recv_event.wake()

    def recv(self, flags=0, copy=True, track=False):
        if flags & _zmq.NOBLOCK:
            msg = super(Socket, self).recv(flags, copy, track)
            # Instead of calling both wake methods, could call
            # self.getsockopt(EVENTS) which would trigger wakeups if
            # needed.
            self._eventlet_send_event.wake()
            self._eventlet_recv_event.wake()
            return msg

        deadline = None
        if hasattr(_zmq, 'RCVTIMEO'):
            sock_timeout = self.getsockopt(_zmq.RCVTIMEO)
            if sock_timeout == -1:
                pass
            elif sock_timeout > 0:
                deadline = self._eventlet_clock() + sock_timeout / 1000.0
            else:
                raise ValueError(sock_timeout)

        flags |= _zmq.NOBLOCK
        with self._eventlet_recv_lock:
            while True:
                try:
                    return super(Socket, self).recv(flags, copy, track)
                except _zmq.ZMQError as e:
                    if e.errno == _zmq.EAGAIN:
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
                    self._eventlet_send_event.wake()

