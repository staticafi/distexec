#!/usr/bin/env python
#
# This script distributes task between computers. The task
# is to be run the Symbiotic tool on given benchark.
#
# Copyright (c) 2014,2018 Marek Chalupa
# E-mail: chalupa@fi.muni.cz
#
# Permission to use, copy, modify, distribute, and sell this software and its
# documentation for any purpose is hereby granted without fee, provided that
# the above copyright notice appear in all copies and that both that copyright
# notice and this permission notice appear in supporting documentation, and
# that the name of the copyright holders not be used in advertising or
# publicity pertaining to distribution of the software without specific,
# written prior permission. The copyright holders make no representations
# about the suitability of this software for any purpose. It is provided "as
# is" without express or implied warranty.
#
# THE COPYRIGHT HOLDERS DISCLAIM ALL WARRANTIES WITH REGARD TO THIS SOFTWARE,
# INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO
# EVENT SHALL THE COPYRIGHT HOLDERS BE LIABLE FOR ANY SPECIAL, INDIRECT OR
# CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
# TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE
# OF THIS SOFTWARE.
#

import subprocess
import select
import fcntl
import os
from time import strftime, sleep

class Monitor:
    def _notify(self, fd, data, isstderr):
        "Notify monitor about data"
        self.callback(fd, data, isstderr)

    def callback(self, fd, data, isstderr):
        "Take action on the data. Overridden by the user."
        raise NotImplementedError("Must be overriden")

    def input(self, fd, data, isstderr):
        "Take action on the data. Overridden by the user."
        raise NotImplementedError("Must be overriden")

    def done(self, fd, exitstatus):
        return exitstatus

class LineMonitor(Monitor):
    def __init__(self):
        self._stdout_line = ''
        self._stderr_line = ''

    def _notify(self, fd, data, isstderr):
        "Notify monitor about data"
        for d in data:
            if ord(d) != ord('\n'):
                if isstderr:
                    self._stderr_line += chr(ord(d))
                else:
                    self._stdout_line += chr(ord(d))
            else:
                if isstderr:
                    self.callback(fd, self._stderr_line, isstderr)
                    self._stderr_line = ''
                else:
                    self.callback(fd, self._stdout_line, isstderr)
                    self._stdout_line = ''

    def callback(self, fd, line, isstderr):
        raise NotImplementedError("Must be overriden")


class PrintMonitor(LineMonitor):
    def callback(self, fd, line, isstderr):
        if isstderr:
            print("[{0} stderr]: {1}".format(fd, line))
        else:
            print("[{0} stdout]: {1}".format(fd, line))

class Dispatcher(object):
    def __init__(self, monitor = None):
        self._poller = select.poll()
        self._fds = {}
        self._monitor = monitor

    def _registerFd(self, fd, data):
        """ Add new fd to the poller """

        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        self._fds[fd] = data
        self._poller.register(fd, select.POLLIN | #select.POLLOUT |
                              select.POLLERR | select.POLLHUP)

    def _unregisterFd(self, fd):
        """ Remove fd from the poller """

        data = self._fds[fd]
        self._fds.pop(fd)
        self._poller.unregister(fd)

        return data

    def _killTasks(self):
        for (proc, monitor) in self._fds.values():
            if proc.poll() is None:
                proc.terminate()
            # for sure
            if proc.poll() is None:
                proc.kill()

    def _dispatch(self):
        for fd, flags in self._poller.poll():
            (proc, monitor) = self._fds.get(fd)
            if flags & select.POLLIN:
                assert proc
                if fd == proc.stdout.fileno():
                    data = proc.stdout.read()
                elif fd == proc.stderr.fileno():
                    data = proc.stderr.read()
                else:
                    assert False

                if monitor:
                    monitor._notify(fd, data, fd == proc.stderr.fileno())
           #elif flags & select.POLLOUT:
           #    if monitor:
           #        data = monitor.input()
           #        proc.stdin.write(data)
           #        proc.stdin.flush()
           #    else:
           #        self._killTasks()
           #        assert False
            elif flags & select.POLLHUP:
                # the process finished
                # remove the old benchmark
                if monitor:
                    monitor.done(fd, proc.poll())
                self._unregisterFd(fd)

                if self._monitor:
                    self._monitor.done(fd, proc, monitor, proc.poll())
            elif flags & select.POLLERR:
                print('Waiting for benchmark failed')
                self._killTasks()
                return False

        return self._fds != {}

    def registerProcess(self, proc, monitor):
        """ Set running process to be tracked down by poller """
        self._registerFd(proc.stdout.fileno(), (proc, monitor))
        self._registerFd(proc.stderr.fileno(), (proc, monitor))
        #self._registerFd(proc.stdin.fileno(), (proc, monitor))

    def run(self, cmd, monitor):
        """ Dispatch tasks over network and wait for outcomes """
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             stdin=subprocess.PIPE)
        self.registerProcess(p, monitor)

    def monitor(self, monitor = None):
        if monitor:
            self._monitor = monitor

        try:
            while self._dispatch():
                pass
        except KeyboardInterrupt:
            self._killTasks()
            print('Stopping...')

