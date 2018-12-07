#!/usr/bin/python

from os import mkdir
from os.path import isfile, isdir
from sys import stdout, stderr, exit, argv
from distexec import Dispatcher, LineMonitor
from subprocess import check_call, call

from argparse import ArgumentParser

def parse_cmd():
    parser = ArgumentParser()
    parser.add_argument('--xmls', default=[], help='Run on these xml files')
    parser.add_argument('--tasks', default=[], help='Run these tasks (for each xml file)')
    return parser.parse_args()

COLORS = {
    'DARK_BLUE': '\033[0;34m',
    'CYAN': '\033[0;36m',
    'BLUE': '\033[1;34m',
    'PURPLE': '\033[0;35m',
    'RED': '\033[1;31m',
    'GREEN': '\033[1;32m',
    'BROWN': '\033[0;33m',
    'YELLOW': '\033[1;33m',
    'WHITE': '\033[1;37m',
    'GRAY': '\033[0;37m',
    'DARK_GRAY': '\033[1;30m',
    'RESET': '\033[0m'
}


def _print_stream(msg, stream, prefix=None, print_nl=True, color=None):
    """
    Print message to stderr/stdout

    @ msg      : str    message to print
    @ prefix   : str    prefix for the message
    @ print_nl : bool  print new line after the message
    @ color    : str    color to use when printing, default None
    """

    # don't print color when the output is redirected
    # to a file
    if not stream.isatty():
        color = None

    if not color is None:
        stream.write(COLORS[color])

    if msg == '':
        return
    if not prefix is None:
        stream.write(prefix)

    stream.write(msg)

    if not color is None:
        stream.write(COLORS['RESET'])

    if print_nl:
        stream.write('\n')

    stream.flush()

def print_stderr(msg, prefix=None, print_nl=True, color=None):
    _print_stream(msg, stderr, prefix, print_nl, color)


def print_stdout(msg, prefix=None, print_nl=True, color=None):
    _print_stream(msg, stdout, prefix, print_nl, color)



class SyncMonitor(LineMonitor):
    def __init__(self, machine, noout = False):
        LineMonitor.__init__(self)
        self._machine = machine
        self._noout = noout

    def callback(self, fd, line, isstderr):
        if isstderr:
            print("[{0} stderr]: {1}".format(self._machine, line))
        elif not self._noout:
            print("[{0}]: {1}".format(self._machine, line))

    def input(self):
        print('{0}> Wants input:\n'.format(self._machine))
        return stdin.readline()

user = "xchalup4"

machines = ["ben01", "ben02", "ben03", "ben04", "ben06",
            "ben08", "ben09", "ben12", "ben14"]

machines = ["ben02", "ben03", "ben04", "ben05", "ben06", "ben07",
            "ben08", "ben12", "ben13", "ben14"]

machines = ["ben01", "ben02", "ben03", "ben04", "ben05", "ben06",
            "ben07", "ben09", "ben12", "ben13"]

archive="run.zip"
script="run.sh"

folder="/var/data/xchalup4/experiments/"

def runall(cmd, noout = False):
    global machines

    d = Dispatcher()
    for machine in machines:
        sshcmd = ['ssh', "{0}@{1}".format(user, machine)] + cmd
        print("{0}> {1}".format(machine, " ".join(sshcmd)))
        d.run(sshcmd, SyncMonitor(machine, noout))
    d.monitor()

def copyall(fl):
    global machines

    d = Dispatcher()
    for machine in machines:
        scpcmd = ['scp', fl, "{0}@{1}:{2}".format(user, machine, folder)]
        print("{0}> {1}".format(machine, " ".join(scpcmd)))
        d.run(scpcmd, SyncMonitor(machine))
    d.monitor()

# prepare environment
try:
    # create the results dir if it does not exists
    mkdir('results')
except OSError as e:
    print('Directory results already exists')

assert isdir('results')

runall(['killall', '-9', 'klee', 'symbiotic', 'benchexec'])
runall(['mkdir', '-p', folder])
copyall(archive)
runall(['cd', folder, ';', 'unzip', '-o', archive, '&&',
        'chmod', '+x', script], True)

free_machines = set()

class Run(LineMonitor):
    def __init__(self, machine, xml, tasks):
        LineMonitor.__init__(self)
        self._machine = machine
        self._xml = xml
        self._tasks = tasks
        # we monitor two fds (stdout, stderr),
        # here we count how many we already closed
        self._closed_fds = 0

        filename="{0}-{1}-{2}.log".format(machine, xml, tasks).replace('/','-')
        while isfile(filename):
            filename += '-next.log'
        self._logfile = open(filename, "w+")

    def __del__(self):
        self._logfile.close()
        del self

    def callback(self, fd, line, isstderr):
        if isstderr:
            print_stderr("[{0} stderr]: {1}".format(self._machine, line))
        #else:
        #    print("[{0}]: {1}".format(self._machine, line))

        # log all lines
        self._logfile.write(line)
        self._logfile.write('\n')
        self._logfile.flush()

    def input(self):
        print('{0}> Wants input:\n'.format(self._machine))
        return stdin.readline()

class RunDispatcherMonitor: #(DispatcherMonitor):
    def __init__(self, tasks):
        self._dispatcher = Dispatcher()
        self._tasks = tasks
        self._free_machines = set()
        for machine in machines:
            self._free_machines.add(machine)

    def done(self, fd, proc, data, exitstatus):
        # we register stdout and stder, so do not take action until we get
        # done for both of them
        data._closed_fds += 1
        if data._closed_fds < 2:
            return

        if exitstatus == 0:
            print_stdout("Task {0}-{1} on {2} done".format(data._xml, data._tasks,
                                                       data._machine),
                         color="BROWN")

            cmd = ['scp', "{0}@{1}:{2}/results/*".format(user, data._machine,
                   folder), 'results/']
            print_stdout("Fetching the results from {0}".format(data._machine),
                         color="GREEN")
            print("{0}> {1}".format(data._machine, " ".join(cmd)))
            call(cmd)

        else:
            print_stderr("[{0}]:".format(data._machine),
                         "Running task failed (exitstatus {0})".format(exitstatus),
                         color='RED')
            print("Re-adding the task to be executed")
            self._tasks.add((data._xml, data._tasks))

        self._free_machines.add(data._machine)

        if len(self._tasks) > 0:
            self.runTask()

    def runTask(self):
        assert len(self._free_machines) > 0
        machine = self._free_machines.pop()
        task = self._tasks.pop()

        print_stdout("Starting task {0}-{1} on {2}".format(task[0], task[1],
                                                           machine),
                     color="BROWN")

        cmd = ['ssh', "{0}@{1}".format(user, machine),
               'cd', '{0}/;'.format(folder),
               './{0}'.format(script), task[0], '-t', task[1]]
        print("{0}> {1}".format(machine, " ".join(cmd)))
        self._dispatcher.run(cmd, Run(machine, task[0], task[1]))

    def run(self):
        while len(self._free_machines) > 0 and\
              len(self._tasks) > 0:
            self.runTask()

        self._dispatcher.monitor(self)

args = parse_cmd()


##  -- defaults

xmls = [ 'symbiotic.xml', 'symbiotic-noslice.xml', 'symbiotic-sttt-instr.xml']
#xmls = [ 'symbiotic-memsafety.xml' ]

memsafety_categories = ['MemSafety-Other',
                        'MemSafety-TerminCrafted',
                        'MemSafety-LinkedLists',
                        'MemSafety-MemCleanup',
                        'MemSafety-Heap',
                        'MemSafety-Arrays']

categories = ['ReachSafety-Arrays',
              'ReachSafety-BitVectors',
              'ReachSafety-ControlFlow',
              'ReachSafety-Floats',
              'ReachSafety-Heap',
              'ReachSafety-Loops',
              'ReachSafety-ProductLines',
              'ReachSafety-Recursive',
              'ReachSafety-Sequentialized',
              'MemSafety-Other',
              'MemSafety-TerminCrafted',
              'MemSafety-LinkedLists',
              'MemSafety-Heap',
              'MemSafety-Arrays',
              'MemSafety-MemCleanup',
              'Systems_BusyBox_MemSafety',
              'Systems_BusyBox_NoOverflows',
              'Systems_DeviceDriversLinux64_ReachSafety',
              'NoOverflows-BitVectors',
              'NoOverflows-Other']

#categories = memsafety_categories

##  -- cmd line
if args.xmls:
    xmls = args.xmls.split(',')
if args.tasks:
    categories = args.tasks.split(',')

tasks = set()
#tasks.add(('memsafety/rc-rt-no-slicing.xml', 'MemSafety-TerminCrafted'))
#tasks.add(('memsafety/rc-rt-no-slicing.xml', 'MemSafety-Other'))
for x in xmls:
    for c in categories:
        tasks.add((x,c))

runner = RunDispatcherMonitor(tasks)
runner.run()

runall(['killall', '-9', 'klee', 'symbiotic', 'benchexec'])
#runall(['killall', '-9', 'Ultimate.py', 'benchexec'])
