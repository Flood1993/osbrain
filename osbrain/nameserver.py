"""
Implementation of name server.
"""
import os
import time
import random
import multiprocessing
import signal

import Pyro4
from Pyro4.naming import BroadcastServer

from .common import format_exception
from .address import address_to_host_port
from .address import SocketAddress
from .proxy import Proxy
from .proxy import NSProxy


@Pyro4.expose
class NameServer(Pyro4.naming.NameServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def ping(self):
        """
        A simple test method to check if the name server is running correctly.
        """
        return 'pong'

    def agents(self):
        """
        List agents registered in the name server.
        """
        agents = self.list()
        return [name for name in agents if name != 'Pyro.NameServer']

    def async_shutdown_agents(self):
        """
        Shutdown all agents registered in the name server.
        """
        for name, address in self.list().items():
            if name == 'Pyro.NameServer':
                continue
            agent = Pyro4.core.Proxy(address)
            if agent.get_attr('running'):
                agent.after(0, 'shutdown')
            else:
                agent.kill()

    def async_shutdown(self):
        """
        Shutdown the name server. All agents will be shutdown as well.
        """
        self.async_shutdown_agents()
        self._pyroDaemon.shutdown()


Pyro4.naming.NameServer = NameServer


class CustomNameServerDaemon(Pyro4.naming.NameServerDaemon):
    def __init__(self, *args, **kwargs):
        print('Creating a custom name server daemon\n')
        super().__init__(*args, **kwargs)
        signal.signal(signal.SIGINT, self._sigint_handler)

    def _sigint_handler(self, _signal, _frame):
        print('Custom name server daemon received SIGINT')
        signal.signal(signal.SIGINT, signal.default_int_handler)


class NameServerProcess(multiprocessing.Process):
    """
    Name server class. Instances of a name server are system processes which
    can be run independently.
    """
    def __init__(self, addr=None):
        super().__init__()
        if isinstance(addr, int):
            addr = '127.0.0.1:%s' % addr
        self.addr = addr
        self.host, self.port = address_to_host_port(addr)
        self.shutdown_event = multiprocessing.Event()
        self.uri = None
        self.queue = multiprocessing.Queue()

    def run(self):
        signal.signal(signal.SIGINT, self._sigint_handler)

        try:
            self.daemon = Pyro4.naming.NameServerDaemon(self.host, self.port)
        except Exception:
            self.queue.put(format_exception())
            return
        self.queue.put('STARTED')
        self.uri = self.daemon.uriFor(self.daemon.nameserver)
        self.host = self.uri.host
        self.port = self.uri.port
        self.addr = SocketAddress(self.host, self.port)
        internal_uri = self.daemon.uriFor(self.daemon.nameserver, nat=False)
        bcserver = None
        hostip = self.daemon.sock.getsockname()[0]
        # Start broadcast responder
        bcserver = BroadcastServer(internal_uri)
        print("Broadcast server running on %s" % bcserver.locationStr)
        bcserver.runInThread()
        print("NS running on %s (%s)" % (self.daemon.locationStr, hostip))
        print("URI = %s" % self.uri)
        try:
            self.daemon.requestLoop(lambda: not self.shutdown_event.is_set())
        finally:
            self.daemon.close()
            if bcserver is not None:
                bcserver.close()
        print("NS shut down.")

    def start(self):
        os.environ['OSBRAIN_NAMESERVER_ADDRESS'] = str(self.addr)
        super().start()
        status = self.queue.get()
        if status == 'STARTED':
            return
        raise RuntimeError('An error occured while creating the daemon!' +
                           '\n===============\n'.join(['', status, '']))

    def agents(self):
        """
        List agents registered in the name server.
        """
        print('agents 1\n')
        proxy = NSProxy(self.addr)
        print('agents 2\n')
        agents = proxy.list()
        print('agents 3\n')
        proxy.release()
        print('agents 4\n')
        return [name for name in agents if name != 'Pyro.NameServer']

    def shutdown_all(self):
        """
        Shutdown all agents registered in the name server.
        """
        print('> NS shutdown_all called\n')
        for agent in self.agents():
            print('Hello World 1\n')
            proxy = Proxy(agent, self.addr)
            print('Hello World 2\n')
            proxy.unsafe.after(0, 'shutdown')
            print('Hello World 3\n')
            proxy.release()
        print('> NS shutdown_all exit\n')

    def shutdown(self):
        """
        Shutdown the name server. All agents will be shutdown as well.
        """
        print('> NS shutdown called\n')
        self.shutdown_all()
        print('> NS creating proxy\n')
        nameserver = NSProxy(self.addr)
        # Wait for all agents to be shutdown (unregistered)
        while len(nameserver.list()) > 1:
            print('> NS shutdown looping\n')
            time.sleep(0.1)
        self.shutdown_event.set()
        self.terminate()
        self.join()
        print('> NS shutdown exit\n')

    def _sigint_handler(self, _signal, _frame):
        print('> SIGINT NS enter\n')
        signal.signal(signal.SIGINT, signal.default_int_handler)
        self.shutdown()
        print('> SIGINT NS exit\n')


def random_nameserver_process(host='127.0.0.1', port_start=10000,
                              port_stop=20000, timeout=3.):
    """
    Start a random NameServerProcess.

    Parameters
    ----------
    host : str, default is '127.0.0.1'
        Host address where the name server will bind to.
    port_start : int
        Lowest port number allowed.
    port_stop : int
        Highest port number allowed.

    Returns
    -------
    NameServerProcess
        The name server process started.
    """
    t0 = time.time()
    exception = TimeoutError('Name server starting timed out!')
    while True:
        try:
            # Bind to random port
            port = random.randrange(port_start, port_stop + 1)
            addr = SocketAddress(host, port)
            nameserver = NameServerProcess(addr)
            nameserver.start()
            return nameserver
        except RuntimeError as error:
            exception = error
        if time.time() - t0 > timeout:
            raise exception


def run_nameserver(addr=None):
    """
    Ease the name server creation process.

    This function will create a new nameserver, start the process and then run
    its main loop through a proxy.

    Parameters
    ----------
    addr : SocketAddress, default is None
        Name server address.

    Returns
    -------
    proxy
        A proxy to the name server.
    """
    if not addr:
        addr = random_nameserver_process().addr
    else:
        NameServerProcess(addr).start()
    return NSProxy(addr)
