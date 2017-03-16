import signal
import os
import time

from osbrain import run_nameserver
from osbrain import run_agent

# signal.signal(signal.SIGCHLD, signal.SIG_IGN)
signal.signal(signal.SIGINT, signal.SIG_IGN)

ns = run_nameserver()
#a0 = run_agent('a0', ns.addr())

#while len(ns.agents()) != 1:
#    time.sleep(0.1)

#print('Agent successfully registered\n')

print('Wait 1\n')
os.wait()
# print('Wait 2\n')
# os.wait()
# print('Finished main\n')

print('Main script finished OK\n')
