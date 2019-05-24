#!/usr/bin/env python3

from queue import Queue
import subprocess
import time
from threading import Thread, Lock

SLEEP_SECONDS = .5

BLUE = "\033[1;34m" # init
YELLOW = "\033[1;33m" # ready
GREEN = "\033[0;32m" # running
RED = "\033[1;31m" # error
DEFAULT = "\033[0;37m" 

class DAG():
  def __init__(self, name, threads=1):
    """Inits a DAG instance"""
    self.name = name
    self.nodes = []
    self.threads = threads
    self._queue = Queue()
    self._lock = Lock()

  def _work_queue(self):
    while True:
      node = self._queue.get()
      try:
        node.run()
      except RuntimeError:
        pass # TODO: Fix this - take it out and troubleshoot
      finally:
        self._queue.task_done()
        time.sleep(SLEEP_SECONDS)

  def _get_nodes_by_status(self, status):
    """Returns a list of nodes which status is <status>"""
    return sorted([x for x in self.nodes if x.status == status], key=lambda x: x.name)

  def _update_nodes_status(self):
    """Update the status of the nodes"""
    for node in self.nodes:
      if node.status == 'init' and all(map(lambda x: x.status=='done', [ n for n in self.nodes if n.name in node.dependencies])):
        # if all node dependencies are done
        node.set_ready()

  def add_node(self, node):
    """Adds a Node instance to the DAG instance"""
    # TODO: Add circular reference handler
    self.nodes.append(node)

  def _log(self):
    """Logs status of nodes"""
    with self._lock:
      print('----------------------------------------------------------------------------')
      for n in sorted(self.nodes, key=lambda x: x.name):
        print("%s | %s%s%s\t | %s\t | %s" % (
          self.name,
          BLUE if n.status == 'init' else YELLOW if n.status == 'ready' else GREEN if n.status == 'running' else RED if n.status == 'error' else DEFAULT,
          n.status, DEFAULT,
          n.name,
          n.task))
      print('----------------------------------------------------------------------------')

  def run(self):
    """Runs the DAG instance"""
    # Create workers (daemon threads)
    for worker in range(self.threads):
      thread = Thread(target=self._work_queue, args=())
      thread.setDaemon(True)
      thread.start()
    # Loop adding ready nodes to the queue
    self._log()
    self._update_nodes_status()
    while self._get_nodes_by_status('ready') or self._get_nodes_by_status('running'):
      self._log()
      for node in self._get_nodes_by_status('ready'):
        self._queue.put(node)
      time.sleep(SLEEP_SECONDS)
      self._update_nodes_status()
    self._queue.join() # wait for everything to finish
    self._log()

class Node():
  def __init__(self, name, task=None, dependencies=[]):
    """Inits a Node instance"""
    self.name = name
    self.task = task
    self.dependencies = dependencies
    self.status = 'init'

  def run(self):
    """Runs the node task"""
    if self.status != 'ready':
      raise RuntimeError("Node '%s' is not ready" % self.name)
    else:
      try:
        self.status = 'running'
        subprocess.check_call([self.task], shell=True)
        self.status = 'done'
      except subprocess.CalledProcessError as e:
        self.status = 'error'
        # TODO: add error message `self.error_message = e`?

  def set_ready(self):
    """Sets node status to 'ready'"""
    # TODO: add control - can only set status='ready' if status=='init'
    self.status = 'ready'

  def add_dependency(self, dependency):
    """Adds a dependency to the node"""
    self.dependencies = list(set(self.dependencies.append(dependency)))

# TODO: Add tests
