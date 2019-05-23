#!/usr/bin/env python3

import time
import os
from queue import Queue
from threading import Thread, Lock
import subprocess

SLEEP_SECONDS = .5
COLOR_GREEN = "\033[0;32m"
COLOR_LIGHT_GRAY = "\033[0;37m"

class DAG():
  def __init__(self, name, threads=1):
    """Inits a DAG instance"""
    self.name = name
    self.nodes = []
    self.threads = threads
    self.q = Queue()
    self.t_lock = Lock()

  def _work_queue(self):
    while True:
      node = self.q.get()
      with self.t_lock:
        print("[INFO] Started  : '%s'" % node.name) 
      try:
        node.run()
      except RuntimeError:
        pass # TODO: Fix this - take it out and troubleshoot
      finally:
        self.q.task_done()
        with self.t_lock:
          print("[INFO] Finished: '%s'" % node.name)
        time.sleep(SLEEP_SECONDS)

  def _get_nodes_by_status(self, status):
    """Returns a list of nodes which status is <status>"""
    return sorted([x for x in self.nodes if x.status == status], key=lambda x: x.name)

  def _update_nodes_status(self):
    """Update the status of the nodes"""
    for node in self.nodes:
      if node.status == 'init' and node.dependencies == []:
        # if node has no dependencies
        node.set_ready()
      if node.status == 'init' and all(map(lambda x: x.status=='done', [ n for n in self.nodes if n.name in node.dependencies])):
        # if all node dependencies are done
        node.set_ready()

  def add_node(self, node):
    """Adds a Node instance to the DAG instance"""
    # TODO: Add circular reference handler
    self.nodes.append(node)

  def run(self):
    """Runs the DAG instance"""

    # Create workers (daemon threads)
    for worker in range(self.threads):
      thread = Thread(target=self._work_queue, args=())
      thread.setDaemon(True)
      thread.start()
    
    # Loop an add ready nodes to the queue
    self._update_nodes_status()
    while self._get_nodes_by_status('ready') or self._get_nodes_by_status('running'):
      with self.t_lock:
        print('----------------------------------------------------------------------------')
        for n in self._get_nodes_by_status('done'):    print("[STAT] DONE    | '%s'\t | '%s'" % (n.name, n.task))
        for n in self._get_nodes_by_status('running'): print("[STAT] RUNNING | '%s'\t | '%s'" % (n.name, n.task))
        for n in self._get_nodes_by_status('ready'):   print("[STAT] READY   | '%s'\t | '%s'" % (n.name, n.task))
        for n in self._get_nodes_by_status('init'):    print("[STAT] INIT    | '%s'\t | '%s'" % (n.name, n.task))
        print('----------------------------------------------------------------------------')
      for node in self._get_nodes_by_status('ready'):
        self.q.put(node)
      time.sleep(SLEEP_SECONDS)
      self._update_nodes_status()
    self.q.join() # wait for everything to finish
    print('Ended threaded')

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
      self.status = 'running'
      #os.system(self.task) # does not wait
      # subprocess.Popen(self.task, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # does not wait
      subprocess.call(self.task, shell=True)
      # TODO: add error handler
      self.status = 'done'

  def set_ready(self):
    """Sets node status to 'ready'"""
    # TODO: add control - can only set status='ready' if status=='init'
    self.status = 'ready'

if __name__ == '__main__':
  print('Start')
  my_workflow = DAG('my_workflow', threads=2)
  node0 = Node(name='node-hello', task='echo "Hello world!"')
  node1 = Node(name='node-eat', task='echo "Start eating like crazy!"', dependencies=['node-hello'])
  node2 = Node(name='node-pizza', task='echo "Eating: pizza"', dependencies=['node-eat'])
  node3 = Node(name='node-fruit', task='echo "Eating: fruit"', dependencies=['node-eat'])
  node4 = Node(name='node-candy', task='echo "Eating: candy"', dependencies=['node-eat'])
  node2 = Node(name='node-pizza', task='echo "Eating: pizza" & sleep 3', dependencies=['node-eat'])
  node3 = Node(name='node-fruit', task='echo "Eating: fruit" & sleep 1', dependencies=['node-eat'])
  node4 = Node(name='node-candy', task='echo "Eating: candy" & sleep 2', dependencies=['node-eat'])
  node5 = Node(name='node-bed', task='echo "Going to bed"', dependencies=['node-pizza', 'node-fruit', 'node-candy'])
  node6 = Node(name='node-sleep', task='echo "Sleeping" & sleep 2', dependencies=['node-bed'])
  node7 = Node(name='node-wake', task='echo "Woke up!"', dependencies=['node-sleep'])
  my_workflow.add_node(node5)
  my_workflow.add_node(node6)
  my_workflow.add_node(node7)
  my_workflow.add_node(node0)
  my_workflow.add_node(node1)
  my_workflow.add_node(node2)
  my_workflow.add_node(node3)
  my_workflow.add_node(node4)
  my_workflow.run()
  print('End')
