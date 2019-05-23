#!/usr/bin/env python3

import time
import os

SLEEP_SECONDS = 1

class DAG():
  def __init__(self, name):
    """Inits a DAG instance"""
    self.name = name
    self.nodes = []

  def _get_nodes_by_status(self, status):
    """Returns a list of nodes which status is <status>"""
    return [x for x in self.nodes if x.status == status]

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
    self._update_nodes_status()
    nodes_ready = self._get_nodes_by_status('ready')
    while nodes_ready or self._get_nodes_by_status('running'):
      for node in nodes_ready:
        node.run() # TODO: thread this
      time.sleep(SLEEP_SECONDS)
      self._update_nodes_status()
      nodes_ready = self._get_nodes_by_status('ready')

class Node():
  def __init__(self, name, task=None, dependencies=[]):
    """Inits a Node instance"""
    self.name = name
    self.task = task
    self.dependencies = dependencies
    self.status = 'init'

  def run(self):
    """Runs the node.task"""
    if self.status != 'ready':
      raise RuntimeError("Node '%s' is not ready" % self.name)
    else:
      self.satus = 'running'
      # TODO: run in the background
      os.system(self.task)
      # TODO: add error handler
      self.status = 'done'

  def set_ready(self):
    """Sets node status to 'ready'"""
    # TODO: add control - can only set status='ready' if status=='init'
    self.status = 'ready'

if __name__ == '__main__':
  print('Start')
  my_workflow = DAG('my_workflow')
  node0 = Node(name='node-hello', task='echo "Hello world!"')
  node1 = Node(name='node-eat', task='echo "Start eating like crazy!"', dependencies=['node-hello'])
  node2 = Node(name='node-pizza', task='echo "Eating: pizza"', dependencies=['node-eat'])
  node3 = Node(name='node-fruit', task='echo "Eating: fruit"', dependencies=['node-eat'])
  node4 = Node(name='node-candy', task='echo "Eating: candy"', dependencies=['node-eat'])
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
