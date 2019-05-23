#!/usr/bin/env python3

from workflow import DAG, Node

# Create workflow
my_workflow = DAG('my_workflow', threads=2)

# Create tasks
task0 = Node(name='node-hello', task='echo "Hello world!"')
task1 = Node(name='node-eat', task='echo "Start eating like crazy!"', dependencies=['node-hello'])
task2 = Node(name='node-pizza', task='echo "Eating: pizza" & sleep 3', dependencies=['node-eat'])
task3 = Node(name='node-fruit', task='echo "Eating: fruit" & sleep 1', dependencies=['node-eat'])
task4 = Node(name='node-candy', task='echo "Eating: candy" & sleep 2', dependencies=['node-eat'])
task5 = Node(name='node-bed', task='echo "Going to bed"', dependencies=['node-pizza', 'node-fruit', 'node-candy'])
task6 = Node(name='node-sleep', task='echo "Sleeping" & sleep 2', dependencies=['node-bed'])
task7 = Node(name='node-wake', task='echo "Woke up!"', dependencies=['node-sleep'])

# Add tasks in no particular order
my_workflow.add_node(task5)
my_workflow.add_node(task6)
my_workflow.add_node(task7)
my_workflow.add_node(task0)
my_workflow.add_node(task1)
my_workflow.add_node(task2)
my_workflow.add_node(task3)
my_workflow.add_node(task4)

# Run workflow
my_workflow.run()
