#!/usr/bin/python
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.

DOCUMENTATION = '''
---
module: ecs_task
short_description: run, start or stop a task in ecs
description:
    - Creates or deletes instances of task definitions.
version_added: "2.0"
author: 
    - Mark Chance(@Java1Guy)
    - Justin Menga(@jmenga)
requirements: [ json, boto, botocore, boto3 ]
options:
    operation:
        description:
            - Which task operation to execute
        required: True
        choices: ['run', 'start', 'stop']
    cluster:
        description:
            - The name of the cluster to run the task on
        required: False
    task_definition:
        description:
            - The task definition to start or run
            - This can either be the task definition family name or ARN of the task definition
        required: False
    overrides:
        description:
            - A dictionary of values to pass to the new instances
        required: False
    count:
        description:
            - How many new instances to start
        required: False
    task:
        description:
            - The task to stop
        required: False
    container_instances:
        description:
            - The list of container instances on which to deploy the task
        required: False
    started_by:
        description:
            - A value showing who or what started the task (for informational purposes)
        required: False
    timeout:
        version_added: 2.2
        description: 
            - The time to wait for the task to complete
            - A value of 0 (default) will not wait for the task to complete
            - If the task does not complete within the timeout period, a failure will occur
        required: False
        default: 0
extends_documentation_fragment:
    - aws
    - ec2
'''

EXAMPLES = '''
# Simple example of run task
- name: Run task
  ecs_task:
    operation: run
    cluster: console-sample-app-static-cluster
    task_definition: console-sample-app-static-taskdef
    count: 1
    started_by: ansible_user
  register: task_output

# Simple example of run task and wait 300 seconds for task to complete
- name: Run task with timeout
  ecs_task:
      operation: run
      cluster: console-sample-app-static-cluster
      task_definition: console-sample-app-static-taskdef
      timeout: 300

# Simple example of run task with overrides
# For environment variables, the overrides will append or overwrite to the environment
# variables defined in the specified task defintion
- name: Run task with overrides
  ecs_task:
      operation: run
      cluster: console-sample-app-static-cluster
      task_definition: console-sample-app-static-taskdef
      overrides: 
        containerOverrides:
            - name: my-container
              command: 
                - run.sh
                - "--some-flag"
              environment:
                - name: SOME_VAR
                  value: some value            

# Simple example of start task
- name: Start a task
  ecs_task:
      operation: start
      cluster: console-sample-app-static-cluster
      task_definition: console-sample-app-static-taskdef
      task: "arn:aws:ecs:us-west-2:172139249013:task/3f8353d1-29a8-4689-bbf6-ad79937ffe8a"
      container_instances:
      - arn:aws:ecs:us-west-2:172139249013:container-instance/79c23f22-876c-438a-bddf-55c98a3538a8
      started_by: ansible_user
  register: task_output

- name: Stop a task
  ecs_task:
      operation: stop
      cluster: console-sample-app-static-cluster
      task_definition: console-sample-app-static-taskdef
      task: "arn:aws:ecs:us-west-2:172139249013:task/3f8353d1-29a8-4689-bbf6-ad79937ffe8a"
'''
RETURN = '''
task:
    description: details about the tast that was started
    returned: success
    type: complex
    contains:
        taskArn:
            description: The Amazon Resource Name (ARN) that identifies the task.
            returned: always
            type: string
        clusterArn:
            description: The Amazon Resource Name (ARN) of the of the cluster that hosts the task.
            returned: only when details is true
            type: string
        taskDefinitionArn:
            description: The Amazon Resource Name (ARN) of the task definition.
            returned: only when details is true
            type: string
        containerInstanceArn:
            description: The Amazon Resource Name (ARN) of the container running the task.
            returned: only when details is true
            type: string
        overrides:
            description: The container overrides set for this task.
            returned: only when details is true
            type: list of complex
        lastStatus:
            description: The last recorded status of the task.
            returned: only when details is true
            type: string
        desiredStatus:
            description: The desired status of the task.
            returned: only when details is true
            type: string
        containers:
            description: The container details.
            returned: only when details is true
            type: list of complex
        startedBy:
            description: The used who started the task.
            returned: only when details is true
            type: string
        stoppedReason:
            description: The reason why the task was stopped.
            returned: only when details is true
            type: string
        createdAt:
            description: The timestamp of when the task was created.
            returned: only when details is true
            type: string
        startedAt:
            description: The timestamp of when the task was started.
            returned: only when details is true
            type: string
        stoppedAt:
            description: The timestamp of when the task was stopped.
            returned: only when details is true
            type: string
'''
import time
try:
    import boto
    import botocore
    HAS_BOTO = True
except ImportError:
    HAS_BOTO = False

try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

class EcsExecManager:
    """Handles ECS Tasks"""

    def __init__(self, module):
        self.module = module

        try:
            region, ec2_url, aws_connect_kwargs = get_aws_connection_info(module, boto3=True)
            if not region:
                module.fail_json(msg="Region must be specified as a parameter, in EC2_REGION or AWS_REGION environment variables or in boto configuration file")
            self.ecs = boto3_conn(module, conn_type='client', resource='ecs', region=region, endpoint=ec2_url, **aws_connect_kwargs)
        except boto.exception.NoAuthHandlerFound, e:
            module.fail_json(msg="Can't authorize connection - "+str(e))

    def poll_tasks(self, status, cluster, timeout):
        poll_interval = 10
        poll_count = timeout / poll_interval + 1
        counter = 0
        while counter < poll_count:
            failures = status.get('failures')
            if failures:
                self.module.fail_json(msg='One or more tasks failed - ' + str(failures))
            tasks = status.get('tasks')
            tasks_complete = all(t.get('lastStatus') == 'STOPPED' for t in tasks)
            if tasks_complete:
                non_zero = [c.get('taskArn') for t in tasks for c in t.get('containers') if c.get('exitCode') != 0]
                if non_zero:
                    self.module.fail_json(msg='The following tasks failed with a non-zero exit code - ' + str(non_zero))
                else:
                    return status
            status = self.ecs.describe_tasks(cluster=cluster, tasks=[task.get('taskArn') for task in tasks])
            time.sleep(poll_interval)
            counter += 1
        self.module.fail_json(msg='Timed out waiting for tasks to complete - current status: ' + str(tasks))

    def run_task(self, cluster, task_definition, overrides, count, startedBy, timeout):
        if overrides is None:
            overrides = dict()
        response = self.ecs.run_task(
            cluster=cluster,
            taskDefinition=task_definition,
            overrides=overrides,
            count=count,
            startedBy=startedBy)
        # include tasks and failures
        if timeout > 0:
            response = self.poll_tasks(response, cluster, timeout)
        return response['tasks']

    def start_task(self, cluster, task_definition, overrides, container_instances, startedBy, timeout):
        args = dict()
        if cluster:
            args['cluster'] = cluster
        if task_definition:
            args['taskDefinition']=task_definition
        if overrides:
            args['overrides']=overrides
        if container_instances:
            args['containerInstances']=container_instances
        if startedBy:
            args['startedBy']=startedBy
        response = self.ecs.start_task(**args)
        # include tasks and failures
        if timeout > 0:
            response = self.poll_tasks(response, cluster, timeout)
        return response['tasks']

    def stop_task(self, cluster, task):
        response = self.ecs.stop_task(cluster=cluster, task=task)
        return response['task']

def main():
    argument_spec = ec2_argument_spec()
    argument_spec.update(dict(
        operation=dict(required=True, choices=['run', 'start', 'stop'] ),
        cluster=dict(required=False, type='str' ), # R S P
        task_definition=dict(required=False, type='str' ), # R* S*
        overrides=dict(required=False, type='dict'), # R S
        count=dict(required=False, type='int', default=1), # R
        task=dict(required=False, type='str' ), # P*
        container_instances=dict(required=False, type='list'), # S*
        started_by=dict(required=False, type='str', default='ansible'), # R S
        timeout=dict(required=False, type='int', default=0)
    ))

    module = AnsibleModule(argument_spec=argument_spec, supports_check_mode=True)

    # Validate Requirements
    if not HAS_BOTO:
      module.fail_json(msg='boto is required.')

    if not HAS_BOTO3:
      module.fail_json(msg='boto3 is required.')

    # Validate Inputs
    if module.params['operation'] == 'run':
        if not 'task_definition' in module.params and module.params['task_definition'] is None:
            module.fail_json(msg="To run a task, a task_definition must be specified")
        task_to_list = module.params['task_definition']
        status_type = "RUNNING"

    if module.params['operation'] == 'start':
        if not 'task_definition' in module.params and module.params['task_definition'] is None:
            module.fail_json(msg="To start a task, a task_definition must be specified")
        if not 'container_instances' in module.params and module.params['container_instances'] is None:
            module.fail_json(msg="To start a task, container instances must be specified")
        task_to_list = module.params['task']
        status_type = "RUNNING"

    if module.params['operation'] == 'stop':
        if not 'task' in module.params and module.params['task'] is None:
            module.fail_json(msg="To stop a task, a task must be specified")
        if not 'task_definition' in module.params and module.params['task_definition'] is None:
            module.fail_json(msg="To stop a task, a task definition must be specified")
        task_to_list = module.params['task_definition']
        status_type = "STOPPED"

    service_mgr = EcsExecManager(module)

    results = dict(changed=False)
    if module.params['operation'] == 'run':
        if not module.check_mode:
            results['task'] = service_mgr.run_task(
                module.params['cluster'],
                module.params['task_definition'],
                module.params['overrides'],
                module.params['count'],
                module.params['started_by'],
                module.params['timeout']
            )
        results['changed'] = True

    elif module.params['operation'] == 'start':
        if not module.check_mode:
            results['task'] = service_mgr.start_task(
                module.params['cluster'],
                module.params['task_definition'],
                module.params['overrides'],
                module.params['container_instances'],
                module.params['started_by'],
                module.params['timeout']
            )
        results['changed'] = True

    elif module.params['operation'] == 'stop':
        if not module.check_mode:
        # it exists, so we should delete it and mark changed.
        # return info about the cluster deleted
            results['task'] = service_mgr.stop_task(
                module.params['cluster'],
                module.params['task']
            )
        results['changed'] = True

    module.exit_json(**results)

# import module snippets
from ansible.module_utils.basic import *
from ansible.module_utils.ec2 import *

if __name__ == '__main__':
    main()