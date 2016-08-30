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
module: ansible_tower
short_description: Launch Ansible Tower jobs
description:
  - Launches jobs from an Ansible Tower host
  - See http://docs.ansible.com/ansible-tower/latest/html/towerapi/launch_jobtemplate.html for further details on launching Ansible Tower jobs
requirements:
  - requests >= 2.10.0
version_added: "2.2"
author: Justin Menga (@jmenga)
options:
    host:
        description:
            - The name of the Ansible Tower host including HTTP resource identifier and HTTP port (optional)
        required: true
    user:
        description:
            - Username used to authenticate to Ansible Tower
        required: true
    password:
        description:
            - Password used to authenticate to Ansible Tower
        required: true
    job_template_name:
        description:
            - The name of the job template to launch.  Either job_template_id or job_template_name must be specified.
        required: false
    job_template_id:
        description:
            - The id of the job template to launch.  Either job_template_id or job_template_name must be specified.
        required: false
    extra_vars:
        description:
            - A dictionary of extra variables to pass to Ansible Tower when launching the job.
        required: false
    tags:
        description:
            - A comma-separated list (string) of tags to run in the job
        required: false
    limit:
        description:
            - A comma-separated list (string) of hosts or groups that the job will operate on
        required: false
    inventory:
        description:
            - The id (integer) of the inventory to use when the job is run
        required: false
    credential:
        description:
            - The id (integer) of the credential to use when the job is run
        required: false
    verify_ssl:
        description:
            - Enables or disables SSL verification of the Ansible Tower connection
        required: false
        default: True
    timeout:
        description:
            - Time to wait (seconds) for the Ansible Tower to complete.  A value of 0 (default) means wait indefinitely.
        required: false
        default: 0
'''

EXAMPLES = '''
# Launch an Ansible Tower job by name
- ansible_tower:
    host: https://tower.example.com
    user: admin
    password: somepass
    job_template_name: My Job Template

# Launch an Ansible Tower job by id and disable SSL verification
- ansible_tower:
    host: https://tower.example.com
    user: admin
    password: somepass
    job_template_id: 10
    verify_ssl: false

# Launch an Ansible Tower job with extra variables
- ansible_tower:
    host: https://tower.example.com
    user: admin
    password: somepass
    job_template_name: My Job Template
    extra_vars:
        environment: dev
        some_var: some_value

# Launch an Ansible Tower job and limit hosts in staging and production
# See http://docs.ansible.com/ansible-tower/latest/html/userguide/inventories.html for further details on limit patterns
- ansible_tower:
    host: https://tower.example.com
    user: admin
    password: somepass
    job_template_name: My Job Template
    limit: key-name=staging,key-name=production

# Launch multiple Ansible Tower jobs asychronously
# Set poll to 0 to immediately exit and continue to the next task
- ansible_tower:
    host: https://tower.example.com
    user: admin
    password: somepass
    job_template_name: My Job Template 1
  register: job_1
  async: 600
  poll: 0
- ansible_tower:
    host: https://tower.example.com
    user: admin
    password: somepass
    job_template_name: My Job Template 2
  register: job_2
  async: 600
  poll: 0
- name: Check Job 1
  async_status: jid="{{ job_1.ansible_job_id }}"
  register: job_1
  until: job_1.finished
  retries: 60
  delay: 10
- name: Check Job 2
  async_status: jid="{{ job_2.ansible_job_id }}"
  register: job_2
  until: job_2.finished
  retries: 60
  delay: 10
- name: Print Job 1 result
  debug: msg={{ job_1 }}
- name: Print Job 2 result
  debug: msg={{ job_2 }}
'''

RETURN = '''
job_result:
    description: The Ansible Tower job result dictionary
    returned: always
    type: dict
'''

try:
    import requests
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    # Suppress insecure warnings if using this module asynchronously
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

from ansible.module_utils.basic import AnsibleModule
from time import sleep
import json, sys

# API paths
AUTH_PATH = "/api/v1/authtoken/"
JOB_PATH = "/api/v1/jobs/"
JOB_TEMPLATE_PATH = "/api/v1/job_templates/"
POLL_INTERVAL = 10

class AnsibleTowerManager:
    """Handles Ansible Tower Jobs"""
    def __init__(self, params):
        self.host = params['host']
        self.user = params['user']
        self.password = params['password']
        self.verify_ssl = params['verify_ssl']
        self.extra_vars = params['extra_vars']
        self.cloud_credential = params['cloud_credential']
        self.credential = params['credential']
        self.inventory = params['inventory']
        self.tags = params['tags']
        self.limit = params['limit']
        self.timeout = params['timeout']
        self.session = requests.Session()
        self.session.headers.update({'Content-Type':'application/json'})
        
        # Authenticate
        self.authenticate()
        
    def authenticate(self):
        auth_body = dict(username=self.user, password=self.password)
        auth_response = self.session.post(url=self.host + AUTH_PATH, data=json.dumps(auth_body), verify=self.verify_ssl)
        auth_response.raise_for_status()
        auth_token = json.loads(auth_response.text).get('token')
        self.session.headers.update({'Authorization':'Token ' + auth_token})

    def poll_job(self, job_id):
        poll_count = self.timeout / POLL_INTERVAL + 1
        counter = 0
        job_status = 'pending'
        job_url = self.host + JOB_PATH + str(job_id) + "/"
        while counter < poll_count and job_status in ['pending', 'waiting', 'running']:
            sleep(POLL_INTERVAL)
            job_response = self.session.get(url=job_url, verify=self.verify_ssl)
            job_response.raise_for_status()
            job_result = json.loads(job_response.text)
            job_status = job_result.get('status')
            counter += 1
        return job_result

    def launch_job(self, job_template_id, launch_data):
        launch_url = self.host + JOB_TEMPLATE_PATH + str(job_template_id) + "/launch/"
        launch_job_response = self.session.post(url=launch_url, data=json.dumps(launch_data), verify=self.verify_ssl)
        launch_job_response.raise_for_status()
        launch_job_data = json.loads(launch_job_response.text)
        job_id = launch_job_data.get('id') or launch_job_data.get('job')
        return self.poll_job(job_id)

    def get_job_template_id(self, job_template_name):
        job_template_response = self.session.get(
            url=self.host + JOB_TEMPLATE_PATH, 
            params={'name': job_template_name}, 
            verify=self.verify_ssl)
        job_template_response.raise_for_status()
        job_template_results = next((j for j in json.loads(job_template_response.text)['results']), dict())
        return job_template_results.get('id')

    def get_launch_requirements(self, job_template_id):
        launch_url = self.host + JOB_TEMPLATE_PATH + str(job_template_id) + "/launch/"
        launch_response = self.session.get(url=launch_url, verify=self.verify_ssl)
        launch_response.raise_for_status()
        return json.loads(launch_response.text)

    def check_credential_required(self, launch_requirements):
        credential_required = launch_requirements.get('credential_needed_to_start') or False
        if credential_required and not self.credential:
            raise ValueError('You must specify a credential ID')

    def check_required_vars(self, launch_requirements):
        required_vars = launch_requirements.get('variables_needed_to_start') or []
        if not set(required_vars).issubset(set(self.extra_vars)):
            raise ValueError('You must specify the following extra variables: ' + str(required_vars))

    def check_inventory_required(self, launch_requirements):
        inventory_required = launch_requirements.get('credential_needed_to_start') or False
        if inventory_required and not self.inventory:
            raise ValueError('You must specify an inventory ID')

    def check_launch_requirements(self, job_template_id):
        launch_requirements = self.get_launch_requirements(job_template_id)
        self.check_required_vars(launch_requirements)
        self.check_credential_required(launch_requirements)
        self.check_inventory_required(launch_requirements)

    def create_launch_data(self):
        launch_data = dict(extra_vars=self.extra_vars)
        if self.credential: 
            launch_data['credential'] = self.credential
        if self.inventory: 
            launch_data['inventory'] = self.inventory
        if self.tags: 
            launch_data['job_tags'] = self.tags
        if self.limit: 
            launch_data['limit'] = self.limit
        return launch_data

def main():
    argument_spec = dict(
        host=dict(required=True, type='str'),
        user=dict(required=True, type='str'),
        password=dict(required=True, type='str'),
        job_template_name=dict(required=False, type='str'),
        job_template_id=dict(required=False, type='int'),
        timeout=dict(required=False, default=sys.maxsize, type='int'),
        extra_vars=dict(required=False, default=dict(), type='dict'),
        tags=dict(required=False, type='str'),
        limit=dict(required=False, type='str'),
        inventory=dict(required=False, type='int'),
        credential=dict(required=False, type='int'),
        cloud_credential=dict(required=False, type='int'),
        verify_ssl=dict(required=False, default=True, type='bool')
    )
    module = AnsibleModule(argument_spec=argument_spec, supports_check_mode=False)
    if not HAS_REQUESTS:
      module.fail_json(msg='requests is required.')
    
    # Validate parameters
    job_template_id = module.params['job_template_id']
    job_template_name = module.params['job_template_name']
    if not job_template_id and not job_template_name:
        module.fail_json(msg='You must specify either job_template_id or job_template_name.')

    # Create Ansible Tower manager
    try:
        manager = AnsibleTowerManager(module.params)
    except requests.exceptions.HTTPError as e:
        module.fail_json(msg='Tower authentication failed: %s %s' % (e, e.response.text))
    except requests.exceptions.ConnectionError as e:
        module.fail_json(msg='Could not connect to Tower host: %s' % e)

    # Get job template launch data
    try:
        job_template_id = job_template_id or manager.get_job_template_id(job_template_name)
    except requests.exceptions.HTTPError as e:
        module.fail_json(msg='Tower returned an error response attempting to query for the job template name: %s %s' % (e, e.response.text))
    if not job_template_id:
        module.fail_json(msg='Invalid job template id - please check the provided job template name')

    # Check launch requirements and create launch_data
    try:
        manager.check_launch_requirements(job_template_id)
        launch_data = manager.create_launch_data()
    except ValueError as e:
        module.fail_json(msg='Launch requirements error: %s' % e)
    except requests.exceptions.HTTPError as e:
        module.fail_json(msg='Error obtaining launch data: %s %s' % (e, e.response.text))

    # Launch job
    try:
        job_result = manager.launch_job(job_template_id, launch_data)
    except requests.exceptions.HTTPError as e:
        module.fail_json(msg='Error launching job: %s %s' % (e, e.response.text))
    if job_result.get('status') in ['pending', 'waiting', 'running']:
        module.fail_json(msg='Timed out waiting for job to complete - current status: %s' % str(job_result))
    if job_result.get('failed'):
        module.fail_json(msg='Job failed: %s' % job_result)

    # Return result
    result = dict()
    result['changed'] = True
    result['job_result'] = job_result
    module.exit_json(**result)

if __name__ == '__main__':
    main()