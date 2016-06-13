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
module: rds_snapshot
short_description: Create, Copy and Share AWS RDS Snapshots
description:
  - Creates, copies and shares AWS RDS snapshots
dependencies:
  - boto3>=1.0.0
version_added: "2.2"
author: Justin Menga (@jmenga)
options:
    db_instance_id:
      description:
        - The RDS DB Instance Identifier of the source database.  
        - Must be specified if source_snapshot_id is not specified.  
        - Ignored if source_snapshot_id is specified.
      required: false
    snapshot_prefix:
      description:
        - Prefix to append to created RDS snapshots.
        - For snapshots created manually or from automated snapshots, defaults to manual-<db_instance_id>
        - For snapshots created from shared or public snapshots, defaults to a blank string
      required: false
      default: manual-<db_instance_id>
    snapshot_name:
      description:
        - Full name to assign to created snapshot
        - snapshot_prefix does not apply if snapshot_name is defined
        - For snapshots created manually or from automated snapshots, snapshot name defaults to snapshot_prefix + timestamp of snapshot created time
        - For snapshots created from shared or public snapshots, snapshot name defaults to snapshot name of the source snapshot
      required: false
    source_snapshot_id:
      description:
        - The name or ARN of the source RDS snapshot.  Use this if you want to only copy or share a specific RDS snapshot.
        - This setting causes the db_instance_id setting to be ignored
        - If the source snapshot is an automated, shared or public snapshot, a manual snapshot will first be created.
      required: false
    snapshot_type:
      description:
        - Only relevent when db_instance_id is specified, determines the snapshot creation behaviour.
        - manual: A new snapshot will be created from the specified RDS instance.
        - automated: A new manual snapshot will be copied from the latest available automated snapshot for the specified RDS instance.
        - shared: A new manual snapshot will be copied from the latest available shared snapshot for the specified RDS instance.
        - public: A new manual snapshot will be copied from the latest available public snapshot for the specified RDS instance.
      required: false
      default: manual
    local_destinations:
      description:
        - A list of account IDs to share the snapshot with in the same AWS region.
      required: false
    remote_destinations:
      description:
        - A list of remote regions to copy the snapshot to.  For each region, an optional list of account IDs can be included to share the copied snapshot with.  
    tags:
      description:
        - A dictionary of tags to apply to generated snapshots.
      required: false
    copy_tags:
      description:
        - Controls if existing tags should be copied to generate snapshots.
      required: false
      default: False
    kms_key_id:
      description:
        - AWS Key Management Service (KMS) Key Identifier for encrypted DB snapshots
        - If you copy a snapshot and specify a key identifier, the new snapshot is encrypted using the key
        - If you copy an encrypted snapshot shared from another AWS account, you must specify a value for the key identifier
      required: false
      default: False

extends_documentation_fragment:
    - aws
    - ec2
'''

EXAMPLES = '''
# Note: These examples do not set authentication details, see the AWS Guide for details.

# Create a manual RDS snapshot for the specified RDS instance named my-test-snapshot
- rds_snapshot:
    db_instance_id: pbcnim3c08eueg
    snapshot_name: my-test-snapshot

# Create a manual snapshot named manual-<db-instance-id>-<snapshot-created-timestamp> from latest automated snapshot
- rds_snapshot:
    db_instance_id: pbcnim3c08eueg
    snapshot_type: automated

# Create a manual snapshot named my-app-<snapshot-created-timestamp> from latest shared snapshot
- rds_snapshot:
    db_instance_id: pbcnim3c08eueg
    snapshot_type: shared
    snapshot_prefix: my-app

# Create a manual snapshot from the latest automated snapshot for instance pbcnim3c08eueg and share with account 12345678901
- rds_snapshot:
    db_instance_id: pbcnim3c08eueg
    snapshot_type: automated
    local_destinations:
      - 12345678901

# Creates a manual snapshot from source snapshot id
# Copy to us-west-2 region and share with account 12345678902 in that region
- rds_snapshot:
    source_snapshot_id: arn:aws:rds:ap-southeast-2:12345678901:snapshot:pbcnim3c08eueg-2016-06-06-01-01
    remote_destinations:
      - region: us-west-2
        accounts:
          - 12345678902
'''

RETURN = '''
source_snapshot:
    description: Details about the source snapshot
    returned: always
    type: dict
snapshot:
    description: Details about the manual snapshot that was created
    returned: always - if no manual snapshot was created, the snapshot dictionary will be the same as the source_snapshot dictionary
    type: dict
snapshot_arn:
    description: Amazon Resource Name (ARN) of the snapshot
    returned: always
    type: dict
local_destinations:
    description: A list of the accounts the snapshot was shared with in the local region
    returned: only if local_destinations is specified
    type: list of accounts
remote_destinations:
    description: A list of regions and accounts the snapshot was copied to
    returned: only if remote_destinations is specified
    type: dict
'''

try:
    import botocore, boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

from functools import partial
import json, time, re

POLL_INTERVAL = 10

class RdsServiceManager:
    """Handles RDS Service"""

    def __init__(self, module, region=None):
        self.module = module

        try:
            default_region, ec2_url, aws_connect_kwargs = get_aws_connection_info(module, boto3=True)
            region = region or default_region
            self.client = boto3.client('rds', region_name=region, **aws_connect_kwargs)
            self.account = boto3.client('sts').get_caller_identity().get('Account')
            self.region = region
        except botocore.exceptions.NoRegionError:
            self.module.fail_json(msg="Region must be specified as a parameter, in AWS_DEFAULT_REGION environment variable or in boto configuration file")
        except boto.exception.NoAuthHandlerFound as e:
            self.module.fail_json(msg="Can't authorize connection - "+str(e))

    def describe_snapshot(self, snapshot_id=None, db_instance_id=None, **kwargs):
      if snapshot_id:
        try:
          return self.client.describe_db_snapshots(DBSnapshotIdentifier=snapshot_id, **kwargs).get('DBSnapshots')[0]
        except botocore.exceptions.ClientError as e:
          if e.response['Error']['Code'] == 'DBSnapshotNotFound':
            return None
          else:
            self.module.fail_json(msg="Error describing snapshot " + snapshot_id + " - " + str(e))
      if db_instance_id:
        response = sorted(
          self.client.describe_db_snapshots(DBInstanceIdentifier=db_instance_id, **kwargs).get('DBSnapshots'),
          key=lambda snapshot: snapshot['SnapshotCreateTime'],
          reverse=True
        )
        if response:
          return response[0]
        else:
          return None
      self.module.fail_json(msg="Invalid parameters supplied - either snapshot_id or db_instance_id must be specified")

    def describe_snapshots(self, db_instance_id=None, **kwargs):
      response = self.client.describe_db_snapshots(**kwargs).get('DBSnapshots')
      if db_instance_id and response:
        response = sorted(
          [s for s in response if s.get('DBInstanceIdentifier') == db_instance_id],
          key=lambda snapshot: snapshot['SnapshotCreateTime'],
          reverse=True
        )
      if response:
        return response[0]
      else:
        return None

    def poll_snapshot_status(self, snapshot):
      changed = False
      while snapshot.get('Status') != 'available':
        time.sleep(POLL_INTERVAL)
        snapshot = self.describe_snapshot(snapshot_id=snapshot['DBSnapshotIdentifier'])
        if not snapshot:
          self.module.fail_json(msg="The snapshot could no longer be found - it may have been deleted recently.")
        changed = True
      return snapshot, changed

    def create_snapshot(self, db_snapshot_id, db_instance_id, tags=list()):
      try:
        return self.client.create_db_snapshot(
          DBSnapshotIdentifier=db_snapshot_id,
          DBInstanceIdentifier=db_instance_id,
          Tags=tags
        ).get('DBSnapshot')
      except Exception as e:
        self.module.fail_json(msg="An error occurred when attempting to create a snapshot - " + str(e))
      
    def copy_snapshot(self, source_db_snapshot_id, target_db_snapshot_id, kms_key_id='', tags=list(), copy_tags=False):
      try:
        return self.client.copy_db_snapshot(
          SourceDBSnapshotIdentifier=source_db_snapshot_id,
          TargetDBSnapshotIdentifier=target_db_snapshot_id,
          KmsKeyId=kms_key_id,
          Tags=tags,
          CopyTags=copy_tags
        ).get('DBSnapshot')
      except Exception as e:
        self.module.fail_json(msg="An error occurred when attempting to create a snapshot - " + str(e))

    def copy_or_existing(self, source_snapshot, snapshot_name, kms_key_id='', tags=list(), copy_tags=False):
      snapshot = self.describe_snapshot(db_instance_id=source_snapshot['DBInstanceIdentifier'], snapshot_id=snapshot_name)
      if snapshot:
        return snapshot
      else:
        return self.copy_snapshot(
          source_db_snapshot_id=source_snapshot['DBSnapshotIdentifier'],
          target_db_snapshot_id=snapshot_name,
          kms_key_id=kms_key_id,
          tags=tags,
          copy_tags=copy_tags
        )

    def share_snapshot(self, db_snapshot_id, accounts):
      response = self.client.modify_db_snapshot_attribute(
        DBSnapshotIdentifier=db_snapshot_id,
        AttributeName='restore',
        ValuesToAdd=accounts
      )
      attributes = response.get('DBSnapshotAttributesResult').get('DBSnapshotAttributes')
      return next((a.get('AttributeValues') for a in attributes if a.get('AttributeName') == 'restore'),[])

    def share_or_existing(self, snapshot, accounts):
      shared_accounts = self.get_restore_attributes(db_snapshot_id=snapshot['DBSnapshotIdentifier'])
      if set(shared_accounts) & set(accounts) != set(accounts):
        self.share_snapshot(db_snapshot_id=snapshot['DBSnapshotIdentifier'],accounts=accounts)
        return True
      else:
        return False

    def get_restore_attributes(self, db_snapshot_id):
      response = self.client.describe_db_snapshot_attributes(DBSnapshotIdentifier=db_snapshot_id)
      attributes = response.get('DBSnapshotAttributesResult').get('DBSnapshotAttributes')
      return next((a.get('AttributeValues') for a in attributes if a.get('AttributeName') == 'restore'),[])

def get_snapshot_name(snapshot=dict(), snapshot_prefix='', create_time=None):
  if not snapshot:
    create_time = create_time or time
    return snapshot_prefix + create_time.strftime('-%Y-%m-%d-%H-%M')
  if snapshot['SnapshotType'] in ['shared','public']:
    return snapshot_prefix + re.sub(r'.*:snapshot:(.*)','\\1',snapshot['DBSnapshotIdentifier'])
  else:
    snapshot_prefix = snapshot_prefix or 'manual-' + snapshot['DBInstanceIdentifier']  
    create_time = create_time or snapshot['SnapshotCreateTime']
    return snapshot_prefix + create_time.strftime('-%Y-%m-%d-%H-%M')

def get_snapshot_arn(snapshot_id, region, account):
  return 'arn:aws:rds:' + region + ':' + account + ':snapshot:' + snapshot_id

def main():
    argument_spec = ec2_argument_spec()
    argument_spec.update(dict(
        db_instance_id=dict(required=False, type='str'),
        snapshot_prefix=dict(required=False, type='str'),
        snapshot_name=dict(required=False, type='str'),
        source_snapshot_id=dict(required=False, type='str'),
        snapshot_type=dict(required=False, default=False, type='str'),
        local_destinations=dict(required=False, default=[], type='list'),
        remote_destinations=dict(required=False, default=[], type='list'),
        timeout=dict(required=False, default=900, type='int'),
        tags=dict(required=False, default={}, type='dict'),
        copy_tags=dict(required=False,default=False, type='bool'),
        kms_key_id=dict(required=False, default='', type='str')
    ))
    module = AnsibleModule(argument_spec=argument_spec, supports_check_mode=False)
    if not HAS_BOTO3:
      module.fail_json(msg='boto3 is required.')
    
    # Get parameters
    service_mgr = RdsServiceManager(module)
    db_instance_id = module.params.get('db_instance_id')
    source_snapshot_id = module.params.get('source_snapshot_id')
    snapshot_prefix = module.params.get('snapshot_prefix') or ''
    snapshot_name = module.params.get('snapshot_name')
    snapshot_type = module.params.get('snapshot_type')
    local_destinations = [str(l) for l in module.params.get('local_destinations')]
    remote_destinations = [
      {'region': r.get('region'), 'accounts': [str(a) for a in r.get('accounts',{})]} 
      for r in module.params.get('remote_destinations') 
    ]
    tags = [
      { 'Key': key, 'Value': value }
      for key,value in module.params.get('tags',{}).items()
    ]
    copy_tags = module.params.get('copy_tags') or False
    kms_key_id = module.params.get('kms_key_id')

    # Create result dictionary
    result = dict()
    result['changed'] = False

    # We use either a source snapshot or db instance id
    if source_snapshot_id:
      snapshot = service_mgr.describe_snapshot(
        snapshot_id=source_snapshot_id,
        IncludeShared=True,
        IncludePublic=True
      )
      if not snapshot:
          module.fail_json(msg='Could not find snapshot for ' + source_snapshot_id)
      result['source_snapshot'] = snapshot
      if snapshot['SnapshotType'] in ['automated','shared','public']:
        # Snapshot type is not manual, so we need to create a manual copy
        snapshot_name = snapshot_name or get_snapshot_name(snapshot=snapshot, snapshot_prefix=snapshot_prefix)
        snapshot = service_mgr.copy_or_existing(
          snapshot, 
          snapshot_name, 
          kms_key_id=kms_key_id, 
          tags=tags, 
          copy_tags=copy_tags
        )
        snapshot, result['changed'] = service_mgr.poll_snapshot_status(snapshot)
    elif db_instance_id:
      if snapshot_type in ['shared','public','automated']:
        # Find latest snapshot
        snapshot = service_mgr.describe_snapshot(
          db_instance_id=db_instance_id, 
          SnapshotType=snapshot_type,
          IncludeShared=(snapshot_type == 'shared'),
          IncludePublic=(snapshot_type == 'public')
          )
        result['source_snapshot'] = snapshot
        if not snapshot:
          module.fail_json(msg='Could not find latest ' + snapshot_type + ' snapshot for ' + db_instance_id)
        snapshot_name = snapshot_name or get_snapshot_name(snapshot=snapshot, snapshot_prefix=snapshot_prefix)
        snapshot = service_mgr.copy_or_existing(
          snapshot, 
          snapshot_name, 
          kms_key_id=kms_key_id, 
          tags=tags, 
          copy_tags=copy_tags
        )
        snapshot, result['changed'] = service_mgr.poll_snapshot_status(snapshot)
      else:
        # Create a point-in-time snapshot from DB instance
        snapshot_prefix = snapshot_prefix or 'manual-' + db_instance_id
        snapshot_name = snapshot_name or get_snapshot_name(snapshot_prefix=snapshot_prefix, create_time=time)
        snapshot = service_mgr.describe_snapshot(snapshot_id=snapshot_name)
        if not snapshot:
          snapshot = service_mgr.create_snapshot(
            db_snapshot_id=snapshot_name, 
            db_instance_id=db_instance_id, 
            tags=tags
          )
          snapshot, result['changed'] = service_mgr.poll_snapshot_status(snapshot)
          result['source_snapshot'] = {}
    else:
      module.fail_json(msg='You must specify either db_instance_id or source_snapshot_id')

    # Share with local destinations
    result['local_destinations'] = []
    if local_destinations:
      result['changed'] = service_mgr.share_or_existing(snapshot,local_destinations)
      result['local_destinations'] = local_destinations

    # Copy and share with remote destinations
    # Each copy is initiated prior to polling to allow 
    # multiple snapshot copy operations to run concurrently
    local_region = service_mgr.region
    local_account = service_mgr.account
    result['remote_destinations'] = []

    for remote_destination in remote_destinations:
      region = remote_destination.get('region')
      accounts = remote_destination.get('accounts') or []
      if region != local_region:
        # Copy snapshot to remote region
        service_mgr = RdsServiceManager(module,region)
        remote_snapshot = service_mgr.describe_snapshot(db_instance_id=snapshot['DBInstanceIdentifier'], snapshot_id=snapshot['DBSnapshotIdentifier'])
        if not remote_snapshot:
          snapshot_arn = get_snapshot_arn(snapshot['DBSnapshotIdentifier'],local_region, service_mgr.account)
          remote_snapshot = service_mgr.copy_snapshot(
            source_db_snapshot_id=snapshot_arn,
            target_db_snapshot_id=snapshot['DBSnapshotIdentifier'],
            kms_key_id=kms_key_id, 
            tags=tags,
            copy_tags=copy_tags
          )
          result['changed'] = True
        result['remote_destinations'].append({ 
          'region': region, 
          'snapshot': remote_snapshot, 
          'snapshot_arn': get_snapshot_arn(remote_snapshot['DBSnapshotIdentifier'],region,local_account),
          'accounts': accounts 
        })

    # Poll and share operations
    for destination in result['remote_destinations']:
      remote_snapshot = service_mgr.poll_snapshot_status(destination['snapshot'])
      # Share to accounts in remote region
      accounts = destination.get('accounts')
      if accounts:
        result['changed'] = result['changed'] or service_mgr.share_or_existing(remote_snapshot,accounts)

    result['snapshot'] = snapshot
    result['snapshot_arn'] = get_snapshot_arn(snapshot['DBSnapshotIdentifier'],local_region,local_account)
    module.exit_json(**result)

# import module snippets
from ansible.module_utils.basic import *
from ansible.module_utils.ec2 import *

if __name__ == '__main__':
    main()