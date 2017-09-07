"""Script to create Jenkins Slaves."""

from __future__ import unicode_literals

import argparse
import datetime
import sys
import time

try:
  from googleapiclient import discovery
except ImportError:
  from apiclient import discovery

import googleapiclient.errors


class SlaveManager(object):
  """Class for managing Jenkins Slaves."""

  DEFAULT_SCOPES = ['https://www.googleapis.com/auth/devstorage.read_write']

  def __init__(self, project, zone=None, debug=False):
    """Create a new SlaveManager."""

    self._debug = debug
    self._project = project
    self._zone = zone

    self._client = self._CreateComputeClient()

  def _Debug(self, msg):
    """Prints a message if debug is set."""
    if self._debug:
      self._Log(msg)

  def _Log(self, msg):
    """Prints a message with timestamp."""
    print '[{0:%c}] {1:s}'.format(datetime.datetime.now(), msg)

  def _CreateComputeClient(self):
    """Creates an API client to do compute operations with."""
    return discovery.build('compute', 'v1')

  def _WaitForOperation(self, operation):
    """Waits for an API operation to complete."""
    while True:
      result = self._client.zoneOperations().get(
          project=self._project, zone=self._zone, operation=operation['name']
      ).execute()
      if result['status'] == 'DONE':
        if 'error' in result:
          raise Exception(result['error'])
        return result
      time.sleep(1)

  def _GetSerialPortOutput(self, instance_name, port):
    """Get the output from a serial port of the instance."""
    operation = self._client.instances().getSerialPortOutput(
        instance=instance_name, project=self._project, zone=self._zone,
        port=port)
    output = operation.execute()
    return output['contents']

  def _MakeAttachPD(self, persistent_disks):
    """Builds a list of dicts describing all disks to attach.

    Args:
      persistent_disks(list(str)): the attached disks passed as argument.
    Returns:
      list(dict): the list of disks to attach.
    """
    disk_list = list()
    mode = 'READ_ONLY'
    if persistent_disks:
      for pd in persistent_disks:
        source_disk = pd
        device_name = pd
        if pd.find(':') > 0:
          device_name, source_disk = pd.split(':', 1)
        source_url = (
            'https://www.googleapis.com/compute/v1/projects/{0:s}/zones/{1:s}/'
            'disks/{2:s}').format(self._project, self._zone, source_disk)
        disk_list.append(
            {
                'deviceName': device_name,
                'source': source_url,
                'mode': mode
            }
        )
    return disk_list

  def CreateInstance(
      self, instance_name, disk_size=None, source_image=None, machinetype=None,
      metadata=None, network=None, persistent_disks=None, scopes=None):
    """Creates a GCE instance.

    Args:
        instance_name (str): the name to give to the slave.
        disk_size (int): the size of the system disk, in GB. Must be larger than
          the image size.
        source_image (str): the path to the disk image to use.
          Must be in the form: '/projects/<project_name>/zones/images/...')
        machinetype (str): the type of the machine to use (default:
          n1-standard-8).
        metadata (dict): optional metadata to set for the instance.
        network (str): type of network to use (default: 'default')
        persistent_disks (list(str)): list of strings describing the disks to
          attach to the instance (Read-Only)
        scopes (list[str]): the list of scopes to set for the instance.
    """

    scopes = scopes or self.DEFAULT_SCOPES

    self._Log('Creating new instance {0:s}'.format(instance_name))

    project_url = 'compute/v1/projects/{0:s}'.format(self._project)
    machine_type_url = '{0:s}/zones/{1:s}/machineTypes/{2:s}'.format(
        project_url, self._zone, machinetype)
    network_url = '{0:s}/global/networks/{1:s}'.format(project_url, network)

    disks = [
        {
            'index': 0,
            'boot': True,
            'mode': 'READ_WRITE',
            'autoDelete': True,
            'initializeParams': {
                'diskName': '{0:s}-bootdisk'.format(instance_name),
                'diskSizeGb': disk_size,
                'sourceImage': source_image,
            }
        }
    ]

    persistent_disks = self._MakeAttachPD(persistent_disks)
    for persistent_disk in persistent_disks:
      disks.append(persistent_disk)

    instance_dict = {
        'name': instance_name,
        'machineType': machine_type_url,
        'disks': disks,
        'networkInterfaces': [{
            'accessConfigs': [{
                'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}],
            'network': network_url, }],
        'serviceAccounts': [{
            'email': 'default',
            'scopes': scopes,
        }],
    }
    if metadata:
      instance_dict['metadata'] = metadata

    self._Debug(instance_dict)

    operation = self._client.instances().insert(
        project=self._project, body=instance_dict, zone=self._zone).execute()
    self._WaitForOperation(operation)

  def UpdateInstanceMetadata(self, instance_name, new_metadata):
    """Update the instance metadata."""
    operation = self._client.instances().setMetadata(
        instance=instance_name, project=self._project, zone=self._zone,
        body=new_metadata)
    return operation.execute()


if __name__ == '__main__':
  parser = argparse.ArgumentParser(
      formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument(
      '--attach_persistent_disk', action='append', required=False,
      help='Select Persistent Disks to attach. ie: evidences-images')
  parser.add_argument(
      '--debug', required=False, action='store_true',
      help='Print debug information')
  parser.add_argument(
      '--disk_size', action='store', required=False, default=200, type=int,
      help='Boot disk size, in GB')
  parser.add_argument(
      '--instance_name', action='store', required=True, help='Name of instance')
  parser.add_argument(
      '--source_image', action='store', required=True,
      help='Path to the image, ie: /projects/<project_name>/zones/images/...')
  parser.add_argument(
      '--linux_startup_script_url', action='store', required=False,
      help='GCS url to a startup script for a Linux slave')
  parser.add_argument(
      '--machine_type', action='store', required=False, default='n1-standard-8',
      help='Type of machine')
  parser.add_argument(
      '--network', action='store', required=False, default='default',
      help='Type of network to use')
  parser.add_argument(
      '--project', action='store', required=True, help='Name of the project')
  parser.add_argument(
      '--ssh_pub_key', action='append', required=False,
      help=('Specify SSH public keys to use. '
            'Example: \'root:ssh-rsa AAAA... root\''))
  parser.add_argument(
      '--windows_startup_script_url', action='store', required=False,
      help='GCS url to a startup script for a Windows slave')
  parser.add_argument(
      '--zone', action='store', required=True, help='The zone for the instance')

  flags = parser.parse_args(sys.argv[1:])

  instance_metadata = None

  manager = SlaveManager(
      project=flags.project, zone=flags.zone, debug=flags.debug)

  instance_metadata = {'items': []}

  if flags.windows_startup_script_url:
    startup_item = {
        'key': 'windows-startup-script-url',
        'value': flags.windows_startup_script_url
    }
    instance_metadata['items'].append(startup_item)

  if flags.linux_startup_script_url:
    startup_item = {
        'key': 'startup-script-url',
        'value': flags.linux_startup_script_url
    }
    instance_metadata['items'].append(startup_item)

  if flags.ssh_pub_key:
    ssh_key_item = {
        'key': 'ssh-keys',
        'value': '\n'.join(flags.linux_startup_script_url)
    }
    instance_metadata['items'].append(ssh_key_item)

  try:
    manager.CreateInstance(
        flags.instance_name, persistent_disks=flags.attach_persistent_disk,
        source_image=flags.source_image, machinetype=flags.machine_type,
        metadata=instance_metadata, network=flags.network)
  except googleapiclient.errors.HttpError as error:
    if error.resp['status'] == '409':
      error_message = (
          'There is already an instance names {0:s} in project {1:s}'.format(
              flags.instance_name, flags.project)
      )
      print error_message
