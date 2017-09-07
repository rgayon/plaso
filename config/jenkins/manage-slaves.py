from __future__ import unicode_literals

import argparse
import base64
import copy
import datetime
import json
import sys
import time

from Crypto.Cipher import PKCS1_OAEP
from Crypto.PublicKey import RSA
from Crypto.Util.number import long_to_bytes

try:
  from googleapiclient import discovery
except ImportError:
  from apiclient import discovery

import googleapiclient.errors
from oauth2client import client as oauth2client


class SlaveManager(object):
  """Class for managing Jenkins Slaves."""

  DEFAULT_DISKSIZE_GB = 50
  DEFAULT_MACHINETYPE = 'n1-standard-8'
  DEFAULT_NETWORK = 'default'
  DEFAULT_SCOPES = ['https://www.googleapis.com/auth/devstorage.read_write']
  DEFAULT_ZONE = 'europe-west1-d'

  def __init__(self,  project, zone=None, debug=False):
    """Create a new SlaveManager."""

    self._debug = debug

    self._project = project
    self._zone = zone or self.DEFAULT_ZONE

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
          project = self._project, zone=self._zone, operation=operation['name']
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

  def GetInstance(self, instance_name):
    """Returns a dict containing a Compute Engine instance information."""
    operation = self._client.instances().get(
        instance=instance_name, project=self._project, zone=self._zone
    ).execute()
    return operation

  def _CreateInstance(self, instance_name, image=None, machinetype=None,
                      metadata=None, network=None, scopes=None):
    """Creates a GCE instance.

    Args:
        instance_name (str): the name to give to the slave.
        image (str): the disk image to use. Can be either the name of an image
          in the current project, or a complete image path (starting with
          '/projects/<project_name>/zones...')
        machinetype (str): the type of the machine to use (default:
          n1-standard-8).
        metadata (dict): optional metadata to set for the instance.
        network (str): type of network to use (default: 'default')
        scopes (list[str]): the list of scopes to set for the instance.
    """

    disk_size = self.DEFAULT_DISKSIZE_GB
    machinetype = machinetype or self.DEFAULT_MACHINETYPE
    network = network or self.DEFAULT_NETWORK
    scopes = scopes or self.DEFAULT_SCOPES
    source_image = image

    project_url = 'compute/v1/projects/{0:s}'.format(self._project)
    zone_url = '%s/zones/%s' % (project_url, self._zone)
    if not source_image.startswith('/projects/'):
      source_image = '/projects/{0:s}/global/images/{1:s}'.format(
          self._project, source_image)
    machine_type_url = '{0:s}/zones/{1:s}/machineTypes/{2:s}'.format(
          project_url, self._zone, machinetype)
    network_url = '{0:s}/global/networks/{1:s}'.format(project_url, network)

    instance_dict = {
        'name': instance_name,
        'machineType': machine_type_url,
        'disks': [{
          'index': 0,
          'boot': True,
          'mode': 'READ_WRITE',
          'autoDelete': True,
          'initializeParams': {
            'diskName': instance_name,
            'diskSizeGb': disk_size,
            'sourceImage': source_image,
          },
        }],
        'networkInterfaces': [{
          'accessConfigs': [{
            'type': 'ONE_TO_ONE_NAT',
            'name': 'External NAT'
           }],
          'network': network_url,
        }],
        'serviceAccounts': [{
             'email': 'default',
             'scopes': scopes,
        }],
    }
    if metadata:
      instance_dict['metadata'] = metadata

    operation = self._client.instances().insert(
        project=self._project, body=instance_dict, zone=self._zone).execute()
    response = self._WaitForOperation(operation)

    self._Log(
        'Waiting for instance to have finished booting and running'
        'startup scripts...')
    finished = False
    while not finished:
      install_log = self._GetSerialPortOutput(instance_name, 1)
      last_line = None
      for line in install_log.split('\n'):
        index = line.find(': ')
        if index>0:
          last_line = line[index+2:]
          if line.find('MetadataScripts: Finished running startup scripts')>0:
            finished = True
          if line.find('Startup finished in ') > 0:
            # Linux
            finished = True
      if last_line:
        self._Debug('...still booting (last line: {0:s})'.format(last_line))
      else:
        self._Debug('...still booting)'.format(last_line))
      time.sleep(30)

  def UpdateInstanceMetadata(self, instance_name, new_metadata):
    """Update the instance metadata."""
    operation = self._client.instances().setMetadata(
        instance=instance_name, project=self._project, zone=self._zone,
        body=new_metadata)
    return operation.execute()

  def SpinUpNewSlave(self, instance_name, image=None, machinetype=None,
                     metadata=None, network=None, scopes=None):
    """Creates a new Jenkins Slave instance and configures it accordingly.

    Args:
      instance_name (str): the name to give to the slave.
      image (str): the disk image to use. Can be either the name of an image
        in the current project, or a complete image path (starting with
        '/projects/<project_name>/zones...')
      machinetype (str): the type of the machine to use (default:
        n1-standard-8).
      metadata (dict): optional metadata to set for the instance.
      network (str): type of network to use (default: 'default')
      scopes (list[str]): the list of scopes to set for the instance.

    Returns:
      dict: a dict containing some information about the new slave.
    """
    self._Log('Creating new instance {0:s}'.format(instance_name))
    try:
      self._CreateInstance(
        instance_name, image=image, machinetype=machinetype, metadata=metadata,
        network=network, scopes=scopes)
      time.sleep(2*60)  # The startup script might reboot the machine so we wait a bit
    except googleapiclient.errors.HttpError as e:
      print "raised"
      pass


    instance = self.GetInstance(instance_name)
    instance_ref = instance['networkInterfaces'][0]
    instance_info = {
        'name': instance_name,
        'external_ip': instance_ref['accessConfigs'][0]['natIP'] ,
        'internal_ip': instance_ref['networkIP'],
    }

    self._Log('Configuring new instance {0:s}'.format(instance_name))
    results = self._ConfigureInstance(instance_name)
    if results:
      instance_info.update(results)
    return instance_info


class LinuxSlaveManager(SlaveManager):
  """Class for managing Linux Slaves."""

  DEFAULT_USER = 'root'
  DEFAULT_DISKSIZE_GB = '200'


class WindowsSlaveManager(SlaveManager):
  """Class for managing Windows Slaves."""

  DEFAULT_USER = "plaso_test"

  def _DecryptPassword(self, encrypted_password, key):
    """Decrypt a base64 encoded encrypted password using the provided key.

    Args:
      encrypted_password(str): the encrypted password read from the console.
      key(RSA): the key used for the session.

    Returns:
      str: the clear text password"""
    decoded_password = base64.b64decode(encrypted_password)
    cipher = PKCS1_OAEP.new(key)
    password = cipher.decrypt(decoded_password)
    return password

  def _GetModulusExponentInBase64(self, key):
    """Return the public modulus and exponent for the key in bas64 encoding."""
    mod = long_to_bytes(key.n)
    exp = long_to_bytes(key.e)

    modulus = base64.b64encode(mod)
    exponent = base64.b64encode(exp)

    return modulus, exponent

  def _GetJsonString(self, user, modulus, exponent, email=''):
    """Return the JSON string object that represents the windows-keys entry."""

    utc_now = datetime.datetime.utcnow()
    # These metadata entries are one-time-use, so the expiration time does
    # not need to be very far in the future. In fact, one minute would
    # generally be sufficient. Five minutes allows for minor variations
    # between the time on the client and the time on the server.
    expire_time = utc_now + datetime.timedelta(minutes=5)
    expire_time_string = expire_time.strftime('%Y-%m-%dT%H:%M:%SZ')

    data = {'userName': user,
            'modulus': modulus,
            'exponent': exponent,
            'email': email,
            'expireOn': expire_time_string}
    return json.dumps(data)

  def _UpdateWindowsKeys(self, old_metadata, metadata_entry):
    """Return updated metadata contents with the new windows-keys entry."""
    # Simply overwrites the "windows-keys" metadata entry. Production code may
    # want to append new lines to the metadata value and remove any expired
    # entries.
    new_metadata = copy.deepcopy(old_metadata)
    if 'items' in new_metadata:
      for item in new_metadata['items']:
          if item['key'] == 'windows-keys':
              item['value'] = metadata_entry
    else:
      new_metadata['items']= [{'key':'windows-keys', 'value': metadata_entry }]
    return new_metadata

  def _GetEncryptedPasswordFromSerialPort(self, serial_port_output, modulus):
    """Find and return the correct encrypted password, based on the modulus."""
    # In production code, this may need to be run multiple times if the output
    # does not yet contain the correct entry.
    output = serial_port_output.split('\n')
    for line in reversed(output):
      try:
        entry = json.loads(line)
        if modulus == entry['modulus']:
          return entry['encryptedPassword']
      except ValueError:
        pass

  def ChangePassword(self, instance_name, user=None):
    """Changes/set the password for a user."""
    user = user or self.DEFAULT_USER
    self._Debug('Changing password for user {0:s}'.format(user))
    key = RSA.generate(2048)
    modulus, exponent = self._GetModulusExponentInBase64(key)

    # Get existing metadata
    instance_ref = self.GetInstance(instance_name)
    old_metadata = instance_ref['metadata']

    # Create and set new metadata
    metadata_entry = self._GetJsonString(user, modulus, exponent)
    new_metadata = self._UpdateWindowsKeys(old_metadata, metadata_entry)
    result = self.UpdateInstanceMetadata(instance_name, new_metadata)

    enc_password = None
    self._Debug('Trying to get password from console....')
    while not enc_password:
      time.sleep(30)
      # Encrypted passwords are printed to COM4 on the windows server:
      serial_port_output = self._GetSerialPortOutput(instance_name, port=4)
      enc_password = self._GetEncryptedPasswordFromSerialPort(serial_port_output, modulus)
      if not enc_password:
        self._Debug('... failed! sleeping (expecting modulus {0:s})'.format(modulus[0:12]))

    password = self._DecryptPassword(enc_password, key)
    return (user, password)

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('action', choices=['create'])
  parser.add_argument('--bootstrap_script', required=False)
  parser.add_argument('--bootstrap_script_prepend', required=False)
  parser.add_argument('--bootstrap_url', required=False)
  parser.add_argument('--debug', required=False, action='store_true')
  parser.add_argument('--instance_name', required=False)
  parser.add_argument('--image', required=True)
  parser.add_argument('--machine_type', required=False)
  parser.add_argument('--network', required=False)
  parser.add_argument('--os', choices=['linux', 'windows'], required=True)
  parser.add_argument('--project', required=True)
  parser.add_argument('--user', required=False)
  parser.add_argument('--zone', required=False)

  flags = parser.parse_args(sys.argv[1:])

  instance_metadata = None

  if flags.os == 'windows':
    manager = WindowsSlaveManager(project=flags.project, zone=flags.zone, debug=flags.debug)
  else:
    manager = LinuxSlaveManager(project=flags.project, zone=flags.zone, debug=flags.debug)

  if flags.action == 'create':
    if not flags.instance_name:
      parser.error('Creating a new Slave requires --instance_name to be specified')

    script_prepend = ''
    if flags.bootstrap_script_prepend:
      with open(flags.bootstrap_script_prepend, 'r') as script_prepend_fd:
        script_prepend = script_prepend_fd.read()

    if flags.bootstrap_script:
      with open(flags.bootstrap_script, 'r') as script_fd:
        script = script_fd.read()
        if flags.os == 'windows':
          instance_metadata = {
              'items': [
                  {'key': 'windows-startup-script-ps1',
                   'value': '{0:s}{1:s}'.format(script_prepend,script)}
              ]
          }
        else:
          items = []
          if flags.bootstrap_script:
            items.append({'key': 'startup-script-url', 'value': flags.bootstrap_script})

          if flags.ssh_pubkey:
            with open(flags.ssh_pubkey, 'r') as ssh_pubkey_fd:
              items.append({'key': 'ssh-keys', 'value': ssh_pubkey_fd.read().strip()})

          instance_metadata = {'items': items}

    instance_info = manager.SpinUpNewSlave(
        flags.instance_name, image=flags.image, machinetype=flags.machine_type,
        metadata=instance_metadata, network=flags.network)

    print instance_info

    #TODO ;with internal IP, reset ssh_known hosts
