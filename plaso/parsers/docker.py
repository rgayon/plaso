# -*- coding: utf-8 -*-
"""Parser for Docker configuration and log files."""

from datetime import datetime
import json
import os

from plaso.events import time_events
from plaso.events import text_events
from plaso.lib import errors
from plaso.lib import eventdata
from plaso.lib import timelib
from plaso.parsers import manager
from plaso.parsers import interface


class DockerJSONParser(interface.FileObjectParser):
  """ Will generate various events from Docker json config and log files.
      This handles :
      - Per container config file
        DOCKER_DIR/containers/<container_id>/config.json
      - Per container stdout/stderr output log
        DOCKER_DIR/containers/<container_id>/<container_id>-json.log
      - Filesystem layer config files
        DOCKER_DIR/graph/<layer_id>/json
  """

  NAME = u'dockerjson'
  DESCRIPTION = u'Parser for JSON Docker files.'

  def ParseFileObject(self, parser_mediator, file_object):

    # Trivial check if we actually have a JSON file
    #   check for initial character being open brace.
    file_object.seek(0, os.SEEK_SET)

    if file_object.read(1) != b'{':
      raise errors.UnableToParseFile((
          u'[{0:s}] {1:s} is not a valid Preference file, '
          u'missing opening brace.').format(
              self.NAME, parser_mediator.GetDisplayName()))

    file_object.seek(0, os.SEEK_SET)

    # We need the path to the file to know what it describes
    try:
      json_file_path = parser_mediator.GetFileEntry().path_spec.location
    except AttributeError as exception:
      # Compressed JSON file
      json_file_path = parser_mediator.GetFileEntry().path_spec.parent.location
    try:
      if json_file_path.find(u'/containers') > 0:
        if json_file_path.find(u'/config.json') > 0:
          self._ParseContainerConfigJSON(file_object, parser_mediator)
        if json_file_path.find(u'-json.log') > 0:
          self._ParseContainerLogJSON(file_object, parser_mediator)
      elif json_file_path.find(u'/graph') > 0:
        if json_file_path.find(u'/json') > 0:
          self._ParseLayerConfigJSON(file_object, parser_mediator)
    except ValueError as exception:
      if exception == u'No JSON object could be decoded':
        raise errors.UnableToParseFile((
            u'[{0:s}] Unable to parse file {1:s} as '
            u'JSON: {2:s}').format(
                self.NAME, parser_mediator.GetDisplayName(), exception))
      else:
        raise exception

  def _GetDateTimeFromString(self, ss):
    """Converts text timestamps from JSON files into Timestamps """
    # Docker uses Go time lib, and RFC3339 times. This isn't supposed to
    # happen in stdlib before python 3.6. See http://bugs.python.org/issue15873
    # This is a cheap hack to reuse existing timelib functions.
    s = ss.replace(u'Z', '')
    if len(s) >= 26:
      # Slicing to 26 because python doesn't understand nanosec timestamps
      d = datetime.strptime(s[:26], u'%Y-%m-%dT%H:%M:%S.%f')
    else:
      try:
        d = datetime.strptime(s, u'%Y-%m-%dT%H:%M:%S.%f')
      except ValueError:
        d = datetime.strptime(s, u'%Y-%m-%dT%H:%M:%S')
    return  timelib.Timestamp.FromPythonDatetime(d)

  def _ParseLayerConfigJSON(self, file_object, parser_mediator):
    """This handles :
      - Filesystem layer config files
        DOCKER_DIR/graph/<layer_id>/json
    """
    j = json.load(file_object)
    ts = None
    path = parser_mediator.GetFileEntry().path_spec.location
    attr = {u'layer_id':path.split(u'/')[-2]}

    # Basic checks
    if u'docker_version'  not in j:
      # Not a docker layer JSON file ?
      return
    if u'id' in j and (j[u'id'] != attr[u'layer_id']):
      return
    if u'created' in j:
      ts = self._GetDateTimeFromString(j[u'created'])
      attr[u'command'] = u' '.join(
          [x.strip() for x in j[u'container_config'][u'Cmd']]
          ).replace(u'\t', '')
      parser_mediator.ProduceEvent(
          DockerJSONLayerEvent(ts,
                               eventdata.EventTimestamp.ADDED_TIME,
                               attr)
          )


  def _ParseContainerConfigJSON(self, file_object, parser_mediator):
    """ This handles :
      - Per container config file
        DOCKER_DIR/containers/<container_id>/config.json
    """
    j = json.load(file_object)
    ts = None
    path = parser_mediator.GetFileEntry().path_spec.location
    attr = {u'container_id':path.split(u'/')[-2]}
    # Basic checks
    if u'Driver' not in j or u'ID' not in j or (
        j[u'ID'] != attr[u'container_id']):
      # Not a docker container JSON file
      return
    if u'Config' in j and u'Hostname' in j[u'Config']:
      attr[u'container_name'] = j[u'Config'][u'Hostname']
    if u'State' in j:
      if u'StartedAt' in j[u'State']:
        ts = self._GetDateTimeFromString(j[u'State'][u'StartedAt'])
        attr[u'action'] = u'Container Started'
        parser_mediator.ProduceEvent(
            DockerJSONContainerEvent(ts,
                                     eventdata.EventTimestamp.START_TIME,
                                     attr)
        )
      if u'FinishedAt' in j['State']:
        if j['State']['FinishedAt'] != u'0001-01-01T00:00:00Z':
          # I assume the container is still running
          attr['action'] = u'Container Finished'
          ts = self._GetDateTimeFromString(j['State']['FinishedAt'])
          parser_mediator.ProduceEvent(
              DockerJSONContainerEvent(ts,
                                       eventdata.EventTimestamp.END_TIME,
                                       attr)
          )
    if u'Created' in j:
      ts = self._GetDateTimeFromString(j[u'Created'])
      attr[u'action'] = u'Container Created'
      parser_mediator.ProduceEvent(
          DockerJSONContainerEvent(ts,
                                   eventdata.EventTimestamp.ADDED_TIME,
                                   attr)
      )

  def _ParseContainerLogJSON(self, file_object, parser_mediator):
    """This handles :
      - Per container stdout/stderr output log
        DOCKER_DIR/containers/<container_id>/<container_id>-json.log
    """
    ts = None
    path = parser_mediator.GetFileEntry().path_spec.location
    attr = {u'container_id':path.split(u'/')[-2]}

    for log_line in file_object.read().splitlines():
      # The format is 1 JSON formatted log message per line
      json_log_line = json.loads(log_line)
      if u'log' in json_log_line and u'time' in json_log_line:
        attr[u'log_line'] = json_log_line[u'log']
        attr[u'log_source'] = json_log_line[u'stream']
        ts = self._GetDateTimeFromString(json_log_line[u'time'])
        parser_mediator.ProduceEvent(DockerJSONContainerLogEvent(ts, 0, attr))


class DockerJSONEvent(time_events.TimestampEvent):
  """Default event for stuff parsed from any Docker JSON file."""
  # Basically not used directly, please extend.

  DATA_TYPE = u'docker:json'

  def __init__(self, timestamp, event_type):
    super(DockerJSONEvent, self).__init__(timestamp, event_type)

class DockerJSONContainerLogEvent(text_events.TextEvent):
  DATA_TYPE = u'docker:json:container:log'

class DockerJSONContainerEvent(DockerJSONEvent):
  """Event for stuff parsed from Containers' config.json files."""

  DATA_TYPE = u'docker:json:container'
  def __init__(self, timestamp, event_type, attributes):
    super(DockerJSONContainerEvent, self).__init__(timestamp,
                                                   event_type)
    self.container_id = attributes[u'container_id']
    self.container_name = attributes[u'container_name']
    self.action = attributes[u'action']


class DockerJSONLayerEvent(DockerJSONEvent):
  """ Event for stuff from graph/**/json """

  DATA_TYPE = u'docker:json:layer'
  def __init__(self, timestamp, event_type, attributes):
    super(DockerJSONLayerEvent, self).__init__(timestamp,
                                               event_type)
    self.command = attributes[u'command']
    self.layer_id = attributes[u'layer_id']

manager.ParsersManager.RegisterParsers([
    DockerJSONParser,
])
