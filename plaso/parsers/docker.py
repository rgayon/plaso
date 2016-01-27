# -*- coding: utf-8 -*-
"""Parser for Docker configuration and log files."""

import json
import os

from dfvfs.helpers import text_file

from plaso.events import time_events
from plaso.events import text_events
from plaso.lib import errors
from plaso.lib import eventdata
from plaso.lib import timelib
from plaso.parsers import manager
from plaso.parsers import interface


class DockerJSONParser(interface.FileObjectParser):
  """Generates various events from Docker json config and log files.

  This handles :
  * Per container config file
    DOCKER_DIR/containers/<container_id>/config.json
  * Per container stdout/stderr output log
    DOCKER_DIR/containers/<container_id>/<container_id>-json.log
  * Filesystem layer config files
    DOCKER_DIR/graph/<layer_id>/json
  """

  NAME = u'dockerjson'
  DESCRIPTION = u'Parser for JSON Docker files.'

  def ParseFileObject(self, parser_mediator, file_object):
    """Parses various Docker configuration and log files in JSON format.

    This methods checks whether the file_object points to a docker JSON config
    or log file, and calls the corresponding _Parse* function to generate
    Events.

    Args:
      parser_mediator: a parser mediator object (instance of ParserMediator).
      file_object: a file entry object (instance of dfvfs.FileIO).

    Raises:
      UnableToParseFile: when the file cannot be parsed.
    """
    # Trivial JSON format check: first character must be an open brace.
    if file_object.read(1) != b'{':
      raise errors.UnableToParseFile(
          u'is not a valid JSON file, missing opening brace.')

    file_object.seek(0, os.SEEK_SET)

    file_entry = parser_mediator.GetFileEntry()

    file_system = file_entry.GetFileSystem()

    json_file_path = getattr(file_entry.path_spec, u'location', None)
    if not json_file_path:
      raise errors.UnableToParseFile(
          u'Unable to get location attribute from '
          u'file_entry.path_spec.')
    splited_path = file_system.SplitPath(json_file_path)
    try:
      if u'containers' in splited_path:
        if u'config.json' in splited_path:
          self._ParseContainerConfigJSON(parser_mediator, file_object)
        if json_file_path.endswith(u'-json.log'):
          self._ParseContainerLogJSON(parser_mediator, file_object)
      elif u'graph' in splited_path:
        if u'json' in splited_path > 0:
          self._ParseLayerConfigJSON(parser_mediator, file_object)
    except ValueError as exception:
      if exception == u'No JSON object could be decoded':
        raise errors.UnableToParseFile(exception)
      else:
        raise

  def _ParseLayerConfigJSON(self, parser_mediator, file_object):
    """Extracts events from a Docker filesystem layer configuration file.

    The path of each filesystem layer config file is:
    DOCKER_DIR/graph/<layer_id>/json

    Args:
      parser_mediator: a parser mediator object (instance of ParserMediator).
      file_object: a file entry object (instance of dfvfs.FileIO).

    Raises:
      UnableToParseFile: when the file is not a valid layer config file.
    """
    json_dict = json.load(file_object)
    file_entry = parser_mediator.GetFileEntry()
    path = file_entry.path_spec.location
    file_system = file_entry.GetFileSystem()
    event_attributes = {u'layer_id':file_system.SplitPath(path)[-2]}

    if u'docker_version'  not in json_dict:
      raise errors.UnableToParseFile(
          u'not a valid Docker layer configuration file, missing '
          u'\'docker_version\' key.')

    if u'id' in json_dict and (
        json_dict[u'id'] != event_attributes[u'layer_id']):
      raise errors.UnableToParseFile(
          u'not a valid Docker layer configuration file, missing \'id\' key '
          u'or \'id\' key != {2:s} (layer ID taken from the path to the '
          u'JSON file.)')

    if u'created' in json_dict:
      timestamp = timelib.Timestamp.FromRFC3339(json_dict[u'created'])
      event_attributes[u'command'] = u' '.join(
          [x.strip() for x in json_dict[u'container_config'][u'Cmd']]
          ).replace(u'\t', '')
      parser_mediator.ProduceEvent(
          DockerJSONLayerEvent(
              timestamp, eventdata.EventTimestamp.ADDED_TIME, event_attributes)
          )

  def _ParseContainerConfigJSON(self, parser_mediator, file_object):
    """Extracts events from a Docker container configuration file.

    The path of each container config file is:
    DOCKER_DIR/containers/<container_id>/config.json

    Args:
      parser_mediator: a parser mediator object (instance of ParserMediator).
      file_object: a file entry object (instance of dfvfs.FileIO).

    Raises:
      UnableToParseFile: when the file is not a valid container config file.
    """
    json_dict = json.load(file_object)
    file_entry = parser_mediator.GetFileEntry()
    path = file_entry.path_spec.location
    file_system = file_entry.GetFileSystem()
    container_id = file_system.SplitPath(path)[-2]
    event_attributes = {u'container_id':container_id}

    if u'Driver' not in json_dict:
      raise errors.UnableToParseFile(
          u'not a valid Docker container configuration file, ' u'missing '
          u'\'Driver\' key.')

    if u'ID' not in json_dict or (json_dict[u'ID'] != container_id):
      raise errors.UnableToParseFile((
          u'not a valid Docker container configuration file, missing \'ID\' '
          u'key or \'ID\' key != {0:s} (container ID taken from the path to '
          u'the JSON file.)').format(container_id))

    if u'Config' in json_dict and u'Hostname' in json_dict[u'Config']:
      event_attributes[u'container_name'] = json_dict[u'Config'][u'Hostname']
    else:
      event_attributes[u'container_name'] = u'Unknown container name'

    if u'State' in json_dict:
      if u'StartedAt' in json_dict[u'State']:
        timestamp = timelib.Timestamp.FromRFC3339(
            json_dict[u'State'][u'StartedAt'])
        event_attributes[u'action'] = u'Container Started'
        parser_mediator.ProduceEvent(
            DockerJSONContainerEvent(
                timestamp,
                eventdata.EventTimestamp.START_TIME, event_attributes))
      if u'FinishedAt' in json_dict['State']:
        if json_dict['State']['FinishedAt'] != u'0001-01-01T00:00:00Z':
          # If the timestamp is 0001-01-01T00:00:00Z, the container
          # is still running, so we don't generate a Finished event
          event_attributes['action'] = u'Container Finished'
          timestamp = timelib.Timestamp.FromRFC3339(
              json_dict['State']['FinishedAt'])
          parser_mediator.ProduceEvent(
              DockerJSONContainerEvent(
                  timestamp,
                  eventdata.EventTimestamp.END_TIME,
                  event_attributes))

    created_time = json_dict.get(u'Created', None)
    if created_time:
      timestamp = timelib.Timestamp.FromRFC3339(created_time)
      event_attributes[u'action'] = u'Container Created'
      parser_mediator.ProduceEvent(
          DockerJSONContainerEvent(
              timestamp, eventdata.EventTimestamp.ADDED_TIME, event_attributes)
      )

  def _ParseContainerLogJSON(self, parser_mediator, file_object):
    """Extract events from a Docker container log files.

    The format is one JSON formatted log message per line.

    The path of each container log file (which logs the container stdout and
    stderr) is:
    DOCKER_DIR/containers/<container_id>/<container_id>-json.log

    Args:
      parser_mediator: a parser mediator object (instance of ParserMediator).
      file_object: a file entry object (instance of dfvfs.FileIO).
    """
    file_entry = parser_mediator.GetFileEntry()
    path = file_entry.path_spec.location
    file_system = file_entry.GetFileSystem()
    event_attributes = {u'container_id':file_system.SplitPath(path)[-2]}

    text_file_object = text_file.TextFile(file_object)

    for log_line in text_file_object:
      json_log_line = json.loads(log_line)
      if u'log' in json_log_line and u'time' in json_log_line:
        event_attributes[u'log_line'] = json_log_line[u'log']
        event_attributes[u'log_source'] = json_log_line[u'stream']
        timestamp = timelib.Timestamp.FromRFC3339(json_log_line[u'time'])
        parser_mediator.ProduceEvent(
            DockerJSONContainerLogEvent(timestamp, 0, event_attributes))


class DockerJSONBaseEvent(time_events.TimestampEvent):
  """Class for common Docker event information."""

  DATA_TYPE = u'docker:json'


class DockerJSONContainerLogEvent(text_events.TextEvent):
  """Event parsed from a Docker container's log files."""

  DATA_TYPE = u'docker:json:container:log'


class DockerJSONContainerEvent(DockerJSONBaseEvent):
  """Event parsed from a Docker container's configuration file.

  Attributes:
    action: whether the container was created, started, or finished.
    container_id: the identifier of the current container (sha256).
    container_name: the name of the current container.
  """

  DATA_TYPE = u'docker:json:container'

  def __init__(self, timestamp, event_type, attributes):
    super(DockerJSONContainerEvent, self).__init__(timestamp,
                                                   event_type)
    self.container_id = attributes[u'container_id']
    self.container_name = attributes[u'container_name']
    self.action = attributes[u'action']


class DockerJSONLayerEvent(DockerJSONBaseEvent):
  """Event parsed from a Docker filesystem layer configuration file

  Attributes:
    command: the command used which made Docker create a new layer
    layer_id: the identifier of the current Docker layer (sha1)
  """

  DATA_TYPE = u'docker:json:layer'

  def __init__(self, timestamp, event_type, attributes):
    super(DockerJSONLayerEvent, self).__init__(timestamp,
                                               event_type)
    self.command = attributes[u'command']
    self.layer_id = attributes[u'layer_id']


manager.ParsersManager.RegisterParser(DockerJSONParser)
