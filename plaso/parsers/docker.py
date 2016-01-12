# -*- coding: utf-8 -*-
"""Parser for the Docker conf files

"""

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
  """ An empty docstring """

  NAME = u'dockerjson'
  DESCRIPTION = u'Parser for JSON Docker files.'

  def ParseFileObject(self, parser_mediator, file_object):

    # Should we check for the path before the content is actually JSON?
    # First pass check for initial character being open brace.
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
      # This happens on compressed JSON files
      json_file_path = parser_mediator.GetFileEntry().path_spec.parent.location
    try:
      if json_file_path.find("/containers") > 0:
        if json_file_path.find("/config.json") > 0:
          self._ParseContainerConfigJSON(file_object, parser_mediator)
        if json_file_path.find("-json.log") > 0:
          self._ParseContainerLogJSON(file_object, parser_mediator)
      elif json_file_path.find("/graph") > 0:
        if json_file_path.find("/json") > 0:
          self._ParseLayerConfigJSON(file_object, parser_mediator)
    except ValueError as exception:
      if exception == "No JSON object could be decoded":
        raise errors.UnableToParseFile((
            u'[{0:s}] Unable to parse file {1:s} as '
            u'JSON: {2:s}').format(
                self.NAME, parser_mediator.GetDisplayName(), exception))
      else:
        raise exception

  def _GetDateTimeFromString(self, ss):
    s = ss.replace("Z", "")
    if len(s) >= 26:
      # Slicing to 26 because python doesn't understand nanosec timestamps
      d = datetime.strptime(s[:26], "%Y-%m-%dT%H:%M:%S.%f")
    else:
      try:
        d = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f")
      except ValueError:
        d = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
    return  timelib.Timestamp.FromPythonDatetime(d)

  def _ParseLayerConfigJSON(self, file_object, parser_mediator):
    j = json.load(file_object)
    ts = None
    path = parser_mediator.GetFileEntry().path_spec.location
    attr = {"layer_id":path.split("/")[-2]}
    if "id" not in j or (j["id"] != attr["layer_id"]):
      # Not a docker layer JSON file ?
      return
    if "created" in j:
      ts = self._GetDateTimeFromString(j["created"])
      attr["command"] = " ".join(
          [x.strip() for x in j["container_config"]["Cmd"]]
          ).replace('\t', '')
      parser_mediator.ProduceEvent(
          DockerJSONLayerEvent(ts,
                               eventdata.EventTimestamp.ADDED_TIME,
                               attr)
          )


  def _ParseContainerConfigJSON(self, file_object, parser_mediator):
    j = json.load(file_object)
    ts = None
    path = parser_mediator.GetFileEntry().path_spec.location
    attr = {"container_id":path.split("/")[-2]}
    if "Config" in j and "Hostname" in j["Config"]:
      attr["container_name"] = j["Config"]["Hostname"]
    if "ID" not in j or (j["ID"] != attr["container_id"]):
      # Not a docker container JSON file
      return
    if "State" in j:
      if "StartedAt" in j["State"]:
        ts = self._GetDateTimeFromString(j["State"]["StartedAt"])
        attr["action"] = "Container Started"
        parser_mediator.ProduceEvent(
            DockerJSONContainerEvent(ts,
                                     eventdata.EventTimestamp.START_TIME,
                                     attr)
        )
      if "FinishedAt" in j["State"]:
        if j["State"]["FinishedAt"] != "0001-01-01T00:00:00Z":
          # I assume the container is still running
          attr["action"] = "Container Finished"
          ts = self._GetDateTimeFromString(j["State"]["FinishedAt"])
          parser_mediator.ProduceEvent(
              DockerJSONContainerEvent(ts,
                                       eventdata.EventTimestamp.END_TIME,
                                       attr)
          )
    if "Created" in j:
      ts = self._GetDateTimeFromString(j["Created"])
      attr["action"] = "Container Created"
      parser_mediator.ProduceEvent(
          DockerJSONContainerEvent(ts,
                                   eventdata.EventTimestamp.ADDED_TIME,
                                   attr)
      )

  def _ParseContainerLogJSON(self, file_object, parser_mediator):
    ts = None
    path = parser_mediator.GetFileEntry().path_spec.location
    attr = {"container_id":path.split("/")[-2]}

    for log_line in file_object.read().splitlines():
      json_log_line = json.loads(log_line)
      if "log" in json_log_line and "time" in json_log_line:
        attr["log_line"] = json_log_line["log"]
        attr["log_source"] = json_log_line["stream"]
        ts = self._GetDateTimeFromString(json_log_line["time"])
        parser_mediator.ProduceEvent(DockerJSONContainerLogEvent(ts, 0, attr))


class DockerJSONEvent(time_events.TimestampEvent):
  """Event for stuff parsed from any Docker JSON file."""

  DATA_TYPE = u'docker:json'

  def __init__(self, timestamp, event_type, attributes):
    super(DockerJSONEvent, self).__init__(timestamp, event_type)
    self.attributes = attributes

class DockerJSONContainerLogEvent(text_events.TextEvent):
  DATA_TYPE = u'docker:json:container:log'



class DockerJSONContainerEvent(DockerJSONEvent):
  """Event for stuff parsed from Containers' config.json files."""

  DATA_TYPE = u'docker:json:container'
  def __init__(self, timestamp, event_type, attributes):
    super(DockerJSONContainerEvent, self).__init__(timestamp,
                                                   event_type,
                                                   attributes)
    self.container_id = attributes['container_id']
    self.container_name = attributes['container_name']
    self.action = attributes['action']


class DockerJSONLayerEvent(DockerJSONEvent):
  """ Event for stuff from graph/**/json """

  DATA_TYPE = u'docker:json:layer'
  def __init__(self, timestamp, event_type, attributes):
    super(DockerJSONLayerEvent, self).__init__(timestamp,
                                               event_type,
                                               attributes)
    self.command = attributes['command']
    self.layer_id = attributes['layer_id']

manager.ParsersManager.RegisterParsers([
    DockerJSONParser,
])
