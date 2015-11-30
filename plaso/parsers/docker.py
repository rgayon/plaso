# -*- coding: utf-8 -*-
"""Parser for the Docker conf files

"""

from datetime import datetime
import json
import os
import sys

from plaso.events import time_events
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


    try:
      json_file_path = parser_mediator.GetFileEntry().path_spec.location
    except AttributeError as exception:
      json_file_path = parser_mediator.GetFileEntry().path_spec.parent.location
    try:
      if json_file_path.find("/containers")> 0 :
        if json_file_path.find("/config.json") >0:
          self._ParseContainerConfigJSON(file_object, parser_mediator)
        elif json_file_path.find("/hostconfig.json")>0:
          pass
      elif json_file_path.find("/graph")> 0 :
        if json_file_path.find("/json") >0:
          self._ParseLayerConfigJSON(file_object, parser_mediator)
    except ValueError as exception:
      if exception=="No JSON object could be decoded":
        raise errors.UnableToParseFile((
            u'[{0:s}] Unable to parse file {1:s} as '
            u'JSON: {2:s}').format(
                self.NAME, parser_mediator.GetDisplayName(), exception))
      else:
        raise exception

  def _GetDateTimeFromString(self, s):
    d=datetime(1,1,1) # Looks like it's Docker's default timestamp
    if len(s)  >= 26:
      # Slicing to 26 because python doesn't understand nanosec timestamps
      d= datetime.strptime(s[:26],"%Y-%m-%dT%H:%M:%S.%f")

    return  timelib.Timestamp.FromPythonDatetime(d)

  def _ParseLayerConfigJSON(self,file_object,parser_mediator):
    j = json.load(file_object)
    ts=None
    path = parser_mediator.GetFileEntry().path_spec.location
    attr={"id":path.split("/")[-2]}
    if not "id" in j or ( j["id"]!=attr["id"]):
      # Not a docker layer JSON file
      return
    if "created" in j:
      ts=self._GetDateTimeFromString(j["created"])
      attr["action"]="Layer Created"
      parser_mediator.ProduceEvent(DockerJSONLayerEvent(ts,eventdata.EventTimestamp.ADDED_TIME,attr))


  def _ParseContainerConfigJSON(self,file_object,parser_mediator):
    j = json.load(file_object)
    ts=None
    path = parser_mediator.GetFileEntry().path_spec.location
    attr={"id":path.split("/")[-2]}
    if not "ID" in j or ( j["ID"]!=attr["id"]):
      # Not a docker container JSON file
      return
    if "State" in j:
      if "StartedAt" in j["State"]:
        ts=self._GetDateTimeFromString(j["State"]["StartedAt"])
        attr["action"]="Container Started"
        parser_mediator.ProduceEvent(DockerJSONContainerEvent(ts,eventdata.EventTimestamp.START_TIME,attr))
      if "FinishedAt" in j["State"]:
        ts=self._GetDateTimeFromString(j["State"]["FinishedAt"])
        attr["action"]="Container Finished"
        parser_mediator.ProduceEvent(DockerJSONContainerEvent(ts,eventdata.EventTimestamp.END_TIME,attr))
    if "Created" in j:
      ts=self._GetDateTimeFromString(j["Created"])
      attr["action"]="Container Created"
      parser_mediator.ProduceEvent(DockerJSONContainerEvent(ts,eventdata.EventTimestamp.ADDED_TIME,attr))


class DockerJSONEvent(time_events.TimestampEvent):
  """Event for stuff parsed from any Docker JSON file."""

  DATA_TYPE = u'docker:json'

  def __init__(self, timestamp, event_type,attributes):
    super(DockerJSONEvent, self).__init__(timestamp, event_type)
    self.action=attributes['action']
    self.id_=attributes['id']


class DockerJSONContainerEvent(DockerJSONEvent):
  """Event for stuff parsed from Containers' config.json files."""

  DATA_TYPE = u'docker:json:container'


class DockerJSONLayerEvent(DockerJSONEvent):
  """ Event for stuff from graph/**/json """

  DATA_TYPE = u'docker:json:layer'

manager.ParsersManager.RegisterParsers([
    DockerJSONParser,
])
