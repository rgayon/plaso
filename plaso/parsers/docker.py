# -*- coding: utf-8 -*-
"""Parser for the Docker conf files

"""

from datetime import datetime
import json
import os
import sys

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

    # First pass check for initial character being open brace.
    file_object.seek(0, os.SEEK_SET)

    if file_object.read(1) != b'{':
      raise errors.UnableToParseFile((
          u'[{0:s}] {1:s} is not a valid Preference file, '
          u'missing opening brace.').format(
              self.NAME, parser_mediator.GetDisplayName()))

    file_object.seek(0, os.SEEK_SET)

    json_file_path = file_object.GetPathSpec().location
    try:
      if json_file_path.find("/containers")> 0 :
        if json_file_path.find("/config.json") >0:
          self._ParseContainerConfigJSON(file_object, parser_mediator)
        elif json_file_path.find("/hostconfig.json")>0:
          pass
      elif json_file_path.find("/graph")> 0 :
        pass
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

  def _ParseContainerConfigJSON(self,file_object,parser_mediator):
    j = json.load(file_object)
    ts=None
    path = file_object.GetPathSpec().location
    attr={"containerid":path.split("/")[-2]}
    if "State" in j:
      if "StartedAt" in j["State"]:
        ts=self._GetDateTimeFromString(j["State"]["StartedAt"])
        attr["action"]="Container Started"
      if "FinishedAt" in j["State"]:
        ts=self._GetDateTimeFromString(j["State"]["FinishedAt"])
        attr["action"]="Container Finished"
    if "Created" in j:
      ts=self._GetDateTimeFromString(j["Created"])
      attr["action"]="Container Created"

    if ts:
      event_object = DockerContainerEvent(ts,attr)
      parser_mediator.ProduceEvent(event_object)


class DockerContainerEvent(text_events.TextEvent):
  """Convenience class for a mactime-based event."""

  DATA_TYPE = u'docker:json:container'

  def __init__(self, timestamp, attributes):
    super(DockerContainerEvent, self).__init__(timestamp, 0, attributes)
    self.action=attributes['action']
    self.containerid=attributes['containerid']


manager.ParsersManager.RegisterParser(DockerJSONParser)
