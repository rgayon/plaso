# -*- coding: utf-8 -*-
"""Parser for the Docker conf files

"""

import re

from plaso.events import text_events
from plaso.lib import errors
from plaso.lib import eventdata
from plaso.parsers import manager
from plaso.parsers import interface

import json

class DockerJSONParser(interface.SingleFileBaseParser):
  """ An empty docstring """

  NAME = u'dockerjson'
  DESCRIPTION = u'Parser for JSON Docker files.'

  def ParseFileObject(self, parser_mediator, file_object):
    file_entry = parser_mediator.GetFileEntry()
    file_object = file_entry.GetFileObject()

    try:
      j = json.load(file_object)

      for jj in j:
        if jj.lower() in ["created","startedat","finishedat"]:
          event_object = DockerEvent(12345678900,0,{jj:j[jj]})
          parser_mediator.ProduceEvent(event_object)
    except ValueError as exception:
      raise errors.UnableToParseFile((
          u'[{0:s}] Unable to parse file {1:s} as '
          u'JSON: {2:s}').format(
              self.NAME, parser_mediator.GetDisplayName(), exception))


class DockerEvent(text_events.TextEvent):
  """Convenience class for a mactime-based event."""

  DATA_TYPE = u'docker:json:generic'

  def __init__(self, posix_time, offset, attr):
    """Initializes a mactime-based event object.

    Args:
      posix_time: The POSIX time value.
      usage: The description of the usage of the time value.
      row_offset: The offset of the row.
      data: A dict object containing extracted data from the body file.
    """
    super(TextEvent, self).__init__(self,posix_time,offset,attr)


manager.ParsersManager.RegisterParser(DockerJSONParser)
