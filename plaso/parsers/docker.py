# -*- coding: utf-8 -*-
"""Parser for the Docker conf files

"""

import json
import os

from plaso.events import text_events
from plaso.lib import errors
from plaso.lib import eventdata
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

manager.ParsersManager.RegisterParser(DockerJSONParser)
