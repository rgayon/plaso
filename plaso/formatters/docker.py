# -*- coding: utf-8 -*-
"""The Docker event formatter."""

from plaso.formatters import interface
from plaso.formatters import manager


class DockerEventFormatter(interface.ConditionalEventFormatter):
  """Formatter for a Docker event."""

  DATA_TYPE = u'docker:json'

  FORMAT_STRING_SHORT_PIECES = [
      u'{id}']

  SOURCE_SHORT = u'DOCKER'

class DockerLayerEventFormatter(
    interface.ConditionalEventFormatter):
  """Formatter for a Docker Layer event."""

  DATA_TYPE = u'docker:json:layer'

  FORMAT_STRING_PIECES= (
      u'Layer ID: {id}',
      u'Command: {cmd}'
  )
  FORMAT_STRING_SHORT_PIECES= (
      u'ID: {id}',
      u'Cmd: {cmd}'
  )

  SOURCE_LONG = u'Docker Layer Event'
  SOURCE_SHORT = u'DOCKER'

class DockerContainerEventFormatter(
    interface.ConditionalEventFormatter):
  """Formatter for a Docker event."""

  DATA_TYPE = u'docker:json:container'

  FORMAT_STRING_PIECES = [
      u'Container ID: {id}',
      u'Action: {action}']

  SOURCE_LONG = u'Docker Container Event'
  SOURCE_SHORT = u'DOCKER'


manager.FormattersManager.RegisterFormatters([
    DockerEventFormatter,
    DockerContainerEventFormatter,
    DockerLayerEventFormatter,
])

