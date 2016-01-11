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

class DockerContainerLogEventFormatter(interface.ConditionalEventFormatter):

  DATA_TYPE = u'docker:json:container:log'

  FORMAT_STRING_PIECES= (
      u'text: {log_line}',
      u'Container ID: {container_id}',
      u'source: {log_source}',
  )

  SOURCE_LONG = u'Docker Container Logs'
  SOURCE_SHORT = u'DOCKER'

class DockerLayerEventFormatter(
    interface.ConditionalEventFormatter):
  """Formatter for a Docker Layer event."""

  DATA_TYPE = u'docker:json:layer'

  FORMAT_STRING_PIECES= (
      u'Command: {command}',
      u'Layer ID: {layer_id}',
  )

  SOURCE_LONG = u'Docker Layer'
  SOURCE_SHORT = u'DOCKER'

class DockerContainerEventFormatter(
    interface.ConditionalEventFormatter):
  """Formatter for a Docker event."""

  DATA_TYPE = u'docker:json:container'

  FORMAT_STRING_PIECES = [
      u'Action: {action}',
      u'Container Name: {container_name}',
      u'Container ID: {container_id}',
  ]

  SOURCE_LONG = u'Docker Container'
  SOURCE_SHORT = u'DOCKER'


manager.FormattersManager.RegisterFormatters([
    DockerEventFormatter,
    DockerContainerEventFormatter,
    DockerContainerLogEventFormatter,
    DockerLayerEventFormatter,
])

