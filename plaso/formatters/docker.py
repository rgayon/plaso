# -*- coding: utf-8 -*-
"""The Docker event formatter."""

from plaso.formatters import interface
from plaso.formatters import manager


class DockerContainerEventFormatter(
    interface.ConditionalEventFormatter):
  """Formatter for a Docker event."""

  DATA_TYPE = u'docker:json:container'

  FORMAT_STRING_PIECES = [
      u'Container ID: {containerid}',
      u'Action: {action}']

  FORMAT_STRING_SHORT_PIECES = [
      u'{action}',
      u'{containerid}']

  SOURCE_LONG = u'Docker Container'
  SOURCE_SHORT = u'Docker'


manager.FormattersManager.RegisterFormatter(
    DockerContainerEventFormatter)

