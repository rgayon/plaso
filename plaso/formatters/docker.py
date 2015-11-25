# -*- coding: utf-8 -*-
"""The Docker event formatter."""

from plaso.formatters import interface
from plaso.formatters import manager


class DockerEventFormatter(
    interface.ConditionalEventFormatter):
  """Formatter for a Docker event."""

  DATA_TYPE = u'docker:json:generic'

#  FORMAT_STRING_PIECES = [
#      u'CRX ID: {extension_id}',
#      u'CRX Name: {extension_name}',
#      u'Path: {path}']
#
#  FORMAT_STRING_SHORT_PIECES = [
#      u'{extension_id}',
#      u'{path}']

  SOURCE_LONG = u'Docker'
  SOURCE_SHORT = u'LOG'


manager.FormattersManager.RegisterFormatter(
    DockerEventFormatter)

