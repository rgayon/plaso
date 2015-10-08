# -*- coding: utf-8 -*-
"""The default Windows Registry plugin."""

from plaso.events import windows_events
from plaso.lib import utils
from plaso.parsers import winreg
from plaso.parsers.winreg_plugins import interface


class DefaultPlugin(interface.WindowsRegistryPlugin):
  """Default plugin that extracts minimum information from every registry key.

  The default plugin will parse every registry key that is passed to it and
  extract minimum information, such as a list of available values and if
  possible content of those values. The timestamp used is the timestamp
  when the registry key was last modified.
  """

  NAME = u'winreg_default'
  DESCRIPTION = u'Parser for Registry data.'

  def GetEntries(self, parser_mediator, registry_key, **kwargs):
    """Returns an event object based on a Registry key name and values.

    Args:
      parser_mediator: A parser mediator object (instance of ParserMediator).
      registry_key: A Windows Registry key (instance of
                    dfwinreg.WinRegistryKey).
    """
    values_dict = {}

    if registry_key.number_of_values == 0:
      values_dict[u'Value'] = u'No values stored in key.'

    else:
      for value in registry_key.GetValues():
        if not value.name:
          value_name = u'(default)'
        else:
          value_name = u'{0:s}'.format(value.name)

        if value.data is None:
          value_string = u'[{0:s}] Empty'.format(
              value.data_type_string)
        elif value.DataIsString():
          string_decode = utils.GetUnicodeString(value.data)
          value_string = u'[{0:s}] {1:s}'.format(
              value.data_type_string, string_decode)
        elif value.DataIsInteger():
          value_string = u'[{0:s}] {1:d}'.format(
              value.data_type_string, value.data)
        elif value.DataIsMultiString():
          if not isinstance(value.data, (list, tuple)):
            value_string = u'[{0:s}]'.format(value.data_type_string)
            # TODO: Add a flag or some sort of an anomaly alert.
          else:
            value_string = u'[{0:s}] {1:s}'.format(
                value.data_type_string, u''.join(value.data))
        else:
          value_string = u'[{0:s}]'.format(value.data_type_string)

        values_dict[value_name] = value_string

    event_object = windows_events.WindowsRegistryEvent(
        registry_key.last_written_time, registry_key.path, values_dict,
        offset=registry_key.offset)

    parser_mediator.ProduceEvent(event_object)


winreg.WinRegistryParser.RegisterPlugin(DefaultPlugin)
