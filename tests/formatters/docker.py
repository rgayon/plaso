#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Tests for the Docker JSON event formatters."""

import unittest

from plaso.formatters import docker

from tests.formatters import test_lib


class DockerJSONFormatterTest(test_lib.EventFormatterTestCase):
  """Tests for the Docker JSON event formatters."""

  def testInitializations(self):
    """Tests the initialization of the formatters."""
    event_formatter = docker.DockerBaseEventFormatter()
    self.assertIsNotNone(event_formatter)

    event_formatter = docker.DockerContainerEventFormatter()
    self.assertIsNotNone(event_formatter)

    event_formatter = docker.DockerContainerLogEventFormatter()
    self.assertIsNotNone(event_formatter)

    event_formatter = docker.DockerLayerEventFormatter()
    self.assertIsNotNone(event_formatter)


  def testGetFormatStringAttributeNames(self):
    """Tests the GetFormatStringAttributeNames function."""

    event_formatter = docker.DockerBaseEventFormatter()

    expected_attribute_names = []

    self._TestGetFormatStringAttributeNames(
        event_formatter, expected_attribute_names)

    event_formatter = docker.DockerContainerEventFormatter()

    expected_attribute_names = [u'action',
                                u'container_id',
                                u'container_name']

    self._TestGetFormatStringAttributeNames(
        event_formatter, expected_attribute_names)

    event_formatter = docker.DockerContainerLogEventFormatter()

    expected_attribute_names = [u'container_id',
                                u'log_line',
                                u'log_source']

    self._TestGetFormatStringAttributeNames(
        event_formatter, expected_attribute_names)

    event_formatter = docker.DockerLayerEventFormatter()

    expected_attribute_names = [u'command',
                                u'layer_id']

    self._TestGetFormatStringAttributeNames(
        event_formatter, expected_attribute_names)



if __name__ == '__main__':
  unittest.main()
