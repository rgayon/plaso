#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Tests for the Docker JSON parser."""

import unittest

from plaso.lib import timelib
from plaso.parsers import docker

from tests.parsers import test_lib


class DockerJSONUnitTest(test_lib.ParserTestCase):
  """Tests for the Docker JSON parser."""

  def setUp(self):
    """Makes preparations before running an individual test."""
    self._parser = docker.DockerJSONParser()

  def _testParseContainerLog(self):
    """Tests the _ParseContainerLogJSON function."""
    test_file = self._GetTestFilePath([u'docker',
                                       u'containers',
                                       u'e7d0b7ea5ccf08366e2b0c8afa2318674e8aefe802315378125d2bb83fe3110c',
                                       u'container-json.log'])

    event_queue_consumer = self._ParseFile(self._parser, test_file)
    event_objects = self._GetEventObjectsFromQueue(event_queue_consumer)

    self.assertEqual(len(event_objects), 10)

    expected_times = [u'2016-01-07 16:49:10.000000',
                      u'2016-01-07 16:49:10.200000',
                      u'2016-01-07 16:49:10.230000',
                      u'2016-01-07 16:49:10.237000',
                      u'2016-01-07 16:49:10.237200',
                      u'2016-01-07 16:49:10.237220',
                      u'2016-01-07 16:49:10.237222',
                      u'2016-01-07 16:49:10.237222', # losing sub microsec info
                      u'2016-01-07 16:49:10.237222',
                      u'2016-01-07 16:49:10.237222',
                      ]

    expected_log = u'\x1b]0;root@e7d0b7ea5ccf: /home/plaso\x07root@e7d0b7ea5ccf:/home/plaso# ls\r\n'

    expected_msg = (
        u'Text: '
        u'\x1b]0;root@e7d0b7ea5ccf: /home/plaso\x07root@e7d0b7ea5ccf:/home/plaso# ls, '
        u'Container ID: e7d0b7ea5ccf08366e2b0c8afa2318674e8aefe802315378125d2bb83fe3110c, '
        u'Source: stdout'
        )
    expected_msg_short = (
        u'Text: '
        u'\x1b]0;root@e7d0b7ea5ccf: /home/plaso\x07root@e7d0b7ea5ccf:/home/plaso# ls, '
        u'C...'
        )

    for index,event_object in enumerate(event_objects):
      self.assertEqual(event_objects[index].timestamp,
                       timelib.Timestamp.CopyFromString(expected_times[index])
                       )
      self.assertEqual(event_objects[index].container_id,"e7d0b7ea5ccf08366e2b0c8afa2318674e8aefe802315378125d2bb83fe3110c")
      self.assertEqual(event_objects[index].log_line,expected_log)
      self.assertEqual(event_objects[index].log_source,"stdout")
      self._TestGetMessageStrings(event_object, expected_msg, expected_msg_short)

  def _testParseContainerConfig(self):
    """Tests the _ParseContainerConfigJSON function."""
    test_file = self._GetTestFilePath([u'docker',
                                       u'containers',
                                       u'e7d0b7ea5ccf08366e2b0c8afa2318674e8aefe802315378125d2bb83fe3110c',
                                       u'config.json'])

    event_queue_consumer = self._ParseFile(self._parser, test_file)
    event_objects = self._GetEventObjectsFromQueue(event_queue_consumer)

    self.assertEqual(len(event_objects), 2)

    e_o = event_objects[0]
    self.assertEqual(e_o.timestamp, 1452185348674873)
    self.assertEqual(e_o.action, "Container Started")
    self.assertEqual(e_o.container_id, "e7d0b7ea5ccf08366e2b0c8afa2318674e8aefe802315378125d2bb83fe3110c")
    self.assertEqual(e_o.container_name, "e7d0b7ea5ccf")

    e_o = event_objects[1]
    self.assertEqual(e_o.timestamp, 1452185348507979)
    self.assertEqual(e_o.action, "Container Created")
    self.assertEqual(e_o.container_id, "e7d0b7ea5ccf08366e2b0c8afa2318674e8aefe802315378125d2bb83fe3110c")
    self.assertEqual(e_o.container_name, "e7d0b7ea5ccf")


  def _testParseLayerConfig(self):
    """Tests the _ParseLayerConfigJSON function."""
    test_file = self._GetTestFilePath([u'docker',
                                       u'graph',
                                       u'e7d0b7ea5ccf08366e2b0c8afa2318674e8aefe802315378125d2bb83fe3110c',
                                       u'json'])

    event_queue_consumer = self._ParseFile(self._parser, test_file)
    event_objects = self._GetEventObjectsFromQueue(event_queue_consumer)

    self.assertEqual(len(event_objects), 1)

    e_o = event_objects[0]
    print e_o

    self.assertEqual( e_o.command, expected_cmd)
    expected_cmd = "/bin/sh -c echo '#!/bin/sh' > /usr/sbin/policy-rc.d && ",
                   "echo 'exit 101' >> /usr/sbin/policy-rc.d && chmod +x ",
                   "/usr/sbin/policy-rc.d && dpkg-divert --local --rename ",
                   "--add /sbin/initctl && cp -a /usr/sbin/policy-rc.d ",
                   "/sbin/initctl && sed -i 's/^exit.*/exit 0/' ",
                   "/sbin/initctl && echo 'force-unsafe-io' > ",
                   "/etc/dpkg/dpkg.cfg.d/docker-apt-speedup && echo ",
                   "'DPkg::Post-Invoke { \"rm -f /var/cache/apt/archives/",
                   "*.deb /var/cache/apt/archives/partial/*.deb ",
                   "/var/cache/apt/*.bin || true\"; };'"
    self.assertEqual( e_o.command, expected_cmd)
    self.assertEqual(e_o.layer_id, "e7d0b7ea5ccf08366e2b0c8afa2318674e8aefe802315378125d2bb83fe3110c")
    self.assertEqual(e_o.timestamp, 1444670821623276)
    self.assertEqual(e_o.timestamp_desc, "Creation Time")

  def testParse(self):
    self._testParseContainerConfig()
    self._testParseContainerLog()
    self._testParseLayerConfig()

if __name__ == '__main__':
  unittest.main()
