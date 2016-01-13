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

  def testParse(self):
    self._testParseLog()

  def _testParseLog(self):

    """Tests the Parse function."""
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

if __name__ == '__main__':
  unittest.main()
