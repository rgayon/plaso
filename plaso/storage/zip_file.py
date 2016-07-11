# -*- coding: utf-8 -*-
"""The ZIP-based storage.

The ZIP-based storage can be described as a collection of storage files
(named streams) bundled in a single ZIP archive file.

There are multiple types of streams:
* error_data.#
  The error data streams contain the serialized error objects.
* error_index.#
  The error index streams contain the stream offset to the serialized
  error objects.
* event_data.#
  The event data streams contain the serialized events.
* event_index.#
  The event index streams contain the stream offset to the serialized
  events.
* event_source_data.#
  The event source data streams contain the serialized event source objects.
* event_source_index.#
  The event source index streams contain the stream offset to the serialized
  event source objects.
* event_tag_data.#
  The event tag data streams contain the serialized event tag objects.
* event_tag_index.#
  The event tag index streams contain the stream offset to the serialized
  event tag objects.
* event_timestamps.#
  The event timestamps streams contain the timestamp of the serialized
  events.
* information.dump
  The serialized preprocessing object.
* metadata.txt
  Stream that contains the storage metadata.
* session_completion.#
  Stream that contains information about the completion of a session.
  Only applies to session-based storage.
* session_start.#
  Stream that contains information about the start of a session.
  Only applies to session-based storage.
* task_completion.#
  Stream that contains information about the completion of a task.
  Only applies to task-based storage.
* task_start.#
  Stream that contains information about the start of a task.
  Only applies to task-based storage.

The # in a stream name is referred to as the "store number". Streams with
the same prefix e.g. "event_" and "store number" are related.

+ The event data streams

The event data streams contain the serialized events. The serialized
events are stored in ascending timestamp order within an individual event data
stream. Note that the event data streams themselves are not ordered.

The event data streams were previously referred to as "proto files" because
historically the event data was serialized as protocol buffers (protobufs).

An event data stream consists of:
+------+-----------------+------+-...-+
| size | serialized data | size | ... |
+------+-----------------+------+-...-+

Where size is a 32-bit integer.

+ The event index stream

The event index streams contain the stream offset to the serialized event
objects stored in the corresponding event data stream.

An event data stream consists of an array of 32-bit integers:
+-----+-----+-...-+
| int | int | ... |
+-----+-----+-...-+

+ The event timestamps stream

The event timestamps streams contain the timestamp of the serialized
events.

An event data stream consists of an array of 64-bit integers:
+-----------+-----------+-...-+
| timestamp | timestamp | ... |
+-----------+-----------+-...-+

+ The event tag index stream

The event tag index streams contain information about the event
the tag applies to.

An event data stream consists of an array of event tag index values.
+--------+--------+-...-+
| struct | struct | ... |
+--------+--------+-...-+

See the _SerializedEventTagIndexTable class for more information about
the actual structure of an event tag index value.

+ Version information

Deprecated in version 20160501:
* serializer.txt
  Stream that contains the serializer format.

Deprecated in version 20160511:
* plaso_index.#
  The event index streams contain the stream offset to the serialized
  events.
* plaso_proto.#
  The event data streams contain the serialized events.
* plaso_report.#
* plaso_tagging.#
  The event tag data streams contain the serialized event tag objects.
* plaso_tag_index.#
  The event tag index streams contain the stream offset to the serialized
  event tag objects.
* plaso_timestamps.#
  The event timestamps streams contain the timestamp of the serialized
  events.
"""

import heapq
import io
import logging
import os
import shutil
import tempfile
import warnings
import zipfile

try:
  import ConfigParser as configparser
except ImportError:
  import configparser  # pylint: disable=import-error

import construct

from plaso.lib import definitions
from plaso.lib import errors
from plaso.serializer import json_serializer
from plaso.storage import interface


class _AttributeContainersList(object):
  """Class that defines the attribute containers list.

  The list is unsorted and pops attribute containers in the same order as
  pushed to preserve order.

  The GetAttributeContainerByIndex method should be used to read attribute
  containers from the list while it being filled.

  Attributes:
    data_size (int): total data size of the serialized attribute containers
        on the list.
  """

  def __init__(self):
    """Initializes an attribute container list."""
    super(_AttributeContainersList, self).__init__()
    self._list = []
    self.data_size = 0

  @property
  def number_of_attribute_containers(self):
    """int: number of serialized attribute containers on the list."""
    return len(self._list)

  def Empty(self):
    """Empties the list."""
    self._list = []
    self.data_size = 0

  def GetAttributeContainerByIndex(self, index):
    """Retrieves a specific attribute container from the list.

    Args:
      index (int): attribute container index.

    Returns:
      bytes: serialized attribute container data.
    """
    if index < len(self._list):
      return self._list[index]

  def PopAttributeContainer(self):
    """Pops an attribute container from the list.

    Returns:
      bytes: serialized attribute container data.
    """
    try:
      serialized_data = self._list.pop(0)
      self.data_size -= len(serialized_data)
      return serialized_data

    except IndexError:
      return

  def PushAttributeContainer(self, serialized_data):
    """Pushes an attribute container onto the list.

    Args:
      serialized_data (bytes): serialized attribute container data.
    """
    self._list.append(serialized_data)
    self.data_size += len(serialized_data)


class _EventsHeap(object):
  """Class that defines the events heap."""

  def __init__(self):
    """Initializes an events heap."""
    super(_EventsHeap, self).__init__()
    self._heap = []

  @property
  def number_of_events(self):
    """int: number of serialized events on the heap."""
    return len(self._heap)

  def PopEvent(self):
    """Pops an event from the heap.

    Returns:
      A tuple containing an event (instance of EventObject),
      an integer containing the number of the stream.
      If the heap is empty the values in the tuple will be None.
    """
    try:
      _, stream_number, _, event = heapq.heappop(self._heap)
      return event, stream_number

    except IndexError:
      return None, None

  def PushEvent(self, event, stream_number, entry_index):
    """Pushes an event onto the heap.

    Args:
      event (EventObject): event.
      stream_number (int): serialized data stream number.
      entry_index (int): serialized data stream entry index.
    """
    heap_values = (event.timestamp, stream_number, entry_index, event)
    heapq.heappush(self._heap, heap_values)


class _SerializedEventsHeap(object):
  """Class that defines the serialized events heap.

  Attributes:
    data_size (int): total data size of the serialized events on the heap.
  """

  def __init__(self):
    """Initializes a serialized events heap."""
    super(_SerializedEventsHeap, self).__init__()
    self._heap = []
    self.data_size = 0

  @property
  def number_of_events(self):
    """int: number of serialized events on the heap."""
    return len(self._heap)

  def Empty(self):
    """Empties the heap."""
    self._heap = []
    self.data_size = 0

  def PopEvent(self):
    """Pops an event from the heap.

    Returns:
      A tuple containing an integer containing the event timestamp and
      a binary string containing the serialized event data.
      If the heap is empty the values in the tuple will be None.
    """
    try:
      timestamp, event_data = heapq.heappop(self._heap)

      self.data_size -= len(event_data)
      return timestamp, event_data

    except IndexError:
      return None, None

  def PushEvent(self, timestamp, event_data):
    """Pushes a serialized event onto the heap.

    Args:
      timestamp (int): event timestamp, which contains the number of
          micro seconds since January 1, 1970, 00:00:00 UTC.
      event_data (bytes): serialized event data.
    """
    heap_values = (timestamp, event_data)
    heapq.heappush(self._heap, heap_values)
    self.data_size += len(event_data)


class _EventTagIndexValue(object):
  """Class that defines the event tag index value.

  Attributes:
    event_uuid (str): event identifier formatted as an UUID.
    offset (int): serialized event tag data offset.
    store_number (int): serialized data stream number.
    store_index (int): serialized data stream entry index.
    tag_type (int): tag type.
  """
  TAG_TYPE_UNDEFINED = 0
  TAG_TYPE_NUMERIC = 1
  TAG_TYPE_UUID = 2

  def __init__(
      self, tag_type, offset, event_uuid=None, store_number=None,
      store_index=None):
    """Initializes the tag index value.

    Args:
      tag_type (int): tag type.
      offset (int): serialized event tag data offset.
      event_uuid (Optional[str]): event identifier formatted as an UUID.
      store_number (Optional[int]): serialized data stream number.
      store_index (Optional[int]): serialized data stream entry index.
    """
    super(_EventTagIndexValue, self).__init__()
    self._identifier = None
    self.event_uuid = event_uuid
    self.offset = offset
    self.store_number = store_number
    self.store_index = store_index
    self.tag_type = tag_type

  def __str__(self):
    """str: string representation of the event tag identifier."""
    string = u'tag_type: {0:d} offset: 0x{1:08x}'.format(
        self.tag_type, self.offset)

    if self.tag_type == self.TAG_TYPE_NUMERIC:
      return u'{0:s} store_number: {1:d} store_index: {2:d}'.format(
          string, self.store_number, self.store_index)

    elif self.tag_type == self.TAG_TYPE_UUID:
      return u'{0:s} event_uuid: {1:s}'.format(string, self.event_uuid)

    return string

  @property
  def identifier(self):
    """str: event identifier."""
    if not self._identifier:
      if self.tag_type == self.TAG_TYPE_NUMERIC:
        self._identifier = u'{0:d}:{1:d}'.format(
            self.store_number, self.store_index)

      elif self.tag_type == self.TAG_TYPE_UUID:
        self._identifier = self.event_uuid

    return self._identifier

  @property
  def tag(self):
    """The tag property to support construct.build()."""
    return self


class _SerializedDataStream(object):
  """Class that defines a serialized data stream."""

  _DATA_ENTRY = construct.Struct(
      u'data_entry',
      construct.ULInt32(u'size'))
  _DATA_ENTRY_SIZE = _DATA_ENTRY.sizeof()

  # The maximum serialized data size (40 MiB).
  _MAXIMUM_DATA_SIZE = 40 * 1024 * 1024

  def __init__(self, zip_file, storage_file_path, stream_name):
    """Initializes a serialized data stream object.

    Args:
      zip_file (zipfile.ZipFile): ZIP file that contains the stream.
      storage_file_path (str): path of the storage file.
      stream_name (str): name of the stream.
    """
    super(_SerializedDataStream, self).__init__()
    self._entry_index = 0
    self._file_object = None
    self._path = os.path.dirname(os.path.abspath(storage_file_path))
    self._stream_name = stream_name
    self._stream_offset = 0
    self._zip_file = zip_file

  @property
  def entry_index(self):
    """int: entry index."""
    return self._entry_index

  def _OpenFileObject(self):
    """Opens the file-like object (instance of ZipExtFile).

    Raises:
      IOError: if the file-like object cannot be opened.
    """
    try:
      self._file_object = self._zip_file.open(self._stream_name, mode='r')
    except KeyError as exception:
      raise IOError(
          u'Unable to open stream with error: {0:s}'.format(exception))

    self._stream_offset = 0

  def _ReOpenFileObject(self):
    """Reopens the file-like object (instance of ZipExtFile)."""
    if self._file_object:
      self._file_object.close()
      self._file_object = None

    self._file_object = self._zip_file.open(self._stream_name, mode='r')
    self._stream_offset = 0

  def ReadEntry(self):
    """Reads an entry from the data stream.

    Returns:
      bytes: data or None if no data remaining.

    Raises:
      IOError: if the entry cannot be read.
    """
    if not self._file_object:
      self._OpenFileObject()

    data = self._file_object.read(self._DATA_ENTRY_SIZE)
    if not data:
      return

    try:
      data_entry = self._DATA_ENTRY.parse(data)
    except construct.FieldError as exception:
      raise IOError(
          u'Unable to read data entry with error: {0:s}'.format(exception))

    if data_entry.size > self._MAXIMUM_DATA_SIZE:
      raise IOError(
          u'Unable to read data entry size value out of bounds.')

    data = self._file_object.read(data_entry.size)
    if len(data) != data_entry.size:
      raise IOError(u'Unable to read data.')

    self._stream_offset += self._DATA_ENTRY_SIZE + data_entry.size
    self._entry_index += 1

    return data

  def SeekEntryAtOffset(self, entry_index, stream_offset):
    """Seeks a specific serialized data stream entry at a specific offset.

    Args:
      entry_index (int): serialized data stream entry index.
      stream_offset (int): data stream offset.
    """
    if not self._file_object:
      self._OpenFileObject()

    if stream_offset < self._stream_offset:
      # Since zipfile.ZipExtFile is not seekable we need to close the stream
      # and reopen it to fake a seek.
      self._ReOpenFileObject()

      skip_read_size = stream_offset
    else:
      skip_read_size = stream_offset - self._stream_offset

    if skip_read_size > 0:
      # Since zipfile.ZipExtFile is not seekable we need to read upto
      # the stream offset.
      self._file_object.read(skip_read_size)
      self._stream_offset += skip_read_size

    self._entry_index = entry_index

  def WriteAbort(self):
    """Aborts the write of a serialized data stream."""
    if self._file_object:
      self._file_object.close()
      self._file_object = None

    if os.path.exists(self._stream_name):
      os.remove(self._stream_name)

  def WriteEntry(self, data):
    """Writes an entry to the file-like object.

    Args:
      data (bytes): data.

    Returns:
      int: offset of the entry within the temporary file.

    Raises:
      IOError: if the entry cannot be written.
    """
    data_size = construct.ULInt32(u'size').build(len(data))
    self._file_object.write(data_size)
    self._file_object.write(data)

    return self._file_object.tell()

  def WriteFinalize(self):
    """Finalize the write of a serialized data stream.

    Writes the temporary file with the serialized data to the zip file.

    Returns:
      int: offset of the entry within the temporary file.

    Raises:
      IOError: if the serialized data stream cannot be written.
    """
    offset = self._file_object.tell()
    self._file_object.close()
    self._file_object = None

    current_working_directory = os.getcwd()
    try:
      os.chdir(self._path)
      self._zip_file.write(self._stream_name)
    finally:
      os.remove(self._stream_name)
      os.chdir(current_working_directory)

    return offset

  def WriteInitialize(self):
    """Initializes the write of a serialized data stream.

    Creates a temporary file to store the serialized data.

    Returns:
      int: offset of the entry within the temporary file.

    Raises:
      IOError: if the serialized data stream cannot be written.
    """
    stream_file_path = os.path.join(self._path, self._stream_name)
    self._file_object = open(stream_file_path, 'wb')
    return self._file_object.tell()


class _SerializedDataOffsetTable(object):
  """Class that defines a serialized data offset table."""

  _TABLE = construct.GreedyRange(
      construct.ULInt32(u'offset'))

  _TABLE_ENTRY = construct.Struct(
      u'table_entry',
      construct.ULInt32(u'offset'))
  _TABLE_ENTRY_SIZE = _TABLE_ENTRY.sizeof()

  def __init__(self, zip_file, stream_name):
    """Initializes a serialized data offset table object.

    Args:
      zip_file (zipfile.ZipFile): ZIP file that contains the stream.
      stream_name (str): name of the stream.
    """
    super(_SerializedDataOffsetTable, self).__init__()
    self._offsets = []
    self._stream_name = stream_name
    self._zip_file = zip_file

  @property
  def number_of_offsets(self):
    """int: number of offsets."""
    return len(self._offsets)

  def AddOffset(self, offset):
    """Adds an offset.

    Args:
      offset (int): offset.
    """
    self._offsets.append(offset)

  def GetOffset(self, entry_index):
    """Retrieves a specific serialized data offset.

    Args:
      entry_index (int): table entry index.

    Returns:
      int: serialized data offset.

    Raises:
      IndexError: if the table entry index is out of bounds.
    """
    return self._offsets[entry_index]

  def Read(self):
    """Reads the serialized data offset table.

    Raises:
      IOError: if the offset table cannot be read.
    """
    try:
      file_object = self._zip_file.open(self._stream_name, mode='r')
    except KeyError as exception:
      raise IOError(
          u'Unable to open stream with error: {0:s}'.format(exception))

    try:
      entry_data = file_object.read(self._TABLE_ENTRY_SIZE)
      while entry_data:
        table_entry = self._TABLE_ENTRY.parse(entry_data)

        self._offsets.append(table_entry.offset)
        entry_data = file_object.read(self._TABLE_ENTRY_SIZE)

    except construct.FieldError as exception:
      raise IOError(
          u'Unable to read table entry with error: {0:s}'.format(exception))

    finally:
      file_object.close()

  def Write(self):
    """Writes the offset table.

    Raises:
      IOError: if the offset table cannot be written.
    """
    table_data = self._TABLE.build(self._offsets)
    self._zip_file.writestr(self._stream_name, table_data)


class _SerializedDataTimestampTable(object):
  """Class that defines a serialized data timestamp table."""

  _TABLE = construct.GreedyRange(
      construct.SLInt64(u'timestamp'))

  _TABLE_ENTRY = construct.Struct(
      u'table_entry',
      construct.SLInt64(u'timestamp'))
  _TABLE_ENTRY_SIZE = _TABLE_ENTRY.sizeof()

  def __init__(self, zip_file, stream_name):
    """Initializes a serialized data timestamp table object.

    Args:
      zip_file (zipfile.ZipFile): ZIP file that contains the stream.
      stream_name (str): name of the stream.
    """
    super(_SerializedDataTimestampTable, self).__init__()
    self._stream_name = stream_name
    self._timestamps = []
    self._zip_file = zip_file

  @property
  def number_of_timestamps(self):
    """int: number of timestamps."""
    return len(self._timestamps)

  def AddTimestamp(self, timestamp):
    """Adds a timestamp.

    Args:
      timestamp (int): event timestamp, which contains the number of
          micro seconds since January 1, 1970, 00:00:00 UTC.
    """
    self._timestamps.append(timestamp)

  def GetTimestamp(self, entry_index):
    """Retrieves a specific timestamp.

    Args:
      entry_index (int): table entry index.

    Returns:
      int: event timestamp, which contains the number of micro seconds since
          January 1, 1970, 00:00:00 UTC.

    Raises:
      IndexError: if the table entry index is out of bounds.
    """
    return self._timestamps[entry_index]

  def Read(self):
    """Reads the serialized data timestamp table.

    Raises:
      IOError: if the timestamp table cannot be read.
    """
    try:
      file_object = self._zip_file.open(self._stream_name, mode='r')
    except KeyError as exception:
      raise IOError(
          u'Unable to open stream with error: {0:s}'.format(exception))

    try:
      entry_data = file_object.read(self._TABLE_ENTRY_SIZE)
      while entry_data:
        table_entry = self._TABLE_ENTRY.parse(entry_data)

        self._timestamps.append(table_entry.timestamp)
        entry_data = file_object.read(self._TABLE_ENTRY_SIZE)

    except construct.FieldError as exception:
      raise IOError(
          u'Unable to read table entry with error: {0:s}'.format(exception))

    finally:
      file_object.close()

  def Write(self):
    """Writes the timestamp table.

    Raises:
      IOError: if the timestamp table cannot be written.
    """
    table_data = self._TABLE.build(self._timestamps)
    self._zip_file.writestr(self._stream_name, table_data)


class _SerializedEventTagIndexTable(object):
  """Class that defines a serialized event tag index table."""

  _TAG_STORE_STRUCT = construct.Struct(
      u'tag_store',
      construct.ULInt32(u'store_number'),
      construct.ULInt32(u'store_index'))

  _TAG_UUID_STRUCT = construct.Struct(
      u'tag_uuid',
      construct.PascalString(u'event_uuid'))

  _TAG_INDEX_STRUCT = construct.Struct(
      u'tag_index',
      construct.Byte(u'tag_type'),
      construct.ULInt32(u'offset'),
      construct.IfThenElse(
          u'tag',
          lambda ctx: ctx[u'tag_type'] == 1,
          _TAG_STORE_STRUCT,
          _TAG_UUID_STRUCT))

  def __init__(self, zip_file, stream_name):
    """Initializes a serialized event tag index table object.

    Args:
      zip_file (zipfile.ZipFile): ZIP file that contains the stream.
      stream_name (str): name of the stream.
    """
    super(_SerializedEventTagIndexTable, self).__init__()
    self._event_tag_indexes = []
    self._stream_name = stream_name
    self._zip_file = zip_file

  @property
  def number_of_entries(self):
    """int: number of event tag index entries."""
    return len(self._event_tag_indexes)

  def AddEventTagIndex(
      self, tag_type, offset, event_uuid=None, store_number=None,
      store_index=None):
    """Adds an event tag index.

    Args:
      tag_type (int): event tag type.
      offset (int): serialized event tag data offset.
      event_uuid (Optional[str]): event identifier formatted as an UUID.
      store_number (Optional[str]): store number.
      store_index (Optional[str]): index relative to the start of the store.
    """
    event_tag_index = _EventTagIndexValue(
        tag_type, offset, event_uuid=event_uuid, store_number=store_number,
        store_index=store_index)
    self._event_tag_indexes.append(event_tag_index)

  def GetEventTagIndex(self, entry_index):
    """Retrieves a specific event tag index.

    Args:
      entry_index (int): table entry index.

    Returns:
      _EventTagIndexValue: event tag index value.

    Raises:
      IndexError: if the table entry index is out of bounds.
    """
    return self._event_tag_indexes[entry_index]

  def Read(self):
    """Reads the serialized event tag index table.

    Raises:
      IOError: if the event tag index table cannot be read.
    """
    try:
      _, _, stream_store_number = self._stream_name.rpartition(u'.')
      stream_store_number = int(stream_store_number, 10)
    except ValueError as exception:
      raise IOError((
          u'Unable to determine store number of stream: {0:s} '
          u'with error: {1:s}').format(self._stream_name, exception))

    try:
      file_object = self._zip_file.open(self._stream_name, mode='r')
    except KeyError as exception:
      raise IOError(
          u'Unable to open stream with error: {0:s}'.format(exception))

    try:
      while True:
        try:
          tag_index_struct = self._TAG_INDEX_STRUCT.parse_stream(file_object)
        except (construct.FieldError, AttributeError):
          break

        tag_type = tag_index_struct.get(
            u'tag_type', _EventTagIndexValue.TAG_TYPE_UNDEFINED)
        if tag_type not in (
            _EventTagIndexValue.TAG_TYPE_NUMERIC,
            _EventTagIndexValue.TAG_TYPE_UUID):
          logging.warning(u'Unsupported tag type: {0:d}'.format(tag_type))
          break

        offset = tag_index_struct.get(u'offset', None)
        tag_index = tag_index_struct.get(u'tag', {})
        event_uuid = tag_index.get(u'event_uuid', None)
        store_number = tag_index.get(u'store_number', stream_store_number)
        store_index = tag_index.get(u'store_index', None)

        event_tag_index = _EventTagIndexValue(
            tag_type, offset, event_uuid=event_uuid, store_number=store_number,
            store_index=store_index)
        self._event_tag_indexes.append(event_tag_index)

    finally:
      file_object.close()

  def Write(self):
    """Writes the event tag index table.

    Raises:
      IOError: if the event tag index table cannot be written.
    """
    serialized_entries = []
    for event_tag_index in self._event_tag_indexes:
      entry_data = self._TAG_INDEX_STRUCT.build(event_tag_index)
      serialized_entries.append(entry_data)

    table_data = b''.join(serialized_entries)
    self._zip_file.writestr(self._stream_name, table_data)


class _StorageMetadata(object):
  """Class that implements storage metadata.

  Attributes:
    format_version (int): storage format version.
    serialization_format (str): serialization format.
    storage_type (str): storage type.
  """

  def __init__(self):
    """Initializes storage metadata."""
    super(_StorageMetadata, self).__init__()
    self.format_version = None
    self.serialization_format = None
    self.storage_type = None


class _StorageMetadataReader(object):
  """Class that implements a storage metadata reader."""

  def _GetConfigValue(self, config_parser, section_name, value_name):
    """Retrieves a value from the config parser.

    Args:
      config_parser (ConfigParser): configuration parser.
      section_name (str): name of the section that contains the value.
      value_name (str): name of the value.

    Returns:
      object: value or None if the value does not exists.
    """
    try:
      return config_parser.get(section_name, value_name).decode('utf-8')
    except (configparser.NoOptionError, configparser.NoSectionError):
      return

  def Read(self, stream_data):
    """Reads the storage metadata.

    Args:
      stream_data (bytes): data of the steam.

    Returns:
      _StorageMetadata: storage metadata.
    """
    config_parser = configparser.RawConfigParser()
    config_parser.readfp(io.BytesIO(stream_data))

    section_name = u'plaso_storage_file'

    storage_metadata = _StorageMetadata()

    format_version = self._GetConfigValue(
        config_parser, section_name, u'format_version')

    try:
      storage_metadata.format_version = int(format_version, 10)
    except (TypeError, ValueError):
      storage_metadata.format_version = None

    storage_metadata.serialization_format = self._GetConfigValue(
        config_parser, section_name, u'serialization_format')

    storage_metadata.storage_type = self._GetConfigValue(
        config_parser, section_name, u'storage_type')

    if not storage_metadata.storage_type:
      storage_metadata.storage_type = definitions.STORAGE_TYPE_SESSION

    return storage_metadata


# TODO: merge ZIPStorageFile and StorageFile.
class ZIPStorageFile(interface.BaseStorage):
  """Class that defines the ZIP-based storage file.

  Attributes:
    format_version (int): storage format version.
    serialization_format (str): serialization format.
    storage_type (str): storage type.
  """

  # TODO: remove after merge of ZIPStorageFile and StorageFile.
  # pylint: disable=abstract-method

  # The format version.
  _FORMAT_VERSION = 20160525

  # The earliest format version, stored in-file, that this class
  # is able to read.
  _COMPATIBLE_FORMAT_VERSION = 20160501

  # The format version used for storage files predating storing
  # a format version.
  _LEGACY_FORMAT_VERSION = 20160431

  # The maximum buffer size of serialized data before triggering
  # a flush to disk (196 MiB).
  _MAXIMUM_BUFFER_SIZE = 196 * 1024 * 1024

  # The maximum number of cached tables.
  _MAXIMUM_NUMBER_OF_CACHED_TABLES = 5

  # The maximum serialized report size (24 MiB).
  _MAXIMUM_SERIALIZED_REPORT_SIZE = 24 * 1024 * 1024

  def __init__(
      self, maximum_buffer_size=0,
      storage_type=definitions.STORAGE_TYPE_SESSION):
    """Initializes a ZIP-based storage file object.

    Args:
      maximum_buffer_size (Optional[int]):
          maximum size of a single storage stream. A value of 0 indicates
          the limit is _MAXIMUM_BUFFER_SIZE.
      storage_type (Optional[str]): storage type.

    Raises:
      ValueError: if the maximum buffer size value is out of bounds.
    """
    if (maximum_buffer_size < 0 or
        maximum_buffer_size > self._MAXIMUM_BUFFER_SIZE):
      raise ValueError(u'Maximum buffer size value out of bounds.')

    if not maximum_buffer_size:
      maximum_buffer_size = self._MAXIMUM_BUFFER_SIZE

    super(ZIPStorageFile, self).__init__()
    self._error_stream_number = 1
    self._errors_list = _AttributeContainersList()
    self._event_offset_tables = {}
    self._event_offset_tables_lfu = []
    self._event_stream_number = 1
    self._event_streams = {}
    self._event_source_offset_tables = {}
    self._event_source_offset_tables_lfu = []
    self._event_source_stream_number = 1
    self._event_source_streams = {}
    self._event_sources_list = _AttributeContainersList()
    self._event_tag_index = None
    self._event_tag_stream_number = 1
    self._event_timestamp_tables = {}
    self._event_timestamp_tables_lfu = []
    self._event_heap = None
    self._is_open = False
    self._last_session = 0
    self._last_task = 0
    self._maximum_buffer_size = maximum_buffer_size
    self._read_only = True
    self._serialized_event_tags = []
    self._serialized_event_tags_size = 0
    self._serialized_events_heap = _SerializedEventsHeap()
    self._serializer = json_serializer.JSONAttributeContainerSerializer
    self._path = None
    self._zipfile = None
    self._zipfile_path = None

    self.format_version = self._FORMAT_VERSION
    self.serialization_format = definitions.SERIALIZER_FORMAT_JSON
    self.storage_type = storage_type

  def _BuildTagIndex(self):
    """Builds the tag index that contains the offsets for each tag.

    Raises:
      IOError: if the stream cannot be opened.
    """
    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_tag_index.'
    else:
      stream_name_prefix = u'event_tag_index.'

    self._event_tag_index = {}

    for stream_name in self._GetStreamNames():
      if not stream_name.startswith(stream_name_prefix):
        continue

      event_tag_index_table = _SerializedEventTagIndexTable(
          self._zipfile, stream_name)
      event_tag_index_table.Read()

      for entry_index in range(event_tag_index_table.number_of_entries):
        tag_index_value = event_tag_index_table.GetEventTagIndex(entry_index)
        self._event_tag_index[tag_index_value.identifier] = tag_index_value

  def _GetEventObject(self, stream_number, entry_index=-1):
    """Reads an event from a specific stream.

    Args:
      stream_number (int): number of the serialized event object stream.
      entry_index (Optional[int]): number of the serialized event within
          the stream, where -1 represents the next available event.

    Returns:
      EventObject: event or None.
    """
    event_data, entry_index = self._GetEventObjectSerializedData(
        stream_number, entry_index=entry_index)
    if not event_data:
      return

    if self._serializers_profiler:
      self._serializers_profiler.StartTiming(u'event')

    event = self._serializer.ReadSerialized(event_data)

    if self._serializers_profiler:
      self._serializers_profiler.StopTiming(u'event')

    event.store_number = stream_number
    event.store_index = entry_index

    return event

  def _GetEventObjectSerializedData(self, stream_number, entry_index=-1):
    """Retrieves specific event serialized data.

    By default the first available entry in the specific serialized stream
    is read, however any entry can be read using the index stream.

    Args:
      stream_number (int): number of the serialized event object stream.
      entry_index (Optional[int]): number of the serialized event within
          the stream, where -1 represents the next available event.

    Returns:
      A tuple containing the event serialized data and the entry index
      of the event within the storage file.

    Raises:
      IOError: if the stream cannot be opened.
      ValueError: if the entry index is out of bounds.
    """
    if entry_index < -1:
      raise ValueError(u'Entry index out of bounds.')

    try:
      data_stream = self._GetSerializedEventStream(stream_number)
    except IOError as exception:
      logging.error((
          u'Unable to retrieve serialized data steam: {0:d} '
          u'with error: {1:s}.').format(stream_number, exception))
      return None, None

    if entry_index >= 0:
      try:
        offset_table = self._GetSerializedEventOffsetTable(stream_number)
        stream_offset = offset_table.GetOffset(entry_index)
      except (IndexError, IOError):
        logging.error((
            u'Unable to read entry index: {0:d} from serialized data stream: '
            u'{1:d}').format(entry_index, stream_number))
        return None, None

      data_stream.SeekEntryAtOffset(entry_index, stream_offset)

    event_entry_index = data_stream.entry_index
    try:
      event_data = data_stream.ReadEntry()
    except IOError as exception:
      logging.error((
          u'Unable to read entry from serialized data steam: {0:d} '
          u'with error: {1:s}.').format(stream_number, exception))
      return None, None

    return event_data, event_entry_index

  def _GetEventSource(self, stream_number, entry_index=-1):
    """Reads an event source from a specific stream.

    Args:
      stream_number (int): number of the serialized event source object stream.
      entry_index (Optional[int]): number of the serialized event source
          within the stream, where -1 represents the next available event
          source.

    Returns:
      EventSource: event source or None.
    """
    event_source_data, entry_index = self._GetEventSourceSerializedData(
        stream_number, entry_index=entry_index)
    if not event_source_data:
      return

    if self._serializers_profiler:
      self._serializers_profiler.StartTiming(u'event_source')

    event_source = self._serializer.ReadSerialized(event_source_data)

    if self._serializers_profiler:
      self._serializers_profiler.StopTiming(u'event_source')

    return event_source

  def _GetEventSourceSerializedData(self, stream_number, entry_index=-1):
    """Retrieves specific event source serialized data.

    By default the first available entry in the specific serialized stream
    is read, however any entry can be read using the index stream.

    Args:
      stream_number (int): number of the serialized event source object stream.
      entry_index (Optional[int]): number of the serialized event source
          within the stream, where -1 represents the next available event
          source.

    Returns:
      A tuple containing the event source serialized data and the entry index
      of the event source within the storage file.

    Raises:
      IOError: if the stream cannot be opened.
      ValueError: if the entry index is out of bounds.
    """
    if entry_index < -1:
      raise ValueError(u'Entry index out of bounds.')

    try:
      data_stream = self._GetSerializedEventSourceStream(stream_number)
    except IOError as exception:
      logging.error((
          u'Unable to retrieve serialized data steam: {0:d} '
          u'with error: {1:s}.').format(stream_number, exception))
      return None, None

    if entry_index >= 0:
      try:
        offset_table = self._GetSerializedEventSourceOffsetTable(stream_number)
        stream_offset = offset_table.GetOffset(entry_index)
      except (IOError, IndexError):
        logging.error((
            u'Unable to read entry index: {0:d} from serialized data stream: '
            u'{1:d}').format(entry_index, stream_number))
        return None, None

      data_stream.SeekEntryAtOffset(entry_index, stream_offset)

    event_source_entry_index = data_stream.entry_index
    try:
      event_source_data = data_stream.ReadEntry()
    except IOError as exception:
      logging.error((
          u'Unable to read entry from serialized data steam: {0:d} '
          u'with error: {1:s}.').format(stream_number, exception))
      return None, None

    return event_source_data, event_source_entry_index

  def _GetEventTagIndexValue(self, store_number, entry_index, uuid):
    """Retrieves an event tag index value.

    Args:
      store_number (int): store number.
      entry_index (int): serialized data stream entry index.
      uuid (str): event identifier formatted as an UUID.

    Returns:
      An event tag index value (instance of _EventTagIndexValue).
    """
    if self._event_tag_index is None:
      self._BuildTagIndex()

    # Try looking up event tag by numeric identifier.
    tag_identifier = u'{0:d}:{1:d}'.format(store_number, entry_index)
    tag_index_value = self._event_tag_index.get(tag_identifier, None)

    # Try looking up event tag by UUID.
    if tag_index_value is None:
      tag_index_value = self._event_tag_index.get(uuid, None)

    return tag_index_value

  def _GetLastStreamNumber(self, stream_name_prefix):
    """Retrieves the last stream number.

    Args:
      stream_name_prefix (str): stream name prefix.

    Returns:
      int: last stream number.

    Raises:
      IOError: if the stream number format is not supported.
    """
    last_stream_number = 0
    for stream_name in self._GetStreamNames():
      if stream_name.startswith(stream_name_prefix):
        _, _, stream_number = stream_name.partition(u'.')

        try:
          stream_number = int(stream_number, 10)
        except ValueError:
          raise IOError(
              u'Unsupported stream number: {0:s}'.format(stream_number))

        if stream_number > last_stream_number:
          last_stream_number = stream_number

    return last_stream_number + 1

  def _InitializeMergeBuffer(self, time_range=None):
    """Initializes the events into the merge buffer.

    This function fills the merge buffer with the first relevant event
    from each stream.

    Args:
      time_range (Optional[TimeRange]): time range used to filter events
          that fall in a specific period.
    """
    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_timestamps'
    else:
      stream_name_prefix = u'event_timestamps'

    self._event_heap = _EventsHeap()

    number_range = self._GetSerializedEventStreamNumbers()
    for stream_number in number_range:
      entry_index = -1
      if time_range:
        stream_name = u'{0:s}.{1:06d}'.format(stream_name_prefix, stream_number)
        if self._HasStream(stream_name):
          try:
            timestamp_table = self._GetSerializedEventTimestampTable(
                stream_number)
          except IOError as exception:
            logging.error((
                u'Unable to read timestamp table from stream: {0:s} '
                u'with error: {1:s}.').format(stream_name, exception))

          # If the start timestamp of the time range filter is larger than the
          # last timestamp in the timestamp table skip this stream.
          timestamp_compare = timestamp_table.GetTimestamp(-1)
          if time_range.start_timestamp > timestamp_compare:
            continue

          for table_index in range(timestamp_table.number_of_timestamps - 1):
            timestamp_compare = timestamp_table.GetTimestamp(table_index)
            if time_range.start_timestamp >= timestamp_compare:
              entry_index = table_index
              break

      event = self._GetEventObject(stream_number, entry_index=entry_index)
      # Check the lower bound in case no timestamp table was available.
      while (event and time_range and
             event.timestamp < time_range.start_timestamp):
        event = self._GetEventObject(stream_number)

      if event:
        if time_range and event.timestamp > time_range.end_timestamp:
          continue

        self._event_heap.PushEvent(
            event, stream_number, event.store_number)

        reference_timestamp = event.timestamp
        while event.timestamp == reference_timestamp:
          event = self._GetEventObject(stream_number)
          if not event:
            break

          self._event_heap.PushEvent(
              event, stream_number, event.store_number)

  def _GetSerializedDataStream(
      self, streams_cache, stream_name_prefix, stream_number):
    """Retrieves the serialized data stream.

    Args:
      streams_cache (dict): streams cache.
      stream_name_prefix (str): stream name prefix.
      stream_number (int): number of the stream.

    Returns:
      _SerializedDataStream: serialized data stream.

    Raises:
      IOError: if the stream cannot be opened.
    """
    data_stream = streams_cache.get(stream_number, None)
    if not data_stream:
      stream_name = u'{0:s}.{1:06d}'.format(stream_name_prefix, stream_number)
      if not self._HasStream(stream_name):
        raise IOError(u'No such stream: {0:s}'.format(stream_name))

      data_stream = _SerializedDataStream(
          self._zipfile, self._zipfile_path, stream_name)
      streams_cache[stream_number] = data_stream

    return data_stream

  def _GetSerializedDataOffsetTable(
      self, offset_tables_cache, offset_tables_lfu, stream_name_prefix,
      stream_number):
    """Retrieves the serialized data offset table.

    Args:
      offset_tables_cache (dict): offset tables cache.
      offset_tables_lfu (list[_SerializedDataOffsetTable]): least frequently
          used (LFU) offset tables.
      stream_name_prefix (str): stream name prefix.
      stream_number (str): number of the stream.

    Returns:
      _SerializedDataOffsetTable: serialized data offset table.

    Raises:
      IOError: if the stream cannot be opened.
    """
    offset_table = offset_tables_cache.get(stream_number, None)
    if not offset_table:
      stream_name = u'{0:s}.{1:06d}'.format(stream_name_prefix, stream_number)
      if not self._HasStream(stream_name):
        raise IOError(u'No such stream: {0:s}'.format(stream_name))

      offset_table = _SerializedDataOffsetTable(self._zipfile, stream_name)
      offset_table.Read()

      number_of_tables = len(offset_tables_cache)
      if number_of_tables >= self._MAXIMUM_NUMBER_OF_CACHED_TABLES:
        lfu_stream_number = self._event_offset_tables_lfu.pop()
        del offset_tables_cache[lfu_stream_number]

      offset_tables_cache[stream_number] = offset_table

    if stream_number in offset_tables_lfu:
      lfu_index = offset_tables_lfu.index(stream_number)
      offset_tables_lfu.pop(lfu_index)

    offset_tables_lfu.append(stream_number)

    return offset_table

  def _GetSerializedDataStreamNumbers(self, stream_name_prefix):
    """Retrieves the available serialized data stream numbers.

    Args:
      stream_name_prefix (str): stream name prefix.

    Returns:
      list[int]: available serialized data stream numbers sorted numerically.
    """
    stream_numbers = []
    for stream_name in self._zipfile.namelist():
      if not stream_name.startswith(stream_name_prefix):
        continue

      _, _, stream_number = stream_name.partition(u'.')
      try:
        stream_number = int(stream_number, 10)
        stream_numbers.append(stream_number)
      except ValueError:
        logging.error(
            u'Unable to determine stream number from stream: {0:s}'.format(
                stream_name))

    return sorted(stream_numbers)

  def _GetSerializedEventOffsetTable(self, stream_number):
    """Retrieves the serialized event stream offset table.

    Args:
      stream_number (int): number of the stream.

    Returns:
      _SerializedDataOffsetTable: serialized data offset table.

    Raises:
      IOError: if the stream cannot be opened.
    """
    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_index'
    else:
      stream_name_prefix = u'event_index'

    return self._GetSerializedDataOffsetTable(
        self._event_offset_tables, self._event_offset_tables_lfu,
        stream_name_prefix, stream_number)

  def _GetSerializedEventSourceOffsetTable(self, stream_number):
    """Retrieves the serialized event source stream offset table.

    Args:
      stream_number (int): number of the stream.

    Returns:
      _SerializedDataOffsetTable: serialized data offset table.

    Raises:
      IOError: if the stream cannot be opened.
    """
    return self._GetSerializedDataOffsetTable(
        self._event_source_offset_tables, self._event_source_offset_tables_lfu,
        u'event_source_index', stream_number)

  def _GetSerializedEventSourceStream(self, stream_number):
    """Retrieves the serialized event source stream.

    Args:
      stream_number (int): number of the stream.

    Returns:
      _SerializedDataStream: serialized data stream.

    Raises:
      IOError: if the stream cannot be opened.
    """
    return self._GetSerializedDataStream(
        self._event_streams, u'event_source_data', stream_number)

  def _GetSerializedEventStream(self, stream_number):
    """Retrieves the serialized event stream.

    Args:
      stream_number (int): number of the stream.

    Returns:
      _SerializedDataStream: serialized data stream.

    Raises:
      IOError: if the stream cannot be opened.
    """
    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_proto'
    else:
      stream_name_prefix = u'event_data'

    return self._GetSerializedDataStream(
        self._event_streams, stream_name_prefix, stream_number)

  def _GetSerializedEventSourceStreamNumbers(self):
    """Retrieves the available serialized event source stream numbers.

    Returns:
      list[int]: available serialized data stream numbers sorted numerically.
    """
    return self._GetSerializedDataStreamNumbers(u'event_source_data.')

  def _GetSerializedEventStreamNumbers(self):
    """Retrieves the available serialized event stream numbers.

    Returns:
      list[int]: available serialized data stream numbers sorted numerically.
    """
    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_proto.'
    else:
      stream_name_prefix = u'event_data.'

    return self._GetSerializedDataStreamNumbers(stream_name_prefix)

  def _GetSerializedEventTimestampTable(self, stream_number):
    """Retrieves the serialized event stream timestamp table.

    Args:
      stream_number (int): number of the stream.

    Returns:
      _SerializedDataTimestampTable: serialized data timestamp table.

    Raises:
      IOError: if the stream cannot be opened.
    """
    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_timestamps'
    else:
      stream_name_prefix = u'event_timestamps'

    timestamp_table = self._event_timestamp_tables.get(stream_number, None)
    if not timestamp_table:
      stream_name = u'{0:s}.{1:06d}'.format(stream_name_prefix, stream_number)
      timestamp_table = _SerializedDataTimestampTable(
          self._zipfile, stream_name)
      timestamp_table.Read()

      number_of_tables = len(self._event_timestamp_tables)
      if number_of_tables >= self._MAXIMUM_NUMBER_OF_CACHED_TABLES:
        lfu_stream_number = self._event_timestamp_tables_lfu.pop()
        del self._event_timestamp_tables[lfu_stream_number]

      self._event_timestamp_tables[stream_number] = timestamp_table

    if stream_number in self._event_timestamp_tables_lfu:
      lfu_index = self._event_timestamp_tables_lfu.index(stream_number)
      self._event_timestamp_tables_lfu.pop(lfu_index)

    self._event_timestamp_tables_lfu.append(stream_number)

    return timestamp_table

  def _GetStreamNames(self):
    """Retrieves the stream names.

    Yields:
      str: stream name.
    """
    if self._zipfile:
      for stream_name in self._zipfile.namelist():
        yield stream_name

  def _GetSortedEvent(self, time_range=None):
    """Retrieves the events in increasing chronological order.

    Args:
      time_range (Optional[TimeRange]): time range used to filter events
          that fall in a specific period.

    Returns:
      EventObject: event.
    """
    if not self._event_heap:
      self._InitializeMergeBuffer(time_range=time_range)
      if not self._event_heap:
        return

    event, stream_number = self._event_heap.PopEvent()
    if not event:
      return

    # Stop as soon as we hit the upper bound.
    if time_range and event.timestamp > time_range.end_timestamp:
      return

    next_event = self._GetEventObject(stream_number)
    if next_event:
      self._event_heap.PushEvent(
          next_event, stream_number, event.store_index)

      reference_timestamp = next_event.timestamp
      while next_event.timestamp == reference_timestamp:
        next_event = self._GetEventObject(stream_number)
        if not next_event:
          break

        self._event_heap.PushEvent(
            next_event, stream_number, event.store_index)

    event.tag = self._ReadEventTagByIdentifier(
        event.store_number, event.store_index, event.uuid)

    return event

  def _HasStream(self, stream_name):
    """Determines if the ZIP file contains a specific stream.

    Args:
      stream_name (str): name of the stream.

    Returns:
      bool: True if the ZIP file contains the stream.
    """
    try:
      file_object = self._zipfile.open(stream_name, 'r')
    except KeyError:
      return False

    file_object.close()
    return True

  def _OpenRead(self):
    """Opens the storage file for reading."""
    has_storage_metadata = self._ReadStorageMetadata()
    if not has_storage_metadata:
      # TODO: remove serializer.txt stream support in favor
      # of storage metatdata.
      if self._read_only:
        logging.warning(u'Storage file does not contain a metadata stream.')

      stored_serialization_format = self._ReadSerializerStream()
      if stored_serialization_format:
        self.format_version = self._LEGACY_FORMAT_VERSION

        self.serialization_format = stored_serialization_format

    if self.serialization_format != definitions.SERIALIZER_FORMAT_JSON:
      raise IOError(u'Unsupported serialization format: {0:s}'.format(
          self.serialization_format))

    self._serializer = json_serializer.JSONAttributeContainerSerializer

    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_proto.'
    else:
      stream_name_prefix = u'event_data.'

    self._error_stream_number = self._GetLastStreamNumber(u'error_data.')
    self._event_stream_number = self._GetLastStreamNumber(stream_name_prefix)
    self._event_source_stream_number = self._GetLastStreamNumber(
        u'event_source_data.')

    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_tagging.'
    else:
      stream_name_prefix = u'event_tag_data.'

    self._event_tag_stream_number = self._GetLastStreamNumber(
        stream_name_prefix)

    last_session_start = self._GetLastStreamNumber(u'session_start.')
    last_session_completion = self._GetLastStreamNumber(u'session_completion.')

    # TODO: handle open sessions.
    if last_session_start != last_session_completion:
      logging.warning(u'Detected unclosed session.')

    self._last_session = last_session_completion

    last_task_start = self._GetLastStreamNumber(u'task_start.')
    last_task_completion = self._GetLastStreamNumber(u'task_completion.')

    # TODO: handle open tasks.
    if last_task_start != last_task_completion:
      logging.warning(u'Detected unclosed task.')

    self._last_task = last_task_completion

  def _OpenStream(self, stream_name, access_mode='r'):
    """Opens a stream.

    Args:
      stream_name (str): name of the stream.
      access_mode (Optional[str]): access mode.

    Returns:
      zipfile.ZipExtFile: stream file-like object or None.
    """
    try:
      return self._zipfile.open(stream_name, mode=access_mode)
    except KeyError:
      return

  def _OpenWrite(self):
    """Opens the storage file for writing."""
    logging.debug(u'Writing to ZIP file with buffer size: {0:d}'.format(
        self._maximum_buffer_size))

    if self._event_stream_number == 1:
      self._WriteStorageMetadata()

  def _OpenZIPFile(self, path, read_only):
    """Opens the ZIP file.

    Args:
      path (str): path of the ZIP file.
      read_only (bool): True if the file should be opened in read-only mode.

    Raises:
      IOError: if the ZIP file is already opened or if the ZIP file cannot
               be opened.
    """
    if self._zipfile:
      raise IOError(u'ZIP file already opened.')

    if read_only:
      access_mode = 'r'

      zipfile_path = path
    else:
      access_mode = 'a'

      # Create a temporary directory to prevent multiple ZIP storage
      # files in the same directory conflicting with eachother.
      directory_name = os.path.dirname(path)
      basename = os.path.basename(path)
      directory_name = tempfile.mkdtemp(dir=directory_name)
      zipfile_path = os.path.join(directory_name, basename)

      if os.path.exists(path):
        os.rename(path, zipfile_path)

    try:
      self._zipfile = zipfile.ZipFile(
          zipfile_path, mode=access_mode, compression=zipfile.ZIP_DEFLATED,
          allowZip64=True)
      self._zipfile_path = zipfile_path

    except zipfile.BadZipfile as exception:
      raise IOError(u'Unable to open ZIP file: {0:s} with error: {1:s}'.format(
          zipfile_path, exception))

    self._is_open = True
    self._path = path
    self._read_only = read_only

  def _ReadAttributeContainer(self, container_data, container_type):
    """Reads an attribute container.

    Args:
      container_data (bytes): serialized attribute container data.
      container_type (str): attribute container type.

    Returns:
      AttributeContainer: attribute container or None.
    """
    if not container_data:
      return

    if self._serializers_profiler:
      self._serializers_profiler.StartTiming(container_type)

    attribute_container = self._serializer.ReadSerialized(container_data)

    if self._serializers_profiler:
      self._serializers_profiler.StopTiming(container_type)

    return attribute_container

  def _ReadAttributeContainerFromStreamEntry(self, data_stream, container_type):
    """Reads an attribute container entry from a data stream.

    Args:
      data_stream (_SerializedDataStream): data stream.
      container_type (str): attribute container type.

    Returns:
      AttributeContainer: attribute container or None.
    """
    entry_data = data_stream.ReadEntry()
    return self._ReadAttributeContainer(entry_data, container_type)

  def _ReadAttributeContainersFromStream(self, data_stream, container_type):
    """Reads attribute containers from a data stream.

    Args:
      data_stream (_SerializedDataStream): data stream.
      container_type (str): attribute container type.

    Yields:
      AttributeContainer: attribute container.
    """
    attribute_container = self._ReadAttributeContainerFromStreamEntry(
        data_stream, container_type)

    while attribute_container:
      yield attribute_container

      attribute_container = self._ReadAttributeContainerFromStreamEntry(
          data_stream, container_type)

  def _ReadEventTagByIdentifier(self, store_number, entry_index, uuid):
    """Reads an event tag by identifier.

    Args:
      store_number (int): store number.
      entry_index (int): serialized data stream entry index.
      uuid (str): event identifier formatted as an UUID.

    Returns:
      EventTag: event tag or None.

    Raises:
      IOError: if the event tag data stream cannot be opened.
    """
    tag_index_value = self._GetEventTagIndexValue(
        store_number, entry_index, uuid)
    if tag_index_value is None:
      return

    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_tagging'
    else:
      stream_name_prefix = u'event_tag_data'

    stream_name = u'{0:s}.{1:06d}'.format(
        stream_name_prefix, tag_index_value.store_number)
    if not self._HasStream(stream_name):
      raise IOError(u'No such stream: {0:s}'.format(stream_name))

    data_stream = _SerializedDataStream(
        self._zipfile, self._zipfile_path, stream_name)
    data_stream.SeekEntryAtOffset(entry_index, tag_index_value.store_index)

    return self._ReadAttributeContainerFromStreamEntry(data_stream, u'event')

  def _ReadSerializerStream(self):
    """Reads the serializer stream.

    Note that the serializer stream has been deprecated in format version
    20160501 in favor of the the store metadata stream.

    Returns:
      str: stored serializer format.

    Raises:
      ValueError: if the serializer format is not supported.
    """
    stream_name = u'serializer.txt'
    if not self._HasStream(stream_name):
      return

    serialization_format = self._ReadStream(stream_name)
    if serialization_format != definitions.SERIALIZER_FORMAT_JSON:
      raise ValueError(
          u'Unsupported stored serialization format: {0:s}'.format(
              serialization_format))

    return serialization_format

  def _ReadStorageMetadata(self):
    """Reads the storage metadata.

    Returns:
      bool: True if the storage metadata was read.

    Raises:
      IOError: if the format version or the serializer format is not supported.
    """
    stream_name = u'metadata.txt'
    if not self._HasStream(stream_name):
      return False

    storage_metadata_reader = _StorageMetadataReader()
    stream_data = self._ReadStream(stream_name)
    storage_metadata = storage_metadata_reader.Read(stream_data)

    if not storage_metadata.format_version:
      raise IOError(u'Missing format version.')

    if storage_metadata.format_version < self._COMPATIBLE_FORMAT_VERSION:
      raise IOError(
          u'Format version: {0:d} is too old and no longer supported.'.format(
              storage_metadata.format_version))

    if storage_metadata.format_version > self._FORMAT_VERSION:
      raise IOError(
          u'Format version: {0:d} is too new and not yet supported.'.format(
              storage_metadata.format_version))

    serialization_format = storage_metadata.serialization_format
    if serialization_format != definitions.SERIALIZER_FORMAT_JSON:
      raise IOError(u'Unsupported serialization format: {0:s}'.format(
          serialization_format))

    if storage_metadata.storage_type not in definitions.STORAGE_TYPES:
      raise IOError(u'Unsupported storage type: {0:s}'.format(
          storage_metadata.storage_type))

    self.format_version = storage_metadata.format_version
    self.serialization_format = serialization_format
    self.storage_type = storage_metadata.storage_type

    return True

  def _ReadStream(self, stream_name):
    """Reads data from a stream.

    Args:
      stream_name (str): name of the stream.

    Returns:
      bytes: data of the stream.
    """
    file_object = self._OpenStream(stream_name)
    if not file_object:
      return b''

    try:
      data = file_object.read()
    finally:
      file_object.close()

    return data

  def _WriteAttributeContainer(self, attribute_container):
    """Writes an attribute container.

    Args:
      attribute_container (AttributeContainer): attribute container.

    Returns:
      bytes: serialized attribute container.

    Raises:
      IOError: if the attribute container cannot be serialized.
    """
    if self._serializers_profiler:
      self._serializers_profiler.StartTiming(
          attribute_container.CONTAINER_TYPE)

    try:
      attribute_container_data = self._serializer.WriteSerialized(
          attribute_container)
      if not attribute_container_data:
        raise IOError(
            u'Unable to serialize attribute container: {0:s}.'.format(
                attribute_container.CONTAINER_TYPE))

    finally:
      if self._serializers_profiler:
        self._serializers_profiler.StopTiming(
            attribute_container.CONTAINER_TYPE)

    return attribute_container_data

  def _WriteAttributeContainersHeap(
      self, attribute_containers_list, stream_name_prefix, stream_number):
    """Writes the contents of an attribute containers heap.

    Args:
      attribute_containers_list(_AttributeContainersList): attribute
          containers list.
      stream_name_prefix(str): stream name prefix.
      stream_number(int): stream number.
    """
    stream_name = u'{0:s}_index.{1:06d}'.format(
        stream_name_prefix, stream_number)
    offset_table = _SerializedDataOffsetTable(self._zipfile, stream_name)

    stream_name = u'{0:s}_data.{1:06d}'.format(
        stream_name_prefix, stream_number)

    data_stream = _SerializedDataStream(
        self._zipfile, self._zipfile_path, stream_name)
    entry_data_offset = data_stream.WriteInitialize()

    try:
      for _ in range(attribute_containers_list.number_of_attribute_containers):
        entry_data = attribute_containers_list.PopAttributeContainer()

        offset_table.AddOffset(entry_data_offset)

        entry_data_offset = data_stream.WriteEntry(entry_data)

    except:
      data_stream.WriteAbort()
      raise

    offset_table.Write()
    data_stream.WriteFinalize()

  def _WriteSerializedErrors(self):
    """Writes the buffered serialized errors."""
    if not self._errors_list.data_size:
      return

    if self._serializers_profiler:
      self._serializers_profiler.StartTiming(u'write')

    try:
      self._WriteAttributeContainersHeap(
          self._errors_list, u'error',
          self._error_stream_number)

    finally:
      if self._serializers_profiler:
        self._serializers_profiler.StopTiming(u'write')

    self._error_stream_number += 1
    self._errors_list.Empty()

  def _WriteSerializedEvents(self):
    """Writes the serialized events."""
    if not self._serialized_events_heap.data_size:
      return

    stream_name = u'event_index.{0:06d}'.format(self._event_stream_number)
    offset_table = _SerializedDataOffsetTable(self._zipfile, stream_name)

    stream_name = u'event_timestamps.{0:06d}'.format(self._event_stream_number)
    timestamp_table = _SerializedDataTimestampTable(self._zipfile, stream_name)

    if self._serializers_profiler:
      self._serializers_profiler.StartTiming(u'write')

    stream_name = u'event_data.{0:06d}'.format(self._event_stream_number)
    data_stream = _SerializedDataStream(
        self._zipfile, self._zipfile_path, stream_name)
    entry_data_offset = data_stream.WriteInitialize()

    try:
      for _ in range(self._serialized_events_heap.number_of_events):
        timestamp, entry_data = self._serialized_events_heap.PopEvent()

        timestamp_table.AddTimestamp(timestamp)
        offset_table.AddOffset(entry_data_offset)

        entry_data_offset = data_stream.WriteEntry(entry_data)

    except:
      data_stream.WriteAbort()

      if self._serializers_profiler:
        self._serializers_profiler.StopTiming(u'write')

      raise

    offset_table.Write()
    data_stream.WriteFinalize()
    timestamp_table.Write()

    if self._serializers_profiler:
      self._serializers_profiler.StopTiming(u'write')

    self._event_stream_number += 1
    self._serialized_events_heap.Empty()

  def _WriteSerializedEventSources(self):
    """Writes the serialized event sources."""
    if not self._event_sources_list.data_size:
      return

    if self._serializers_profiler:
      self._serializers_profiler.StartTiming(u'write')

    try:
      self._WriteAttributeContainersHeap(
          self._event_sources_list, u'event_source',
          self._event_source_stream_number)

    finally:
      if self._serializers_profiler:
        self._serializers_profiler.StopTiming(u'write')

    self._event_source_stream_number += 1
    self._event_sources_list.Empty()

  def _WriteSerializedEventTags(self):
    """Writes the serialized event tags."""
    if not self._serialized_event_tags_size:
      return

    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_tag_index'
    else:
      stream_name_prefix = u'event_tag_index'

    stream_name = u'{0:s}.{1:06d}'.format(
        stream_name_prefix, self._event_tag_stream_number)
    event_tag_index_table = _SerializedEventTagIndexTable(
        self._zipfile, stream_name)

    if self._serializers_profiler:
      self._serializers_profiler.StartTiming(u'write')

    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_tagging'
    else:
      stream_name_prefix = u'event_tag_data'

    stream_name = u'{0:s}.{1:06d}'.format(
        stream_name_prefix, self._event_tag_stream_number)
    data_stream = _SerializedDataStream(
        self._zipfile, self._zipfile_path, stream_name)
    entry_data_offset = data_stream.WriteInitialize()

    try:
      for _ in range(len(self._serialized_event_tags)):
        heap_values = heapq.heappop(self._serialized_event_tags)
        store_number, store_index, event_uuid, entry_data = heap_values

        if event_uuid:
          tag_type = _EventTagIndexValue.TAG_TYPE_UUID
        else:
          tag_type = _EventTagIndexValue.TAG_TYPE_NUMERIC

        event_tag_index_table.AddEventTagIndex(
            tag_type, entry_data_offset, event_uuid=event_uuid,
            store_number=store_number, store_index=store_index)

        entry_data_offset = data_stream.WriteEntry(entry_data)

    except:
      data_stream.WriteAbort()

      if self._serializers_profiler:
        self._serializers_profiler.StopTiming(u'write')

      raise

    event_tag_index_table.Write()
    data_stream.WriteFinalize()

    if self._serializers_profiler:
      self._serializers_profiler.StopTiming(u'write')

    self._event_tag_stream_number += 1
    self._serialized_event_tags_size = 0
    self._serialized_event_tags = []

  def _WriteSessionCompletion(self, session_completion):
    """Writes a session completion attribute container.

    Args:
      session_completion (SessionCompletion): session completion attribute
          container.

    Raises:
      IOError: if the storage type does not support writing a session
               completion or the session completion already exists.
    """
    if self.storage_type != definitions.STORAGE_TYPE_SESSION:
      raise IOError(u'Session completion not supported by storage type.')

    stream_name = u'session_completion.{0:06d}'.format(self._last_session)
    if self._HasStream(stream_name):
      raise IOError(u'Session completion: {0:06d} already exists.'.format(
          self._last_session))

    session_completion_data = self._WriteAttributeContainer(session_completion)

    data_stream = _SerializedDataStream(
        self._zipfile, self._zipfile_path, stream_name)
    data_stream.WriteInitialize()
    data_stream.WriteEntry(session_completion_data)
    data_stream.WriteFinalize()

  def _WriteSessionStart(self, session_start):
    """Writes a session start attribute container

    Args:
      session_start (SessionStart): session start attribute container.

    Raises:
      IOError: if the storage type does not support writing a session
               start or the session start already exists.
    """
    if self.storage_type != definitions.STORAGE_TYPE_SESSION:
      raise IOError(u'Session completion not supported by storage type.')

    stream_name = u'session_start.{0:06d}'.format(self._last_session)
    if self._HasStream(stream_name):
      raise IOError(u'Session start: {0:06d} already exists.'.format(
          self._last_session))

    session_start_data = self._WriteAttributeContainer(session_start)

    data_stream = _SerializedDataStream(
        self._zipfile, self._zipfile_path, stream_name)
    data_stream.WriteInitialize()
    data_stream.WriteEntry(session_start_data)
    data_stream.WriteFinalize()

  def _WriteStorageMetadata(self):
    """Writes the storage metadata."""
    stream_name = u'metadata.txt'
    if self._HasStream(stream_name):
      return

    stream_data = (
        b'[plaso_storage_file]\n'
        b'format_version: {0:d}\n'
        b'serialization_format: {1:s}\n'
        b'storage_type: {2:s}\n'
        b'\n').format(
            self._FORMAT_VERSION, self.serialization_format, self.storage_type)

    self._WriteStream(stream_name, stream_data)

  def _WriteStream(self, stream_name, stream_data):
    """Writes data to a stream.

    Args:
      stream_name (str): name of the stream.
      stream_data (bytes): data of the steam.
    """
    # TODO: this can raise an IOError e.g. "Stale NFS file handle".
    # Determine if this be handled more error resiliently.

    # Prevent zipfile from generating "UserWarning: Duplicate name:".
    with warnings.catch_warnings():
      warnings.simplefilter(u'ignore')
      self._zipfile.writestr(stream_name, stream_data)

  def _WriteTaskCompletion(self, task_completion):
    """Writes a task completion attribute container.

    Args:
      task_completion (TaskCompletion): task completion attribute container.

    Raises:
      IOError: if the storage type does not support writing a task
               completion or the task completion already exists.
    """
    if self.storage_type != definitions.STORAGE_TYPE_TASK:
      raise IOError(u'Task completion not supported by storage type.')

    stream_name = u'task_completion.{0:06d}'.format(self._last_task)
    if self._HasStream(stream_name):
      raise IOError(u'Task completion: {0:06d} already exists.'.format(
          self._last_task))

    task_completion_data = self._WriteAttributeContainer(task_completion)

    data_stream = _SerializedDataStream(
        self._zipfile, self._zipfile_path, stream_name)
    data_stream.WriteInitialize()
    data_stream.WriteEntry(task_completion_data)
    data_stream.WriteFinalize()

  def _WriteTaskStart(self, task_start):
    """Writes a task start attribute container.

    Args:
      task_start (TaskStart): task start attribute container.

    Raises:
      IOError: if the storage type does not support writing a task start
               or the task start already exists.
    """
    if self.storage_type != definitions.STORAGE_TYPE_TASK:
      raise IOError(u'Task start not supported by storage type.')

    stream_name = u'task_start.{0:06d}'.format(self._last_task)
    if self._HasStream(stream_name):
      raise IOError(u'Task start: {0:06d} already exists.'.format(
          self._last_task))

    task_start_data = self._WriteAttributeContainer(task_start)

    data_stream = _SerializedDataStream(
        self._zipfile, self._zipfile_path, stream_name)
    data_stream.WriteInitialize()
    data_stream.WriteEntry(task_start_data)
    data_stream.WriteFinalize()

  def AddAnalysisReport(self, analysis_report):
    """Adds an analysis report.

    Args:
      analysis_report (AnalysisReport): analysis report.

    Raises:
      IOError: when the storage file is closed or read-only.
    """
    if not self._is_open:
      raise IOError(u'Unable to write to closed storage file.')

    if self._read_only:
      raise IOError(u'Unable to write to read-only storage file.')

    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_report.'
    else:
      stream_name_prefix = u'analysis_report_data.'

    report_number = 1
    for name in self._GetStreamNames():
      if name.startswith(stream_name_prefix):

        _, _, number_string = name.partition(u'.')
        try:
          number = int(number_string, 10)
        except ValueError:
          logging.error(u'Unable to read in report number.')
          number = 0
        if number >= report_number:
          report_number = number + 1

    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_report'
    else:
      stream_name_prefix = u'analysis_report_data'

    stream_name = u'{0:s}.{1:06}'.format(stream_name_prefix, report_number)

    if self._serializers_profiler:
      self._serializers_profiler.StartTiming(u'analysis_report')

    serialized_report = self._serializer.WriteSerialized(analysis_report)

    if self._serializers_profiler:
      self._serializers_profiler.StopTiming(u'analysis_report')

    if self.format_version <= 20160501:
      self._WriteStream(stream_name, serialized_report)
    else:
      data_stream = _SerializedDataStream(
          self._zipfile, self._zipfile_path, stream_name)
      data_stream.WriteInitialize()
      data_stream.WriteEntry(serialized_report)
      data_stream.WriteFinalize()

  def AddError(self, error):
    """Adds an error.

    Args:
      error (ExtractionError): error.

    Raises:
      IOError: when the storage file is closed or read-only or
               if the error cannot be serialized.
    """
    error.storage_session = self._last_session

    # We try to serialize the error first, so we can skip some
    # processing if it is invalid.
    error_data = self._WriteAttributeContainer(error)

    self._errors_list.PushAttributeContainer(error_data)

    if self._errors_list.data_size > self._maximum_buffer_size:
      self._WriteSerializedErrors()

  def AddEvent(self, event):
    """Adds an event.

    Args:
      event (EventObject): event.

    Raises:
      IOError: when the storage file is closed or read-only or
               if the event cannot be serialized.
    """
    if not self._is_open:
      raise IOError(u'Unable to write to closed storage file.')

    if self._read_only:
      raise IOError(u'Unable to write to read-only storage file.')

    # We try to serialize the event first, so we can skip some
    # processing if it is invalid.
    event_data = self._WriteAttributeContainer(event)

    self._serialized_events_heap.PushEvent(event.timestamp, event_data)

    if self._serialized_events_heap.data_size > self._maximum_buffer_size:
      self._WriteSerializedEvents()

  def AddEventSource(self, event_source):
    """Adds an event source.

    Args:
      event_source (EventSource): event source.

    Raises:
      IOError: when the storage file is closed or read-only or
               if the event source cannot be serialized.
    """
    if not self._is_open:
      raise IOError(u'Unable to write to closed storage file.')

    if self._read_only:
      raise IOError(u'Unable to write to read-only storage file.')

    event_source.storage_session = self._last_session

    # We try to serialize the event source first, so we can skip some
    # processing if it is invalid.
    event_source_data = self._WriteAttributeContainer(event_source)

    self._event_sources_list.PushAttributeContainer(event_source_data)

    if self._event_sources_list.data_size > self._maximum_buffer_size:
      self._WriteSerializedEventSources()

  def AddEventTag(self, event_tag):
    """Adds an event tag.

    Args:
      event_tag (EventTag): event tag.

    Raises:
      IOError: when the storage file is closed or read-only or
               if the event tag cannot be serialized.
    """
    if not self._is_open:
      raise IOError(u'Unable to write to closed storage file.')

    if self._read_only:
      raise IOError(u'Unable to write to read-only storage file.')

    # We try to serialize the event tag first, so we can skip some
    # processing if it is invalid.
    event_tag_data = self._WriteAttributeContainer(event_tag)

    event_uuid = getattr(event_tag, u'event_uuid', None)
    store_index = getattr(event_tag, u'store_index', None)
    store_number = getattr(event_tag, u'store_number', None)

    heap_values = (store_number, store_index, event_uuid, event_tag_data)
    heapq.heappush(self._serialized_event_tags, heap_values)
    self._serialized_event_tags_size += len(event_tag_data)

    if self._serialized_event_tags_size > self._maximum_buffer_size:
      self._WriteSerializedEventSources()

  def AddEventTags(self, event_tags):
    """Adds event tags.

    Args:
      event_tag (list[EventTag]): event tags.

    Raises:
      IOError: when the storage file is closed or read-only or
               if the stream cannot be opened.
    """
    if not self._is_open:
      raise IOError(u'Unable to write to closed storage file.')

    if self._read_only:
      raise IOError(u'Unable to write to read-only storage file.')

    if self._event_tag_index is None:
      self._BuildTagIndex()

    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_tagging'
    else:
      stream_name_prefix = u'event_tag_data'

    for event_tag in event_tags:
      tag_index_value = self._event_tag_index.get(event_tag.string_key, None)

      # This particular event has already been tagged on a previous occasion,
      # we need to make sure we are appending to that particular event tag.
      if tag_index_value is not None:
        stream_name = u'{0:s}.{1:06d}'.format(
            stream_name_prefix, tag_index_value.store_number)

        if not self._HasStream(stream_name):
          raise IOError(u'No such stream: {0:s}'.format(stream_name))

        data_stream = _SerializedDataStream(
            self._zipfile, self._zipfile_path, stream_name)
        # TODO: replace 0 by the actual event tag entry index.
        # This is for code consistency rather then a functional purpose.
        data_stream.SeekEntryAtOffset(0, tag_index_value.offset)

        # TODO: if stored_event_tag is cached make sure to update cache
        # after write.
        stored_event_tag = self._ReadAttributeContainerFromStreamEntry(
            data_stream, u'event_tag')
        if not stored_event_tag:
          continue

        event_tag.AddComment(stored_event_tag.comment)
        event_tag.AddLabels(stored_event_tag.labels)

      self.AddEventTag(event_tag)

    self._WriteSerializedEventTags()

    # TODO: Update the tags that have changed in the index instead
    # of flushing the index.

    # If we already built a list of tag in memory we need to clear that
    # since the tags have changed.
    if self._event_tag_index is not None:
      self._event_tag_index = None

  def Close(self):
    """Closes the storage file.

    Buffered attribute containers are written to file.

    Raises:
      IOError: when trying to write to a closed storage file or
               if the event source cannot be serialized.
    """
    if not self._is_open:
      raise IOError(u'Unable to flush a closed storage file.')

    if not self._read_only:
      self.Flush()

    if self._serializers_profiler:
      self._serializers_profiler.Write()

    self._event_streams = {}
    self._event_offset_tables = {}
    self._event_offset_tables_lfu = []
    self._event_timestamp_tables = {}
    self._event_timestamp_tables_lfu = []

    self._zipfile.close()
    self._zipfile = None
    self._is_open = False

    if self._path != self._zipfile_path and os.path.exists(self._zipfile_path):
      os.rename(self._zipfile_path, self._path)
      directory_name = os.path.dirname(self._zipfile_path)
      os.rmdir(directory_name)

    self._path = None
    self._zipfile_path = None

  def Flush(self):
    """Forces the serialized attribute containers to be written to file.

    Raises:
      IOError: when trying to write to a closed storage file or
               if the event source cannot be serialized.
    """
    if not self._is_open:
      raise IOError(u'Unable to flush a closed storage file.')

    if not self._read_only:
      self._WriteSerializedEventSources()
      self._WriteSerializedEvents()
      self._WriteSerializedEventTags()
      self._WriteSerializedErrors()

  def GetAnalysisReports(self):
    """Retrieves the analysis reports.

    Yields:
      AnalysisReport: analysis report.

    Raises:
      IOError: if the stream cannot be opened.
    """
    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_report.'
    else:
      stream_name_prefix = u'analysis_report_data.'

    for stream_name in self._GetStreamNames():
      if not stream_name.startswith(stream_name_prefix):
        continue

      if self.format_version <= 20160501:
        file_object = self._OpenStream(stream_name)
        if file_object is None:
          raise IOError(u'Unable to open stream: {0:s}'.format(stream_name))

        report_string = file_object.read(self._MAXIMUM_SERIALIZED_REPORT_SIZE)
        yield self._serializer.ReadSerialized(report_string)

      else:
        data_stream = _SerializedDataStream(
            self._zipfile, self._zipfile_path, stream_name)

        for analysis_report in self._ReadAttributeContainersFromStream(
            data_stream, u'analysis_report'):
          yield analysis_report

  def GetErrors(self):
    """Retrieves the errors.

    Yields:
      ExtractionError: error.

    Raises:
      IOError: if a stream is missing.
    """
    for stream_number in range(1, self._error_stream_number):
      stream_name = u'error_data.{0:06}'.format(stream_number)
      if not self._HasStream(stream_name):
        raise IOError(u'No such stream: {0:s}'.format(stream_name))

      data_stream = _SerializedDataStream(
          self._zipfile, self._zipfile_path, stream_name)

      for error in self._ReadAttributeContainersFromStream(
          data_stream, u'error'):
        yield error

  def GetEvents(self, time_range=None):
    """Retrieves the events in increasing chronological order.

    Args:
      time_range (Optional[TimeRange]): time range used to filter events
          that fall in a specific period.

    Yields:
      EventObject: event.
    """
    event = self._GetSortedEvent(time_range=time_range)
    while event:
      yield event
      event = self._GetSortedEvent(time_range=time_range)

  def GetEventSourceByIndex(self, index):
    """Retrieves a specific event source.

    Args:
      index (int): event source index.

    Returns:
      EventSource: event source.

    Raises:
      IOError: if a stream is missing.
    """
    for stream_number in range(1, self._event_source_stream_number):
      offset_table = self._GetSerializedEventSourceOffsetTable(stream_number)
      if index >= offset_table.number_of_offsets:
        index -= offset_table.number_of_offsets
        continue

      stream_name = u'event_source_data.{0:06}'.format(stream_number)
      if not self._HasStream(stream_name):
        raise IOError(u'No such stream: {0:s}'.format(stream_name))

      data_stream = _SerializedDataStream(
          self._zipfile, self._zipfile_path, stream_name)

      stream_offset = offset_table.GetOffset(index)
      data_stream.SeekEntryAtOffset(index, stream_offset)

      return self._ReadAttributeContainerFromStreamEntry(
          data_stream, u'event_source')

    entry_data = self._event_sources_list.GetAttributeContainerByIndex(index)
    return self._ReadAttributeContainer(entry_data, u'event_source')

  def GetEventSources(self):
    """Retrieves the event sources.

    Yields:
      EventSource: event source.

    Raises:
      IOError: if a stream is missing.
    """
    for stream_number in range(1, self._event_source_stream_number):
      stream_name = u'event_source_data.{0:06}'.format(stream_number)
      if not self._HasStream(stream_name):
        raise IOError(u'No such stream: {0:s}'.format(stream_name))

      data_stream = _SerializedDataStream(
          self._zipfile, self._zipfile_path, stream_name)

      for event_source in self._ReadAttributeContainersFromStream(
          data_stream, u'event_source'):
        yield event_source

  def GetEventTags(self):
    """Retrieves the event tags.

    Yields:
      EventTag: event tag.

    Raises:
      IOError: if a stream is missing.
    """
    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_tagging'
    else:
      stream_name_prefix = u'event_tag_data'

    for stream_number in range(1, self._event_tag_stream_number):
      stream_name = u'{0:s}.{1:06}'.format(stream_name_prefix, stream_number)
      if not self._HasStream(stream_name):
        raise IOError(u'No such stream: {0:s}'.format(stream_name))

      data_stream = _SerializedDataStream(
          self._zipfile, self._zipfile_path, stream_name)

      for event_tag in self._ReadAttributeContainersFromStream(
          data_stream, u'event_tag'):
        yield event_tag

  def GetNumberOfEventSources(self):
    """Retrieves the number event sources.

    Returns:
      int: number of event sources.
    """
    number_of_event_sources = 0
    for stream_number in range(1, self._event_source_stream_number):
      offset_table = self._GetSerializedEventSourceOffsetTable(stream_number)
      number_of_event_sources += offset_table.number_of_offsets

    number_of_event_sources += (
        self._event_sources_list.number_of_attribute_containers)
    return number_of_event_sources

  def GetSessions(self):
    """Retrieves the sessions.

    Yields:
      Tuples of a session start (instance of SessionStart) and
      a session completion (instance of SessionCompletion) object.
      The session completion value can be None if not available.

    Raises:
      IOError: if a stream is missing.
    """
    if self.format_version <= 20160501:
      return

    for stream_number in range(1, self._last_session):
      stream_name = u'session_start.{0:06d}'.format(stream_number)
      if not self._HasStream(stream_name):
        raise IOError(u'No such stream: {0:s}'.format(stream_name))

      data_stream = _SerializedDataStream(
          self._zipfile, self._zipfile_path, stream_name)

      session_start = self._ReadAttributeContainerFromStreamEntry(
          data_stream, u'session_start')

      session_completion = None
      stream_name = u'session_completion.{0:06d}'.format(stream_number)
      if self._HasStream(stream_name):
        data_stream = _SerializedDataStream(
            self._zipfile, self._zipfile_path, stream_name)

        session_completion = self._ReadAttributeContainerFromStreamEntry(
            data_stream, u'session_completion')

        yield session_start, session_completion

  def HasAnalysisReports(self):
    """Determines if a storage contains analysis reports.

    Returns:
      bool: True if the storage contains analysis reports.
    """
    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_report.'
    else:
      stream_name_prefix = u'analysis_report_data.'

    for name in self._GetStreamNames():
      if name.startswith(stream_name_prefix):
        return True

    return False

  def HasEventTags(self):
    """Determines if a storage contains event tags.

    Returns:
      bool: True if the storage contains event tags.
    """
    if self.format_version <= 20160501:
      stream_name_prefix = u'plaso_tagging.'
    else:
      stream_name_prefix = u'event_tag_data.'

    for name in self._GetStreamNames():
      if name.startswith(stream_name_prefix):
        return True

    return False

  def Open(self, path=None, read_only=True, **unused_kwargs):
    """Opens the storage file.

    Args:
      path (Optional[str]): path of the storage file.
      read_only (Optional[bool]): True if the file should be opened in
          read-only mode.

    Raises:
      ValueError: if path is missing.
    """
    if not path:
      raise ValueError(u'Missing path.')

    self._OpenZIPFile(path, read_only)
    self._OpenRead()

    if not read_only:
      self._OpenWrite()

  def WriteSessionCompletion(self, session_completion):
    """Writes session completion information.

    Args:
      session_completion (SessionCompletion): session completion information.

    Raises:
      IOError: when the storage file is closed or read-only.
    """
    if not self._is_open:
      raise IOError(u'Unable to write to closed storage file.')

    if self._read_only:
      raise IOError(u'Unable to write to read-only storage file.')

    if self.format_version < 20160511:
      return

    self.Flush()

    self._WriteSessionCompletion(session_completion)
    self._last_session += 1

  def WriteSessionStart(self, session_start):
    """Writes session start information.

    Args:
      session_start (SessionStart): session start information.

    Raises:
      IOError: when the storage file is closed or read-only.
    """
    if not self._is_open:
      raise IOError(u'Unable to write to closed storage file.')

    if self._read_only:
      raise IOError(u'Unable to write to read-only storage file.')

    if self.format_version < 20160511:
      return

    self._WriteSessionStart(session_start)

  def WriteTaskCompletion(self, session_completion):
    """Writes task completion information.

    Args:
      session_completion (TaskCompletion): session completion information.

    Raises:
      IOError: when the storage file is closed or read-only.
    """
    if not self._is_open:
      raise IOError(u'Unable to write to closed storage file.')

    if self._read_only:
      raise IOError(u'Unable to write to read-only storage file.')

    if self.format_version < 20160525:
      return

    self.Flush()

    self._WriteTaskCompletion(session_completion)

  def WriteTaskStart(self, session_start):
    """Writes task start information.

    Args:
      session_start (TaskStart): session start information.

    Raises:
      IOError: when the storage file is closed or read-only.
    """
    if not self._is_open:
      raise IOError(u'Unable to write to closed storage file.')

    if self._read_only:
      raise IOError(u'Unable to write to read-only storage file.')

    if self.format_version < 20160525:
      return

    self._WriteTaskStart(session_start)


# TODO: remove StorageFile.
class StorageFile(ZIPStorageFile):
  """Class that defines the ZIP-based storage file."""

  def __init__(
      self, output_file, buffer_size=0, read_only=False,
      storage_type=definitions.STORAGE_TYPE_SESSION):
    """Initializes the storage file.

    Args:
      output_file: a string containing the name of the output file.
      buffer_size: optional integer containing the maximum size of
                   a single storage stream. A value of 0 indicates
                   the limit is _MAXIMUM_BUFFER_SIZE.
      read_only: optional boolean to indicate we are opening the storage file
                 for reading only.
      storage_type: optional string containing the storage type.

    Raises:
      IOError: if we open the file in read only mode and the file does
               not exist.
    """
    super(StorageFile, self).__init__(
        maximum_buffer_size=buffer_size, storage_type=storage_type)
    self._preprocess_object_serializer = (
        json_serializer.JSONPreprocessObjectSerializer)

    self.Open(path=output_file, read_only=read_only)

  def _ReadPreprocessObject(self, data_stream):
    """Reads a preprocessing object.

    Args:
      data_stream: the data stream object (instance of _SerializedDataStream).

    Returns:
      An preprocessing object (instance of PreprocessObject) or None if the
      preprocessing object cannot be read.
    """
    preprocess_data = data_stream.ReadEntry()
    if not preprocess_data:
      return

    if self._serializers_profiler:
      self._serializers_profiler.StartTiming(u'preprocess_object')

    try:
      preprocess_object = self._preprocess_object_serializer.ReadSerialized(
          preprocess_data)
    except errors.SerializationError as exception:
      logging.error(exception)
      preprocess_object = None

    if self._serializers_profiler:
      self._serializers_profiler.StopTiming(u'preprocess_object')

    return preprocess_object

  def GetStorageInformation(self):
    """Retrieves storage (preprocessing) information stored in the storage file.

    Returns:
      A list of preprocessing objects (instances of PreprocessObject)
      that contain the storage information.
    """
    stream_name = u'information.dump'
    if not self._HasStream(stream_name):
      return []

    data_stream = _SerializedDataStream(
        self._zipfile, self._zipfile_path, stream_name)

    information = []
    preprocess_object = self._ReadPreprocessObject(data_stream)
    while preprocess_object:
      information.append(preprocess_object)
      preprocess_object = self._ReadPreprocessObject(data_stream)

    return information

  def WritePreprocessObject(self, preprocess_object):
    """Writes a preprocess object.

    Args:
      preprocess_object: the preprocess object (instance of PreprocessObject).

    Raises:
      IOError: when the storage file is closed or read-only or
               if the stream cannot be opened.
    """
    if not self._is_open:
      raise IOError(u'Unable to write to closed storage file.')

    if self._read_only:
      raise IOError(u'Unable to write to read-only storage file.')

    stream_name = u'information.dump'
    existing_stream_data = self._ReadStream(stream_name)

    # Store information about store range for this particular
    # preprocessing object. This will determine which stores
    # this information is applicable for.
    if self._serializers_profiler:
      self._serializers_profiler.StartTiming(u'preprocess_object')

    preprocess_object_data = (
        self._preprocess_object_serializer.WriteSerialized(preprocess_object))

    if self._serializers_profiler:
      self._serializers_profiler.StopTiming(u'preprocess_object')

    # TODO: use _SerializedDataStream.
    preprocess_object_data_size = construct.ULInt32(u'size').build(
        len(preprocess_object_data))
    stream_data = b''.join([
        existing_stream_data, preprocess_object_data_size,
        preprocess_object_data])

    self._WriteStream(stream_name, stream_data)


class ZIPStorageFileReader(interface.StorageReader):
  """Class that implements the ZIP-based storage file reader."""

  def __init__(self, input_file):
    """Initializes a storage reader object.

    Args:
      input_file: a string containing the path to the output file.
    """
    super(ZIPStorageFileReader, self).__init__()
    self._storage_file = ZIPStorageFile()
    self._storage_file.Open(path=input_file)

  def Close(self):
    """Closes the storage reader."""
    if self._storage_file:
      self._storage_file.Close()
      self._storage_file = None

  def GetAnalysisReports(self):
    """Retrieves the analysis reports.

    Returns:
      A generator of analysis report objects (instances of AnalysisReport).
    """
    return self._storage_file.GetAnalysisReports()

  def GetErrors(self):
    """Retrieves the errors.

    Returns:
      A generator of error objects (instances of AnalysisError or
      ExtractionError).
    """
    return self._storage_file.GetErrors()

  def GetEvents(self, time_range=None):
    """Retrieves the events in increasing chronological order.

    Args:
      time_range (Optional[TimeRange]): time range used to filter events
          that fall in a specific period.

    Returns:
      EventObject: event.
    """
    return self._storage_file.GetEvents(time_range=time_range)

  def GetEventSources(self):
    """Retrieves the event sources.

    Returns:
      A generator of event source objects (instances of EventSourceObject).
    """
    return self._storage_file.GetEventSources()

  def GetEventTags(self):
    """Retrieves the event tags.

    Returns:
      A generator of event tag objects (instances of EventTagObject).
    """
    return self._storage_file.GetEventTags()


class ZIPStorageFileWriter(interface.StorageWriter):
  """Class that implements the ZIP-based storage file writer."""

  def __init__(
      self, session, output_file, buffer_size=0,
      storage_type=definitions.STORAGE_TYPE_SESSION, task=None):
    """Initializes a storage writer object.

    Args:
      session (Session): session the storage changes are part of.
      output_file (str): path to the output file.
      buffer_size (Optional[int]): estimated size of a protobuf file.
      storage_type (Optional[str]): storage type.
      task(Optional[Task]): task.
    """
    super(ZIPStorageFileWriter, self).__init__(
        session, storage_type=storage_type, task=task)
    self._buffer_size = buffer_size
    self._event_tags = []
    self._merge_task_storage_path = u''
    self._output_file = output_file
    self._storage_file = None
    self._task_storage_path = None

  def _UpdateCounters(self, event):
    """Updates the counters.

    Args:
      event: an event (instance of EventObject).
    """
    self._session.parsers_counter[u'total'] += 1

    parser_name = getattr(event, u'parser', u'N/A')
    self._session.parsers_counter[parser_name] += 1

    # TODO: remove plugin, add parser chain.
    if hasattr(event, u'plugin'):
      plugin_name = getattr(event, u'plugin', u'N/A')
      self._session.parser_plugins_counter[plugin_name] += 1

  def AddAnalysisReport(self, analysis_report):
    """Adds an analysis report.

    Args:
      analysis_report: an analysis report object (instance of AnalysisReport).

    Raises:
      IOError: when the storage writer is closed.
    """
    if not self._storage_file:
      raise IOError(u'Unable to write to closed storage writer.')

    for event_tag in analysis_report.GetTags():
      self.AddEventTag(event_tag)

    self._storage_file.AddAnalysisReport(analysis_report)

    report_identifier = u'Report: {0:s}'.format(analysis_report.plugin_name)

    self._session.analysis_reports_counter[u'Total Reports'] += 1
    self._session.analysis_reports_counter[report_identifier] += 1

  def AddError(self, error):
    """Adds an error.

    Args:
      error: an error object (instance of AnalysisError or ExtractionError).

    Raises:
      IOError: when the storage writer is closed.
    """
    if not self._storage_file:
      raise IOError(u'Unable to write to closed storage writer.')

    self._storage_file.AddError(error)
    self.number_of_errors += 1

  def AddEvent(self, event):
    """Adds an event.

    Args:
      event: an event (instance of EventObject).

    Raises:
      IOError: when the storage writer is closed.
    """
    if not self._storage_file:
      raise IOError(u'Unable to write to closed storage writer.')

    self._storage_file.AddEvent(event)
    self.number_of_events += 1

    self._UpdateCounters(event)

  def AddEventSource(self, event_source):
    """Adds an event source.

    Args:
      event_source: an event source object (instance of EventSource).

    Raises:
      IOError: when the storage writer is closed.
    """
    if not self._storage_file:
      raise IOError(u'Unable to write to closed storage writer.')

    self._storage_file.AddEventSource(event_source)
    self.number_of_event_sources += 1

  def AddEventTag(self, event_tag):
    """Adds an event tag.

    Args:
      event_tag: an event tag object (instance of EventTag).

    Raises:
      IOError: when the storage writer is closed.
    """
    if not self._storage_file:
      raise IOError(u'Unable to write to closed storage writer.')

    self._event_tags.append(event_tag)

    self._session.event_tags_counter[u'Total Tags'] += 1
    for label in event_tag.labels:
      self._session.event_tags_counter[label] += 1

  def CheckTaskStorageReadyForMerge(self, task_name):
    """Checks if a task storage is ready for with the session storage.

    Args:
      task_name (str): unique name of the task.

    Returns:
      bool: True if the storage for the task is ready for merge.

    Raises:
      IOError: if the storage type is not supported or
               if the temporary path for the task storage does not exist.
    """
    if self._storage_type != definitions.STORAGE_TYPE_SESSION:
      raise IOError(u'Unsupported storage type.')

    if not self._merge_task_storage_path:
      raise IOError(u'Missing merge task storage path.')

    storage_file_path = os.path.join(
        self._merge_task_storage_path, u'{0:s}.plaso'.format(task_name))

    return os.path.isfile(storage_file_path)

  def Close(self):
    """Closes the storage writer.

    Raises:
      IOError: when the storage writer is closed.
    """
    if not self._storage_file:
      raise IOError(u'Unable to write to closed storage writer.')

    self._storage_file.Close()
    self._storage_file = None

  def CreateTaskStorage(self, task):
    """Creates a task storage.

    The task storage is used to store attributes created by the task.

    Args:
      task(Task): task.

    Returns:
      StorageWriter: storage writer.

    Raises:
      IOError: if the storage type is not supported or
               if the temporary path for the task storage does not exist.
    """
    if self._storage_type != definitions.STORAGE_TYPE_SESSION:
      raise IOError(u'Unsupported storage type.')

    if not self._task_storage_path:
      raise IOError(u'Missing task storage path.')

    storage_file_path = os.path.join(
        self._task_storage_path, u'{0:s}.plaso'.format(task.identifier))

    return ZIPStorageFileWriter(
        self._session, storage_file_path, buffer_size=self._buffer_size,
        storage_type=definitions.STORAGE_TYPE_TASK, task=task)

  def GetNextEventSource(self):
    """Retrieves the next event source.

    Returns:
      EventSource: event source.

    Raises:
      IOError: when the storage writer is closed.
    """
    if not self._storage_file:
      raise IOError(u'Unable to read from closed storage writer.')

    event_source = self._storage_file.GetEventSourceByIndex(
        self._event_source_index)
    if event_source:
      self._event_source_index += 1
    return event_source

  def MergeTaskStorage(self, task_name):
    """Merges a task storage with the session storage.

    Args:
      task_name (str): unique name of the task.

    Returns:
      bool: True if the task storage was merged.

    Raises:
      IOError: if the storage type is not supported or
               if the temporary path for the task storage does not exist.
    """
    if self._storage_type != definitions.STORAGE_TYPE_SESSION:
      raise IOError(u'Unsupported storage type.')

    if not self._merge_task_storage_path:
      raise IOError(u'Missing merge task storage path.')

    storage_file_path = os.path.join(
        self._merge_task_storage_path, u'{0:s}.plaso'.format(task_name))

    if not os.path.isfile(storage_file_path):
      return False

    storage_reader = ZIPStorageFileReader(storage_file_path)
    self.MergeFromStorage(storage_reader)

    # Force close the storage reader so we can remove the file.
    storage_reader.Close()

    os.remove(storage_file_path)

    return True

  def Open(self):
    """Opens the storage writer.

    Raises:
      IOError: if the storage writer is already opened.
    """
    if self._storage_file:
      raise IOError(u'Storage writer already opened.')

    self._storage_file = StorageFile(
        self._output_file, buffer_size=self._buffer_size,
        storage_type=self._storage_type)

    self._event_source_index = self._storage_file.GetNumberOfEventSources()

  def PrepareMergeTaskStorage(self, task_name):
    """Prepares a task storage for merging.

    Args:
      task_name (str): unique name of the task.

    Raises:
      IOError: if the storage type is not supported or
               if the temporary path for the task storage does not exist.
    """
    if self._storage_type != definitions.STORAGE_TYPE_SESSION:
      raise IOError(u'Unsupported storage type.')

    if not self._task_storage_path:
      raise IOError(u'Missing task storage path.')

    storage_file_path = os.path.join(
        self._task_storage_path, u'{0:s}.plaso'.format(task_name))

    merge_storage_file_path = os.path.join(
        self._merge_task_storage_path, u'{0:s}.plaso'.format(task_name))

    os.rename(storage_file_path, merge_storage_file_path)

  def SetSerializersProfiler(self, serializers_profiler):
    """Sets the serializers profiler.

    Args:
      serializers_profiler (SerializersProfiler): serializers profile.
    """
    self._storage_file.SetSerializersProfiler(serializers_profiler)

  def StartTaskStorage(self):
    """Creates a temporary path for the task storage.

    Raises:
      IOError: if the storage type is not supported or
               if the temporary path for the task storage already exists.
    """
    if self._storage_type != definitions.STORAGE_TYPE_SESSION:
      raise IOError(u'Unsupported storage type.')

    if self._task_storage_path:
      raise IOError(u'Task storage path already exists.')

    output_directory = os.path.dirname(self._output_file)
    self._task_storage_path = tempfile.mkdtemp(dir=output_directory)

    self._merge_task_storage_path = os.path.join(
        self._task_storage_path, u'merge')
    os.mkdir(self._merge_task_storage_path)

  def StopTaskStorage(self, abort=False):
    """Removes the temporary path for the task storage.

    Args:
      abort (bool): True to indicated the stop is issued on abort.

    Raises:
      IOError: if the storage type is not supported or
               if the temporary path for the task storage does not exist.
    """
    if self._storage_type != definitions.STORAGE_TYPE_SESSION:
      raise IOError(u'Unsupported storage type.')

    if not self._task_storage_path:
      raise IOError(u'Missing task storage path.')

    if os.path.isdir(self._merge_task_storage_path):
      if abort:
        shutil.rmtree(self._merge_task_storage_path)
      else:
        os.rmdir(self._merge_task_storage_path)

    if os.path.isdir(self._task_storage_path):
      if abort:
        shutil.rmtree(self._task_storage_path)
      else:
        os.rmdir(self._task_storage_path)

    self._merge_task_storage_path = None
    self._task_storage_path = None

  # TODO: remove during phased processing refactor.
  def WritePreprocessObject(self, preprocess_object):
    """Writes a preprocessing object.

    Args:
      preprocess_object: a preprocess object (instance of PreprocessObject).

    Raises:
      IOError: when the storage writer is closed.
    """
    if not self._storage_file:
      raise IOError(u'Unable to write to closed storage writer.')

    # TODO: write the tags incrementally instead of buffering them
    # into a list.
    if self._event_tags:
      self._storage_file.AddEventTags(self._event_tags)
      # TODO: move the counters out of preprocessing object.
      # Kept for backwards compatibility for now.
      preprocess_object.counter = self._tags_counter

    # TODO: refactor this currently create a preprocessing object
    # for every sync in single processing.
    self._storage_file.WritePreprocessObject(preprocess_object)

  def WriteSessionCompletion(self):
    """Writes session completion information.

    Raises:
      IOError: if the storage type is not supported or
               when the storage writer is closed.
    """
    if self._storage_type != definitions.STORAGE_TYPE_SESSION:
      raise IOError(u'Unsupported storage type.')

    if not self._storage_file:
      raise IOError(u'Unable to write to closed storage writer.')

    session_completion = self._session.CreateSessionCompletion()
    self._storage_file.WriteSessionCompletion(session_completion)

  def WriteSessionStart(self):
    """Writes session start information.

    Raises:
      IOError: if the storage type is not supported or
               when the storage writer is closed.
    """
    if self._storage_type != definitions.STORAGE_TYPE_SESSION:
      raise IOError(u'Unsupported storage type.')

    if not self._storage_file:
      raise IOError(u'Unable to write to closed storage writer.')

    session_start = self._session.CreateSessionStart()
    self._storage_file.WriteSessionStart(session_start)

  def WriteTaskCompletion(self):
    """Writes task completion information.

    Raises:
      IOError: if the storage type is not supported or
               when the storage writer is closed.
    """
    if self._storage_type != definitions.STORAGE_TYPE_TASK:
      raise IOError(u'Unsupported storage type.')

    if not self._storage_file:
      raise IOError(u'Unable to write to closed storage writer.')

    task_completion = self._task.CreateTaskCompletion()
    self._storage_file.WriteTaskCompletion(task_completion)

  def WriteTaskStart(self):
    """Writes task start information.

    Raises:
      IOError: if the storage type is not supported or
               when the storage writer is closed.
    """
    if self._storage_type != definitions.STORAGE_TYPE_TASK:
      raise IOError(u'Unsupported storage type.')

    if not self._storage_file:
      raise IOError(u'Unable to write to closed storage writer.')

    task_start = self._task.CreateTaskStart()
    self._storage_file.WriteTaskStart(task_start)
