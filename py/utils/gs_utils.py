#!/usr/bin/python

# pylint: disable=C0301
"""
Copyright 2014 Google Inc.

Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.

Utilities for accessing Google Cloud Storage, using the boto library.

See http://googlecloudstorage.blogspot.com/2012/09/google-cloud-storage-tutorial-using-boto.html
for implementation tips.
"""
# pylint: enable=C0301

# System-level imports
import errno
import os
import posixpath
import random
import re
import shutil
import sys
import tempfile

# Imports from third-party code
TRUNK_DIRECTORY = os.path.abspath(os.path.join(
    os.path.dirname(__file__), os.pardir, os.pardir))
for import_subdir in ['boto']:
  import_dirpath = os.path.join(
      TRUNK_DIRECTORY, 'third_party', 'externals', import_subdir)
  if import_dirpath not in sys.path:
    # We need to insert at the beginning of the path, to make sure that our
    # imported versions are favored over others that might be in the path.
    sys.path.insert(0, import_dirpath)
from boto.gs.connection import GSConnection
from boto.gs.key import Key
from boto.s3.bucketlistresultset import BucketListResultSet
from boto.s3.prefix import Prefix


class GSUtils(object):
  """Utilities for accessing Google Cloud Storage, using the boto library."""

  def __init__(self, boto_file_path=os.path.join('~','.boto')):
    """Constructor.

    Params:
      boto_file_path: full path (local-OS-style) on local disk where .boto
          credentials file can be found.  An exception is thrown if this file
          is missing.
          TODO(epoger): Change missing-file behavior: allow the caller to
          operate on public files in Google Storage.
    """
    boto_file_path = os.path.expanduser(boto_file_path)
    print 'Reading boto file from %s' % boto_file_path
    boto_dict = _config_file_as_dict(filepath=boto_file_path)
    self._gs_access_key_id = boto_dict['gs_access_key_id']
    self._gs_secret_access_key = boto_dict['gs_secret_access_key']

  def delete_file(self, bucket, path):
    """Delete a single file within a GS bucket.

    TODO(epoger): what if bucket or path does not exist?  Should probably raise
    an exception.  Implement, and add a test to exercise this.

    Params:
      bucket: GS bucket to delete a file from
      path: full path (Posix-style) of the file within the bucket to delete
    """
    conn = self._create_connection()
    b = conn.get_bucket(bucket_name=bucket)
    item = Key(b)
    item.key = path
    item.delete()

  def upload_file(self, source_path, dest_bucket, dest_path):
    """Upload contents of a local file to Google Storage.

    TODO(epoger): Add the extra parameters provided by upload_file() within
    https://github.com/google/skia-buildbot/blob/master/slave/skia_slave_scripts/utils/old_gs_utils.py ,
    so we can replace that function with this one.

    params:
      source_path: full path (local-OS-style) on local disk to read from
      dest_bucket: GCS bucket to copy the file to
      dest_path: full path (Posix-style) within that bucket
    """
    conn = self._create_connection()
    b = conn.get_bucket(bucket_name=dest_bucket)
    item = Key(b)
    item.key = dest_path
    item.set_contents_from_filename(filename=source_path)

  def download_file(self, source_bucket, source_path, dest_path,
                    create_subdirs_if_needed=False):
    """Downloads a single file from Google Cloud Storage to local disk.

    Args:
      source_bucket: GCS bucket to download the file from
      source_path: full path (Posix-style) within that bucket
      dest_path: full path (local-OS-style) on local disk to copy the file to
      create_subdirs_if_needed: boolean; whether to create subdirectories as
          needed to create dest_path
    """
    conn = self._create_connection()
    b = conn.get_bucket(bucket_name=source_bucket)
    item = Key(b)
    item.key = source_path
    if create_subdirs_if_needed:
      _makedirs_if_needed(os.path.dirname(dest_path))
    with open(dest_path, 'w') as f:
      item.get_contents_to_file(fp=f)

  def list_bucket_contents(self, bucket, subdir=None):
    """Returns files in the Google Storage bucket as a (dirs, files) tuple.

    Args:
      bucket: name of the Google Storage bucket
      subdir: directory within the bucket to list, or None for root directory
    """
    # The GS command relies on the prefix (if any) ending with a slash.
    prefix = subdir or ''
    if prefix and not prefix.endswith('/'):
      prefix += '/'
    prefix_length = len(prefix) if prefix else 0

    conn = self._create_connection()
    b = conn.get_bucket(bucket_name=bucket)
    lister = BucketListResultSet(bucket=b, prefix=prefix, delimiter='/')
    dirs = []
    files = []
    for item in lister:
      t = type(item)
      if t is Key:
        files.append(item.key[prefix_length:])
      elif t is Prefix:
        dirs.append(item.name[prefix_length:-1])
    return (dirs, files)

  def _create_connection(self):
    """Returns a GSConnection object we can use to access Google Storage."""
    return GSConnection(
        gs_access_key_id=self._gs_access_key_id,
        gs_secret_access_key=self._gs_secret_access_key)


def _config_file_as_dict(filepath):
  """Reads a boto-style config file into a dict.

  Parses all lines from the file of this form: key = value
  TODO(epoger): Create unittest.

  Params:
    filepath: path to config file on local disk

  Returns: contents of the config file, as a dictionary

  Raises exception if file not found.
  """
  dic = {}
  line_regex = re.compile('^\s*(\S+)\s*=\s*(\S+)\s*$')
  with open(filepath) as f:
    for line in f:
      match = line_regex.match(line)
      if match:
        (key, value) = match.groups()
        dic[key] = value
  return dic


def _makedirs_if_needed(path):
  """Creates a directory (and any parent directories needed), if it does not
  exist yet.

  Args:
    path: full path of directory to create
  """
  try:
    os.makedirs(path)
  except OSError as e:
    if e.errno != errno.EEXIST:
      raise


def _run_self_test():
  bucket = 'chromium-skia-gm'
  remote_dir = 'gs_utils_test/%d' % random.randint(0, sys.maxint)
  subdir = 'subdir'
  filenames_to_upload = ['file1', 'file2']
  gs = GSUtils()

  # Upload test files to Google Storage.
  local_src_dir = tempfile.mkdtemp()
  os.mkdir(os.path.join(local_src_dir, subdir))
  try:
    for filename in filenames_to_upload:
      with open(os.path.join(local_src_dir, subdir, filename), 'w') as f:
        f.write('contents of %s\n' % filename)
      gs.upload_file(source_path=os.path.join(local_src_dir, subdir, filename),
                     dest_bucket=bucket,
                     dest_path=posixpath.join(remote_dir, subdir, filename))
  finally:
    shutil.rmtree(local_src_dir)

  # Get a list of the files we uploaded to Google Storage.
  (dirs, files) = gs.list_bucket_contents(
      bucket=bucket, subdir=remote_dir)
  assert dirs == [subdir], '%s == [%s]' % (dirs, subdir)
  assert files == [], '%s == []' % files
  (dirs, files) = gs.list_bucket_contents(
      bucket=bucket, subdir=posixpath.join(remote_dir, subdir))
  assert dirs == [], '%s == []' % dirs
  assert files == filenames_to_upload, '%s == %s' % (files, filenames_to_upload)

  # Download the files we uploaded to Google Storage, and validate contents.
  local_dest_dir = tempfile.mkdtemp()
  try:
    for filename in filenames_to_upload:
      gs.download_file(source_bucket=bucket,
                       source_path=posixpath.join(remote_dir, subdir, filename),
                       dest_path=os.path.join(local_dest_dir, subdir, filename),
                       create_subdirs_if_needed=True)
      with open(os.path.join(local_dest_dir, subdir, filename)) as f:
        file_contents = f.read()
      assert file_contents == 'contents of %s\n' % filename, (
          '%s == "contents of %s\n"' % (file_contents, filename))
  finally:
    shutil.rmtree(local_dest_dir)

  # Delete all the files we uploaded to Google Storage.
  for filename in filenames_to_upload:
    gs.delete_file(bucket=bucket,
                   path=posixpath.join(remote_dir, subdir, filename))

  # Confirm that we deleted all the files we uploaded to Google Storage.
  (dirs, files) = gs.list_bucket_contents(
      bucket=bucket, subdir=posixpath.join(remote_dir, subdir))
  assert dirs == [], '%s == []' % dirs
  assert files == [], '%s == []' % files


# TODO(epoger): How should we exercise this self-test?
# I avoided using the standard unittest framework, because these Google Storage
# operations are expensive and require .boto permissions.
#
# How can we automatically test this code without wasting too many resources
# or needing .boto permissions?
if __name__ == '__main__':
  _run_self_test()
