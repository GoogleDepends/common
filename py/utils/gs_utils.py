#!/usr/bin/python

# pylint: disable=C0301
"""
Copyright 2014 Google Inc.

Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.

Utilities for accessing Google Cloud Storage, using the boto library (wrapper
for the XML API).

API/library references:
- https://developers.google.com/storage/docs/reference-guide
- http://googlecloudstorage.blogspot.com/2012/09/google-cloud-storage-tutorial-using-boto.html
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
from boto.gs import acl
from boto.gs.bucket import Bucket
from boto.gs.connection import GSConnection
from boto.gs.key import Key
from boto.s3.bucketlistresultset import BucketListResultSet
from boto.s3.connection import SubdomainCallingFormat
from boto.s3.prefix import Prefix

# Permissions that may be set on each file in Google Storage.
# See SupportedPermissions in
# https://github.com/boto/boto/blob/develop/boto/gs/acl.py
PERMISSION_NONE  = None
PERMISSION_OWNER = 'FULL_CONTROL'
PERMISSION_READ  = 'READ'
PERMISSION_WRITE = 'WRITE'

# Types of identifiers we can use to set ACLs.
ID_TYPE_GROUP_BY_DOMAIN = acl.GROUP_BY_DOMAIN
ID_TYPE_GROUP_BY_EMAIL = acl.GROUP_BY_EMAIL
ID_TYPE_GROUP_BY_ID = acl.GROUP_BY_ID
ID_TYPE_USER_BY_EMAIL = acl.USER_BY_EMAIL
ID_TYPE_USER_BY_ID = acl.USER_BY_ID

# Which field we get/set in ACL entries, depending on ID_TYPE.
FIELD_BY_ID_TYPE = {
    ID_TYPE_GROUP_BY_DOMAIN: 'domain',
    ID_TYPE_GROUP_BY_EMAIL: 'email_address',
    ID_TYPE_GROUP_BY_ID: 'id',
    ID_TYPE_USER_BY_EMAIL: 'email_address',
    ID_TYPE_USER_BY_ID: 'id',
}


class AnonymousGSConnection(GSConnection):
  """GSConnection class that allows anonymous connections.

  The GSConnection class constructor in
  https://github.com/boto/boto/blob/develop/boto/gs/connection.py doesn't allow
  for anonymous connections (connections without credentials), so we have to
  override it.
  """
  def __init__(self):
    super(GSConnection, self).__init__(
        # This is the important bit we need to add...
        anon=True,
        # ...and these are just copied in from GSConnection.__init__()
        bucket_class=Bucket,
        calling_format=SubdomainCallingFormat(),
        host=GSConnection.DefaultHost,
        provider='google')


class GSUtils(object):
  """Utilities for accessing Google Cloud Storage, using the boto library."""

  def __init__(self, boto_file_path=None):
    """Constructor.

    Params:
      boto_file_path: full path (local-OS-style) on local disk where .boto
          credentials file can be found.  If None, then the GSUtils object
          created will be able to access only public files in Google Storage.

    Raises an exception if no file is found at boto_file_path, or if the file
    found there is malformed.
    """
    self._gs_access_key_id = None
    self._gs_secret_access_key = None
    if boto_file_path:
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

  def get_acl(self, bucket, path, id_type, id_value):
    """Retrieve partial access permissions on a single file in Google Storage.

    Various users who match this id_type/id_value pair may have access rights
    other than that returned by this call, if they have been granted those
    rights based on *other* id_types (e.g., perhaps they have group access
    rights, beyond their individual access rights).

    Params:
      bucket: GS bucket
      path: full path (Posix-style) to the file within that bucket
      id_type: must be one of the ID_TYPE_* constants defined above
      id_value: get permissions for users whose id_type field contains this
          value

    Returns: the PERMISSION_* constant which has been set for users matching
        this id_type/id_value, on this file; or PERMISSION_NONE if no such
        permissions have been set.
    """
    field = FIELD_BY_ID_TYPE[id_type]
    conn = self._create_connection()
    b = conn.get_bucket(bucket_name=bucket)
    acls = b.get_acl(key_name=path)
    matching_entries = [entry for entry in acls.entries.entry_list
                        if (entry.scope.type == id_type) and
                        (getattr(entry.scope, field) == id_value)]
    if matching_entries:
      assert len(matching_entries) == 1, '%d == 1' % len(matching_entries)
      return matching_entries[0].permission
    else:
      return PERMISSION_NONE

  def set_acl(self, bucket, path, id_type, id_value, permission):
    """Set partial access permissions on a single file in Google Storage.

    Note that a single set_acl() call will not guarantee what access rights any
    given user will have on a given file, because permissions are additive.
    (E.g., if you set READ permission for a group, but a member of that group
    already has WRITE permission, that member will still have WRITE permission.)
    TODO(epoger): Do we know that for sure?  I *think* that's how it works...

    If there is already a permission set on this file for this id_type/id_value
    combination, this call will overwrite it.

    Params:
      bucket: GS bucket
      path: full path (Posix-style) to the file within that bucket
      id_type: must be one of the ID_TYPE_* constants defined above
      id_value: add permission for users whose id_type field contains this value
      permission: permission to add for users matching id_type/id_value;
          must be one of the PERMISSION_* constants defined above.
          If PERMISSION_NONE, then any permissions will be granted to this
          particular id_type/id_value will be removed... but, given that
          permissions are additive, specific users may still have access rights
          based on permissions given to *other* id_type/id_value pairs.

    Example Code:
      bucket = 'gs://bucket-name'
      path = 'path/to/file'
      id_type = ID_TYPE_USER_BY_EMAIL
      id_value = 'epoger@google.com'
      set_acl(bucket, path, id_type, id_value, PERMISSION_READ)
      assert PERMISSION_READ == get_acl(bucket, path, id_type, id_value)
      set_acl(bucket, path, id_type, id_value, PERMISSION_WRITE)
      assert PERMISSION_WRITE == get_acl(bucket, path, id_type, id_value)
    """
    field = FIELD_BY_ID_TYPE[id_type]
    conn = self._create_connection()
    b = conn.get_bucket(bucket_name=bucket)
    acls = b.get_acl(key_name=path)

    # Remove any existing entries that refer to the same id_type/id_value,
    # because the API will fail if we try to set more than one.
    matching_entries = [entry for entry in acls.entries.entry_list
                        if (entry.scope.type == id_type) and
                        (getattr(entry.scope, field) == id_value)]
    if matching_entries:
      assert len(matching_entries) == 1, '%d == 1' % len(matching_entries)
      acls.entries.entry_list.remove(matching_entries[0])

    # Add a new entry to the ACLs.
    if permission != PERMISSION_NONE:
      args = {'type': id_type, 'permission': permission}
      args[field] = id_value
      new_entry = acl.Entry(**args)
      acls.entries.entry_list.append(new_entry)

    # Finally, write back the modified ACLs.
    b.set_acl(acl_or_str=acls, key_name=path)

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
    if self._gs_access_key_id:
      return GSConnection(
          gs_access_key_id=self._gs_access_key_id,
          gs_secret_access_key=self._gs_secret_access_key)
    else:
      return AnonymousGSConnection()

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


def _test_public_read():
  """Make sure we can read from public files without .boto file credentials."""
  gs = GSUtils()
  gs.list_bucket_contents(bucket='chromium-skia-gm-summaries', subdir=None)


def _test_authenticated_round_trip():
  try:
    gs = GSUtils(boto_file_path=os.path.expanduser(os.path.join('~','.boto')))
  except:
    print """
Failed to instantiate GSUtils object with default .boto file path.
Do you have a ~/.boto file that provides the credentials needed to read
and write gs://chromium-skia-gm ?
"""
    raise

  bucket = 'chromium-skia-gm'
  remote_dir = 'gs_utils_test/%d' % random.randint(0, sys.maxint)
  subdir = 'subdir'
  filenames_to_upload = ['file1', 'file2']

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

  # Manipulate ACLs on one of those files, and verify them.
  # TODO(epoger): Test id_types other than ID_TYPE_GROUP_BY_DOMAIN ?
  # TODO(epoger): Test setting multiple ACLs on the same file?
  id_type = ID_TYPE_GROUP_BY_DOMAIN
  id_value = 'google.com'
  fullpath = posixpath.join(remote_dir, subdir, filenames_to_upload[0])
  # Make sure ACL is empty to start with ...
  gs.set_acl(bucket=bucket, path=fullpath,
             id_type=id_type, id_value=id_value, permission=PERMISSION_NONE)
  permission = gs.get_acl(bucket=bucket, path=fullpath,
                          id_type=id_type, id_value=id_value)
  assert permission == PERMISSION_NONE, '%s == %s' % (
      permission, PERMISSION_NONE)
  # ... set it to OWNER ...
  gs.set_acl(bucket=bucket, path=fullpath,
             id_type=id_type, id_value=id_value, permission=PERMISSION_OWNER)
  permission = gs.get_acl(bucket=bucket, path=fullpath,
                          id_type=id_type, id_value=id_value)
  assert permission == PERMISSION_OWNER, '%s == %s' % (
      permission, PERMISSION_OWNER)
  # ... now set it to READ ...
  gs.set_acl(bucket=bucket, path=fullpath,
             id_type=id_type, id_value=id_value, permission=PERMISSION_READ)
  permission = gs.get_acl(bucket=bucket, path=fullpath,
                          id_type=id_type, id_value=id_value)
  assert permission == PERMISSION_READ, '%s == %s' % (
      permission, PERMISSION_READ)
  # ... and clear it again to finish.
  gs.set_acl(bucket=bucket, path=fullpath,
             id_type=id_type, id_value=id_value, permission=PERMISSION_NONE)
  permission = gs.get_acl(bucket=bucket, path=fullpath,
                          id_type=id_type, id_value=id_value)
  assert permission == PERMISSION_NONE, '%s == %s' % (
      permission, PERMISSION_NONE)

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


# TODO(epoger): How should we exercise these self-tests?
# See http://skbug.com/2751
if __name__ == '__main__':
  _test_public_read()
  _test_authenticated_round_trip()
  # TODO(epoger): Add _test_unauthenticated_access() to make sure we raise
  # an exception when we try to access without needed credentials.
