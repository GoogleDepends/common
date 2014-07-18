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
from boto.exception import BotoServerError
from boto.gs import acl
from boto.gs.bucket import Bucket
from boto.gs.connection import GSConnection
from boto.gs.key import Key
from boto.s3.bucketlistresultset import BucketListResultSet
from boto.s3.connection import SubdomainCallingFormat
from boto.s3.prefix import Prefix

# Predefined (aka "canned") ACLs that provide a "base coat" of permissions for
# each file in Google Storage.  See CannedACLStrings in
# https://github.com/boto/boto/blob/develop/boto/gs/acl.py
# Also see https://developers.google.com/storage/docs/accesscontrol
PREDEFINED_ACL_AUTHENTICATED_READ        = 'authenticated-read'
PREDEFINED_ACL_BUCKET_OWNER_FULL_CONTROL = 'bucket-owner-full-control'
PREDEFINED_ACL_BUCKET_OWNER_READ         = 'bucket-owner-read'
PREDEFINED_ACL_PRIVATE                   = 'private'
PREDEFINED_ACL_PROJECT_PRIVATE           = 'project-private'
PREDEFINED_ACL_PUBLIC_READ               = 'public-read'
PREDEFINED_ACL_PUBLIC_READ_WRITE         = 'public-read-write'

# "Fine-grained" permissions that may be set per user/group on each file in
# Google Storage.  See SupportedPermissions in
# https://github.com/boto/boto/blob/develop/boto/gs/acl.py
# Also see https://developers.google.com/storage/docs/accesscontrol
PERMISSION_NONE  = None
PERMISSION_OWNER = 'FULL_CONTROL'
PERMISSION_READ  = 'READ'
PERMISSION_WRITE = 'WRITE'

# Types of identifiers we can use to set "fine-grained" ACLs.
ID_TYPE_GROUP_BY_DOMAIN = acl.GROUP_BY_DOMAIN
ID_TYPE_GROUP_BY_EMAIL  = acl.GROUP_BY_EMAIL
ID_TYPE_GROUP_BY_ID     = acl.GROUP_BY_ID
ID_TYPE_USER_BY_EMAIL   = acl.USER_BY_EMAIL
ID_TYPE_USER_BY_ID      = acl.USER_BY_ID

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
    b = self._connect_to_bucket(bucket_name=bucket)
    item = Key(b)
    item.key = path
    try:
      item.delete()
    except BotoServerError, e:
      e.body = (repr(e.body) +
                ' while deleting bucket=%s, path=%s' % (bucket, path))
      raise

  def upload_file(self, source_path, dest_bucket, dest_path,
                  predefined_acl=None, fine_grained_acl_list=None):
    """Upload contents of a local file to Google Storage.

    TODO(epoger): Add the only_if_modified param provided by upload_file() in
    https://github.com/google/skia-buildbot/blob/master/slave/skia_slave_scripts/utils/old_gs_utils.py ,
    so we can replace that function with this one.

    params:
      source_path: full path (local-OS-style) on local disk to read from
      dest_bucket: GCS bucket to copy the file to
      dest_path: full path (Posix-style) within that bucket
      predefined_acl: which predefined ACL to apply to the file on Google
          Storage; must be one of the PREDEFINED_ACL_* constants defined above.
          If None, inherits dest_bucket's default object ACL.
          TODO(epoger): add unittests for this param, although it seems to work
          in my manual testing
      fine_grained_acl_list: list of (id_type, id_value, permission) tuples
          to apply to the uploaded file (on top of the predefined_acl),
          or None if predefined_acl is sufficient
    """
    b = self._connect_to_bucket(bucket_name=dest_bucket)
    item = Key(b)
    item.key = dest_path
    try:
      item.set_contents_from_filename(filename=source_path,
                                      policy=predefined_acl)
    except BotoServerError, e:
      e.body = (repr(e.body) +
                ' while uploading source_path=%s to bucket=%s, path=%s' % (
                    source_path, dest_bucket, item.key))
      raise
    # TODO(epoger): This may be inefficient, because it calls
    # _connect_to_bucket() again.  Depending on how expensive that
    # call is, we may want to optimize this.
    for (id_type, id_value, permission) in fine_grained_acl_list or []:
      self.set_acl(
          bucket=dest_bucket, path=item.key,
          id_type=id_type, id_value=id_value, permission=permission)

  def upload_dir_contents(self, source_dir, dest_bucket, dest_dir,
                          predefined_acl=None, fine_grained_acl_list=None):
    """Recursively upload contents of a local directory to Google Storage.

    params:
      source_dir: full path (local-OS-style) on local disk of directory to copy
          contents of
      dest_bucket: GCS bucket to copy the files into
      dest_dir: full path (Posix-style) within that bucket; write the files into
          this directory
      predefined_acl: which predefined ACL to apply to the files on Google
          Storage; must be one of the PREDEFINED_ACL_* constants defined above.
          If None, inherits dest_bucket's default object ACL.
          TODO(epoger): add unittests for this param, although it seems to work
          in my manual testing
      fine_grained_acl_list: list of (id_type, id_value, permission) tuples
          to apply to every file uploaded (on top of the predefined_acl),
          or None if predefined_acl is sufficient

    The copy operates as a "merge with overwrite": any files in source_dir will
    be "overlaid" on top of the existing content in dest_dir.  Existing files
    with the same names will be overwritten.

    TODO(epoger): Upload multiple files simultaneously to reduce latency.

    TODO(epoger): Add a "noclobber" mode that will not upload any files would
    overwrite existing files in Google Storage.

    TODO(epoger): Consider adding a do_compress parameter that would compress
    the file using gzip before upload, and add a "Content-Encoding:gzip" header
    so that HTTP downloads of the file would be unzipped automatically.
    See https://developers.google.com/storage/docs/gsutil/addlhelp/
        WorkingWithObjectMetadata#content-encoding
    """
    b = self._connect_to_bucket(bucket_name=dest_bucket)
    for filename in sorted(os.listdir(source_dir)):
      local_path = os.path.join(source_dir, filename)
      if os.path.isdir(local_path):
        self.upload_dir_contents(  # recurse
            source_dir=local_path, dest_bucket=dest_bucket,
            dest_dir=posixpath.join(dest_dir, filename),
            predefined_acl=predefined_acl,
            fine_grained_acl_list=fine_grained_acl_list)
      else:
        item = Key(b)
        dest_path = posixpath.join(dest_dir, filename)
        item.key = dest_path
        try:
          item.set_contents_from_filename(
              filename=local_path, policy=predefined_acl)
        except BotoServerError, e:
          e.body = (repr(e.body) +
                    ' while uploading local_path=%s to bucket=%s, path=%s' % (
                        local_path, dest_bucket, dest_path))
          raise
        # TODO(epoger): This may be inefficient, because it calls
        # _connect_to_bucket() for every file.  Depending on how expensive that
        # call is, we may want to optimize this.
        for (id_type, id_value, permission) in fine_grained_acl_list or []:
          self.set_acl(
              bucket=dest_bucket, path=dest_path,
              id_type=id_type, id_value=id_value, permission=permission)

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
    b = self._connect_to_bucket(bucket_name=source_bucket)
    item = Key(b)
    item.key = source_path
    if create_subdirs_if_needed:
      _makedirs_if_needed(os.path.dirname(dest_path))
    with open(dest_path, 'w') as f:
      try:
        item.get_contents_to_file(fp=f)
      except BotoServerError, e:
        e.body = (repr(e.body) +
                  ' while downloading bucket=%s, path=%s to local_path=%s' % (
                      source_bucket, source_path, dest_path))
        raise

  def download_dir_contents(self, source_bucket, source_dir, dest_dir):
    """Recursively download contents of a Google Storage directory to local disk

    params:
      source_bucket: GCS bucket to copy the files from
      source_dir: full path (Posix-style) within that bucket; read the files
          from this directory
      dest_dir: full path (local-OS-style) on local disk of directory to copy
          the files into

    The copy operates as a "merge with overwrite": any files in source_dir will
    be "overlaid" on top of the existing content in dest_dir.  Existing files
    with the same names will be overwritten.

    TODO(epoger): Download multiple files simultaneously to reduce latency.
    """
    _makedirs_if_needed(dest_dir)
    b = self._connect_to_bucket(bucket_name=source_bucket)
    (dirs, files) = self.list_bucket_contents(
        bucket=source_bucket, subdir=source_dir)

    for filename in files:
      item = Key(b)
      item.key = posixpath.join(source_dir, filename)
      dest_path = os.path.join(dest_dir, filename)
      with open(dest_path, 'w') as f:
        try:
          item.get_contents_to_file(fp=f)
        except BotoServerError, e:
          e.body = (repr(e.body) +
                    ' while downloading bucket=%s, path=%s to local_path=%s' % (
                        source_bucket, item.key, dest_path))
          raise

    for dirname in dirs:
      self.download_dir_contents(  # recurse
          source_bucket=source_bucket,
          source_dir=posixpath.join(source_dir, dirname),
          dest_dir=os.path.join(dest_dir, dirname))

  def get_acl(self, bucket, path, id_type, id_value):
    """Retrieve partial access permissions on a single file in Google Storage.

    Various users who match this id_type/id_value pair may have access rights
    other than that returned by this call, if they have been granted those
    rights based on *other* id_types (e.g., perhaps they have group access
    rights, beyond their individual access rights).

    TODO(epoger): What if the remote file does not exist?  This should probably
    raise an exception in that case.

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
    b = self._connect_to_bucket(bucket_name=bucket)
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

    TODO(epoger): What if the remote file does not exist?  This should probably
    raise an exception in that case.

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
    b = self._connect_to_bucket(bucket_name=bucket)
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

    TODO(epoger): This should raise an exception if subdir does not exist in
    Google Storage; right now, it just returns empty contents.

    Args:
      bucket: name of the Google Storage bucket
      subdir: directory within the bucket to list, or None for root directory
    """
    # The GS command relies on the prefix (if any) ending with a slash.
    prefix = subdir or ''
    if prefix and not prefix.endswith('/'):
      prefix += '/'
    prefix_length = len(prefix) if prefix else 0

    b = self._connect_to_bucket(bucket_name=bucket)
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

  def _connect_to_bucket(self, bucket_name):
    """Returns a Bucket object we can use to access a particular bucket in GS.

    Params:
      bucket_name: name of the bucket (e.g., 'chromium-skia-gm')
    """
    try:
      return self._create_connection().get_bucket(bucket_name=bucket_name)
    except BotoServerError, e:
      e.body = repr(e.body) + ' while connecting to bucket=%s' % bucket_name
      raise

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

  # Upload test files to Google Storage, checking that their fine-grained
  # ACLs were set correctly.
  id_type = ID_TYPE_GROUP_BY_DOMAIN
  id_value = 'chromium.org'
  set_permission = PERMISSION_READ
  local_src_dir = tempfile.mkdtemp()
  os.mkdir(os.path.join(local_src_dir, subdir))
  try:
    for filename in filenames_to_upload:
      with open(os.path.join(local_src_dir, subdir, filename), 'w') as f:
        f.write('contents of %s\n' % filename)
      dest_path = posixpath.join(remote_dir, subdir, filename)
      gs.upload_file(
          source_path=os.path.join(local_src_dir, subdir, filename),
          dest_bucket=bucket, dest_path=dest_path,
          fine_grained_acl_list=[(id_type, id_value, set_permission)])
      got_permission = gs.get_acl(bucket=bucket, path=dest_path,
                                  id_type=id_type, id_value=id_value)
      assert got_permission == set_permission, '%s == %s' % (
          got_permission, set_permission)
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


def _test_dir_upload_and_download():
  """Test upload_dir_contents() and download_dir_contents()."""
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
  filenames = ['file1', 'file2']

  # Create directory tree on local disk and upload it.
  id_type = ID_TYPE_GROUP_BY_DOMAIN
  id_value = 'chromium.org'
  set_permission = PERMISSION_READ
  local_src_dir = tempfile.mkdtemp()
  os.mkdir(os.path.join(local_src_dir, subdir))
  try:
    for filename in filenames:
      with open(os.path.join(local_src_dir, subdir, filename), 'w') as f:
        f.write('contents of %s\n' % filename)
    gs.upload_dir_contents(
        source_dir=local_src_dir, dest_bucket=bucket, dest_dir=remote_dir,
        predefined_acl=PREDEFINED_ACL_PRIVATE,
        fine_grained_acl_list=[(id_type, id_value, set_permission)])
  finally:
    shutil.rmtree(local_src_dir)

  # Validate the list of the files we uploaded to Google Storage.
  (dirs, files) = gs.list_bucket_contents(
      bucket=bucket, subdir=remote_dir)
  assert dirs == [subdir], '%s == [%s]' % (dirs, subdir)
  assert files == [], '%s == []' % files
  (dirs, files) = gs.list_bucket_contents(
      bucket=bucket, subdir=posixpath.join(remote_dir, subdir))
  assert dirs == [], '%s == []' % dirs
  assert files == filenames, '%s == %s' % (files, filenames)

  # Check the fine-grained ACLs we set in Google Storage.
  for filename in filenames:
    got_permission = gs.get_acl(
        bucket=bucket, path=posixpath.join(remote_dir, subdir, filename),
        id_type=id_type, id_value=id_value)
    assert got_permission == set_permission, '%s == %s' % (
        got_permission, set_permission)

  # Download the directory tree we just uploaded, make sure its contents
  # are what we expect, and then delete the tree in Google Storage.
  local_dest_dir = tempfile.mkdtemp()
  try:
    gs.download_dir_contents(source_bucket=bucket, source_dir=remote_dir,
                             dest_dir=local_dest_dir)
    for filename in filenames:
      with open(os.path.join(local_dest_dir, subdir, filename)) as f:
        file_contents = f.read()
      assert file_contents == 'contents of %s\n' % filename, (
          '%s == "contents of %s\n"' % (file_contents, filename))
  finally:
    shutil.rmtree(local_dest_dir)
    for filename in filenames:
      gs.delete_file(bucket=bucket,
                     path=posixpath.join(remote_dir, subdir, filename))


# TODO(epoger): How should we exercise these self-tests?
# See http://skbug.com/2751
if __name__ == '__main__':
  _test_public_read()
  _test_authenticated_round_trip()
  _test_dir_upload_and_download()
  # TODO(epoger): Add _test_unauthenticated_access() to make sure we raise
  # an exception when we try to access without needed credentials.
