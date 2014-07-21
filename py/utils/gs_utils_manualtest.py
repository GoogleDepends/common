#!/usr/bin/python

"""Tests for gs_utils.py.

TODO(epoger): How should we exercise these self-tests? See http://skbug.com/2751
"""

# System-level imports.
import os
import posixpath
import random
import shutil
import sys
import tempfile

# Local imports.
import gs_utils


def _test_public_read():
  """Make sure we can read from public files without .boto file credentials."""
  gs = gs_utils.GSUtils()
  gs.list_bucket_contents(bucket='chromium-skia-gm-summaries', subdir=None)


def _test_authenticated_round_trip():
  try:
    gs = gs_utils.GSUtils(
        boto_file_path=os.path.expanduser(os.path.join('~','.boto')))
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
  id_type = gs.IdType.GROUP_BY_DOMAIN
  id_value = 'chromium.org'
  set_permission = gs.Permission.READ
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
  # TODO(epoger): Test IdTypes other than GROUP_BY_DOMAIN ?
  # TODO(epoger): Test setting multiple ACLs on the same file?
  id_type = gs.IdType.GROUP_BY_DOMAIN
  id_value = 'google.com'
  fullpath = posixpath.join(remote_dir, subdir, filenames_to_upload[0])
  # Make sure ACL is empty to start with ...
  gs.set_acl(bucket=bucket, path=fullpath,
             id_type=id_type, id_value=id_value, permission=gs.Permission.EMPTY)
  permission = gs.get_acl(bucket=bucket, path=fullpath,
                          id_type=id_type, id_value=id_value)
  assert permission == gs.Permission.EMPTY, '%s == %s' % (
      permission, gs.Permission.EMPTY)
  # ... set it to OWNER ...
  gs.set_acl(bucket=bucket, path=fullpath,
             id_type=id_type, id_value=id_value, permission=gs.Permission.OWNER)
  permission = gs.get_acl(bucket=bucket, path=fullpath,
                          id_type=id_type, id_value=id_value)
  assert permission == gs.Permission.OWNER, '%s == %s' % (
      permission, gs.Permission.OWNER)
  # ... now set it to READ ...
  gs.set_acl(bucket=bucket, path=fullpath,
             id_type=id_type, id_value=id_value, permission=gs.Permission.READ)
  permission = gs.get_acl(bucket=bucket, path=fullpath,
                          id_type=id_type, id_value=id_value)
  assert permission == gs.Permission.READ, '%s == %s' % (
      permission, gs.Permission.READ)
  # ... and clear it again to finish.
  gs.set_acl(bucket=bucket, path=fullpath,
             id_type=id_type, id_value=id_value, permission=gs.Permission.EMPTY)
  permission = gs.get_acl(bucket=bucket, path=fullpath,
                          id_type=id_type, id_value=id_value)
  assert permission == gs.Permission.EMPTY, '%s == %s' % (
      permission, gs.Permission.EMPTY)

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
    gs = gs_utils.GSUtils(
        boto_file_path=os.path.expanduser(os.path.join('~','.boto')))
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
  id_type = gs.IdType.GROUP_BY_DOMAIN
  id_value = 'chromium.org'
  set_permission = gs.Permission.READ
  local_src_dir = tempfile.mkdtemp()
  os.mkdir(os.path.join(local_src_dir, subdir))
  try:
    for filename in filenames:
      with open(os.path.join(local_src_dir, subdir, filename), 'w') as f:
        f.write('contents of %s\n' % filename)
    gs.upload_dir_contents(
        source_dir=local_src_dir, dest_bucket=bucket, dest_dir=remote_dir,
        predefined_acl=gs.PredefinedACL.PRIVATE,
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


if __name__ == '__main__':
  _test_public_read()
  _test_authenticated_round_trip()
  _test_dir_upload_and_download()
  # TODO(epoger): Add _test_unauthenticated_access() to make sure we raise
  # an exception when we try to access without needed credentials.
