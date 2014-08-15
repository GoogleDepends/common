#!/usr/bin/python

"""
Copyright 2014 Google Inc.

Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.

Test git_utils.py.
"""

# System-level imports
import os
import tempfile
import unittest

# Imports from within Skia
import git_utils


# A git repo we can use for tests.
REPO = 'https://skia.googlesource.com/common'

# A file in some subdirectory within REPO.
REPO_FILE = os.path.join('py', 'utils', 'git_utils.py')


class NewGitCheckoutTest(unittest.TestCase):

  def test_defaults(self):
    """Test NewGitCheckout created using default parameters."""
    with git_utils.NewGitCheckout(repository=REPO) as checkout:
      filepath = os.path.join(checkout.root, REPO_FILE)
      self.assertTrue(
          os.path.exists(filepath),
          'file %s should exist' % filepath)
    # Confirm that NewGitCheckout cleaned up after itself.
    self.assertFalse(
        os.path.exists(filepath),
        'file %s should not exist' % filepath)

  def test_subdir(self):
    """Create NewGitCheckout with a specific subdirectory."""
    subdir = os.path.dirname(REPO_FILE)
    file_within_subdir = os.path.basename(REPO_FILE)

    containing_dir = tempfile.mkdtemp()
    try:
      with git_utils.NewGitCheckout(repository=REPO, subdir=subdir,
                                    containing_dir=containing_dir) as checkout:
        self.assertTrue(
            checkout.root.startswith(containing_dir),
            'checkout.root %s should be within %s' % (
                checkout.root, containing_dir))
        filepath = os.path.join(checkout.root, file_within_subdir)
        self.assertTrue(
            os.path.exists(filepath),
            'file %s should exist' % filepath)
    finally:
      os.rmdir(containing_dir)

  def test_refspec(self):
    """Create NewGitCheckout with a specific refspec.

    This test depends on the fact that the whitespace.txt file was added to the
    repo in a particular commit.
    See https://skia.googlesource.com/common/+/c2200447734f13070fb3b2808dea58847241ab0e
    ('Initial commit of whitespace.txt')
    """
    filename = 'whitespace.txt'
    hash_without_file = 'f63e1cfff23615157e28942af5f5e8298351cb10'
    hash_with_file = 'c2200447734f13070fb3b2808dea58847241ab0e'

    with git_utils.NewGitCheckout(
        repository=REPO, refspec=hash_without_file) as checkout:
      filepath = os.path.join(checkout.root, filename)
      self.assertEquals(
          hash_without_file, checkout.commithash(),
          '"%s" != "%s"' % (hash_without_file, checkout.commithash()))
      self.assertFalse(
          os.path.exists(filepath),
          'file %s should not exist' % filepath)

    with git_utils.NewGitCheckout(
        repository=REPO, refspec=hash_with_file) as checkout:
      filepath = os.path.join(checkout.root, filename)
      self.assertEquals(
          hash_with_file, checkout.commithash(),
          '"%s" != "%s"' % (hash_with_file, checkout.commithash()))
      self.assertTrue(
          os.path.exists(filepath),
          'file %s should exist' % filepath)


def main(test_case_class):
  """Run the unit tests within this class."""
  suite = unittest.TestLoader().loadTestsFromTestCase(NewGitCheckoutTest)
  results = unittest.TextTestRunner(verbosity=2).run(suite)
  if not results.wasSuccessful():
    raise Exception('failed unittest %s' % test_case_class)


if __name__ == '__main__':
  main(NewGitCheckoutTest)
