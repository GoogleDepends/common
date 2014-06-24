#!/usr/bin/env python
# Copyright (c) 2014 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

"""This module contains functions for using git."""


import re
import shell_utils


def _FindGit():
  """Find the git executable.

  Returns:
      A string suitable for passing to subprocess functions, or None.
  """
  def test_git_executable(git):
    """Test the git executable.

    Args:
        git: git executable path.
    Returns:
        True if test is successful.
    """
    try:
      shell_utils.run([git, '--version'], echo=False)
      return True
    except (OSError,):
      return False

  for git in ('git', 'git.exe', 'git.bat'):
    if test_git_executable(git):
      return git
  return None


GIT = _FindGit()


def Add(addition):
  """Run 'git add <addition>'"""
  shell_utils.run([GIT, 'add', addition])


def AIsAncestorOfB(a, b):
  """Return true if a is an ancestor of b."""
  return shell_utils.run([GIT, 'merge-base', a, b]).rstrip() == FullHash(a)


def FullHash(commit):
  """Return full hash of specified commit."""
  return shell_utils.run([GIT, 'rev-parse', '--verify', commit]).rstrip()


def IsMerge(commit):
  """Return True if the commit is a merge, False otherwise."""
  rev_parse = shell_utils.run([GIT, 'rev-parse', commit, '--max-count=1',
                               '--no-merges'])
  last_non_merge = rev_parse.split('\n')[0]
  # Get full hash since that is what was returned by rev-parse.
  return FullHash(commit) != last_non_merge


def MergeAbort():
  """Abort in process merge."""
  shell_utils.run([GIT, 'merge', '--abort'])


def ShortHash(commit):
  """Return short hash of the specified commit."""
  return shell_utils.run([GIT, 'show', commit, '--format=%h', '-s']).rstrip()


def Fetch(remote=None):
  """Run "git fetch". """
  cmd = [GIT, 'fetch']
  if remote:
    cmd.append(remote)
  shell_utils.run(cmd)


def GetRemoteMasterHash(git_url):
  return shell_utils.run([GIT, 'ls-remote', git_url, '--verify',
                          'refs/heads/master']).rstrip()


def GetCurrentBranch():
  return shell_utils.run([GIT, 'rev-parse', '--abbrev-ref', 'HEAD']).rstrip()


class GitBranch(object):
  """Class to manage git branches.

  This class allows one to create a new branch in a repository to make changes,
  then it commits the changes, switches to master branch, and deletes the
  created temporary branch upon exit.
  """
  def __init__(self, branch_name, commit_msg, upload=True, commit_queue=False,
               delete_when_finished=True):
    self._branch_name = branch_name
    self._commit_msg = commit_msg
    self._upload = upload
    self._commit_queue = commit_queue
    self._patch_set = 0
    self._delete_when_finished = delete_when_finished

  def __enter__(self):
    shell_utils.run([GIT, 'reset', '--hard', 'HEAD'])
    shell_utils.run([GIT, 'checkout', 'master'])
    if self._branch_name in shell_utils.run([GIT, 'branch']):
      shell_utils.run([GIT, 'branch', '-D', self._branch_name])
    shell_utils.run([GIT, 'checkout', '-b', self._branch_name,
                     '-t', 'origin/master'])
    return self

  def commit_and_upload(self, use_commit_queue=False):
    """Commit all changes and upload a CL, returning the issue URL."""
    try:
      shell_utils.run([GIT, 'commit', '-a', '-m', self._commit_msg])
    except shell_utils.CommandFailedException as e:
      if not 'nothing to commit' in e.output:
        raise
    upload_cmd = [GIT, 'cl', 'upload', '-f', '--bypass-hooks',
                  '--bypass-watchlists']
    self._patch_set += 1
    if self._patch_set > 1:
      upload_cmd.extend(['-t', 'Patch set %d' % self._patch_set])
    if use_commit_queue:
      upload_cmd.append('--use-commit-queue')
    shell_utils.run(upload_cmd)
    output = shell_utils.run([GIT, 'cl', 'issue']).rstrip()
    return re.match('^Issue number: (?P<issue>\d+) \((?P<issue_url>.+)\)$',
                    output).group('issue_url')

  def __exit__(self, exc_type, _value, _traceback):
    if self._upload:
      # Only upload if no error occurred.
      try:
        if exc_type is None:
          self.commit_and_upload(use_commit_queue=self._commit_queue)
      finally:
        shell_utils.run([GIT, 'checkout', 'master'])
        if self._delete_when_finished:
          shell_utils.run([GIT, 'branch', '-D', self._branch_name])
