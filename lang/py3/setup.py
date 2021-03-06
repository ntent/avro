#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import shutil
import sys

from setuptools import setup


def Main():
  assert (sys.version_info[0] >= 3), \
      ('Python version >= 3 required, got %r' % sys.version_info)

  py3_dir = os.path.dirname(os.path.abspath(__file__))
  root_dir = os.path.dirname(os.path.dirname(py3_dir))

  # Read and copy Avro version:
  version_file_path = os.path.join(root_dir, 'share', 'VERSION.txt')
  with open(version_file_path, 'r') as f:
    avro_version = f.read().strip()
  shutil.copy(
      src=version_file_path,
      dst=os.path.join(py3_dir, 'avro', 'VERSION.txt'),
  )

  # Copy necessary avsc files:
  avsc_file_path = os.path.join(
      root_dir, 'share', 'schemas',
      'org', 'apache', 'avro', 'ipc', 'HandshakeRequest.avsc')
  shutil.copy(
      src=avsc_file_path,
      dst=os.path.join(py3_dir, 'avro', 'HandshakeRequest.avsc'),
  )

  avsc_file_path = os.path.join(
      root_dir, 'share', 'schemas',
      'org', 'apache', 'avro', 'ipc', 'HandshakeResponse.avsc')
  shutil.copy(
      src=avsc_file_path,
      dst=os.path.join(py3_dir, 'avro', 'HandshakeResponse.avsc'),
  )

  avsc_file_path = os.path.join(
      root_dir, 'share', 'test', 'schemas', 'interop.avsc')
  shutil.copy(
      src=avsc_file_path,
      dst=os.path.join(py3_dir, 'avro', 'tests', 'interop.avsc'),
  )

  # Make sure the avro shell script is executable:
  os.chmod(
      path=os.path.join(py3_dir, 'scripts', 'avro'),
      mode=0o777,
  )

  setup(
    name = 'avro-python3',
    version = avro_version,
    packages = ['avro'],
    package_dir = {'avro': 'avro'},
    scripts = ['scripts/avro'],

    include_package_data=True,
    package_data = {
        'avro': [
            'HandshakeRequest.avsc',
            'HandshakeResponse.avsc',
            'VERSION.txt',
        ],
    },

    test_suite='avro.tests.run_tests',
    tests_require=[],

    # metadata for upload to PyPI
    author = 'Apache Avro',
    author_email = 'avro-dev@hadoop.apache.org',
    description = 'Avro is a serialization and RPC framework.',
    license = 'Apache License 2.0',
    keywords = 'avro serialization rpc',
    url = 'http://hadoop.apache.org/avro',
  )


if __name__ == '__main__':
  Main()
