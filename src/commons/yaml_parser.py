# -*- coding: utf-8 -*-
# !/usr/bin/python
#
# Copyright (c) 2022 Seagate Technology LLC and/or its Affiliates
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.
#

"""Yaml Parser for IO stability."""

import datetime
import logging
import yaml
from src.commons.constants import KB
from src.commons.constants import KIB
from src.commons.constants import ROOT

LOGGER = logging.getLogger(ROOT)


def yaml_parser(yaml_file) -> dict:
    """
    YAML file to python dictionary.

    :param yaml_file: yaml file to parse.
    :return python dict containing file contents.
    """
    LOGGER.debug("YAML file selected for parse: %s", yaml_file)
    yaml_dict = {}
    with open(yaml_file, "r", encoding="utf-8") as obj:
        data = yaml.safe_load(obj)
        yaml_dict.update(data)
    LOGGER.debug("YAML file data: %s", yaml_dict)
    return yaml_dict


def convert_to_bytes(size):
    """
    function to convert any size to bytes.

    :param size: object size
    can be provided as byte(s), kb, kib, mb, mib, gb, gib, tb, tib
    :return equivalent bytes value for object size.
    """
    size = size.lower()
    size_bytes = 0
    if 'bytes' in size or 'byte' in size:
        size_bytes = int(size.split('byte')[0])
    if 'kb' in size:
        size_bytes = int(size.split('kb')[0]) * KB
    if 'kib' in size:
        size_bytes = int(size.split('kib')[0]) * KIB
    if 'mb' in size:
        size_bytes = int(size.split('mb')[0]) * KB * KB
    if 'mib' in size:
        size_bytes = int(size.split('mib')[0]) * KIB * KIB
    if 'gb' in size:
        size_bytes = int(size.split('gb')[0]) * KB * KB * KB
    if 'gib' in size:
        size_bytes = int(size.split('gib')[0]) * KIB * KIB * KIB
    if 'tb' in size:
        size_bytes = int(size.split('tb')[0]) * KB * KB * KB * KB
    if 'tib' in size:
        size_bytes = int(size.split('tib')[0]) * KIB * KIB * KIB * KIB
    LOGGER.debug(size_bytes)
    return size_bytes


def convert_to_time_delta(time):
    """
    function to convert execution time in time delta format.

    :param time : accepts time in format 0d0h0m0s
    :return python timedelta object.
    """
    time = time.lower()
    days = hrs = mnt = sec = 00
    if 'd' in time:
        days = int(time.split('d')[0])
        time = time.split('d')[1]
    if 'h' in time:
        hrs = int(time.split('h')[0])
        time = time.split('h')[1]
    if 'm' in time:
        mnt = int(time.split('m')[0])
        time = time.split('m')[1]
    if 's' in time:
        sec = int(time.split('s')[0])
    datetime_obj = datetime.timedelta(days=days, hours=hrs, minutes=mnt, seconds=sec)
    return datetime_obj


# pylint: disable-msg=too-many-branches
def test_parser(yaml_file, number_of_nodes):
    """
    parse a test yaml file.

    :param yaml_file: accepts and parses a test YAML file
    :param number_of_nodes: accepts number of nodes to calculate sessions (default=1)
    :return python dictionary containing file contents.
    """
    size_types = ["object_size", "part_size"]
    s3_io_test = yaml_parser(yaml_file)
    delta_list = []
    for test, data in s3_io_test.items():
        if "object_size" not in data:
            LOGGER.error("Object size is compulsory")
            return False
        for size_type in size_types:
            if size_type in data:
                if isinstance(data[size_type], dict):
                    if "start" not in data[size_type] or "end" not in data[size_type]:
                        LOGGER.error("Please define range using start and end keys")
                        return False
                    data[size_type]["start"] = convert_to_bytes(data[size_type]["start"])
                    data[size_type]["end"] = convert_to_bytes(data[size_type]["end"])
                elif isinstance(data[size_type], list):
                    out = []
                    for item in data[size_type]:
                        out.append(convert_to_bytes(item))
                    data[size_type] = out
                else:
                    size = data[size_type]
                    data[size_type] = {}
                    data[size_type]["start"] = convert_to_bytes(size)
                    data[size_type]["end"] = convert_to_bytes(size) + 1
        if "range_read" in data:
            if isinstance(data["range_read"], dict):
                if "start" not in data["range_read"] or "end" not in data["range_read"]:
                    LOGGER.error("Please define range using start and end keys")
                    return False
                data["range_read"]["start"] = convert_to_bytes(data["range_read"]["start"])
                data["range_read"]["end"] = convert_to_bytes(data["range_read"]["end"])
            elif isinstance(data["range_read"], str):
                data["range_read"] = convert_to_bytes(data["range_read"])
        if test == "test_1":
            data['start_time'] = datetime.timedelta(hours=00, minutes=00, seconds=00)
            delta_list.append(convert_to_time_delta(data['min_runtime']))
        else:
            data['start_time'] = delta_list.pop()
            delta_list.append(data['start_time'] + convert_to_time_delta(data['min_runtime']))
        data['min_runtime'] = convert_to_time_delta(data['min_runtime'])
        if "part_size" not in data:
            data["part_size"] = {}
            data["part_size"]["start"] = 0
            data["part_size"]["end"] = 0
        if 'sessions_per_node' in data.keys():
            data['sessions'] = data['sessions_per_node'] * number_of_nodes
    LOGGER.debug("test object %s: ", s3_io_test)

    return s3_io_test