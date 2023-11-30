"""utility functions for media backups"""

from datetime import datetime
import os
from pathlib import Path
import logging

import hashlib
import yaml
from numpy import base_repr


class EncounteredDBListExpression(Exception):
    """Exception raised when a dblist expression is found isnide a dblist file"""
    pass


def read_dblist(path):
    """given a path, open and read it (assuming dblist format:
       'one like per database with comments'), and return it as
       a list"""
    wikis = []
    with open(path, 'r') as f:
        lines = f.read().splitlines()
    for line in lines:
        # Strip comments ('#' to end-of-line) and trim whitespace.
        wiki = line.split('#')[0].strip()
        if wiki.startswith('%%'):
            raise EncounteredDBListExpression('Encountered dblist expression inside dblist list file.')
        if wiki != '':
            wikis.append(wiki)
    return wikis


def read_yaml_config(file_name):
    """Given a filename, open and read it (assuming yaml format)
       on the user's home dir and return a dictionary with its contents"""
    logger = logging.getLogger('backup')
    home_dir = str(Path.home())
    config_file_path = os.path.join(home_dir, file_name)
    with open(config_file_path, 'r') as f:
        try:
            return yaml.safe_load(f) or {}
        except yaml.YAMLError:
            logger.error('Yaml configuration "%s" could not be loaded', file_name)
            return {}


def sha1sum(path):
    """Calculates the sha1 sum of a given file without loading it at once in memory"""
    sha1sum = hashlib.sha1()
    with open(path, 'rb') as fd:
        block = fd.read(2**16)
        while len(block) != 0:
            sha1sum.update(block)
            block = fd.read(2**16)
    return sha1sum.hexdigest().zfill(40)


def sha256sum(path):
    """Calculates the sha256 sum of a given file without loading it at once in memory"""
    sha256sum = hashlib.sha256()
    with open(path, 'rb') as fd:
        block = fd.read(2**16)
        while len(block) != 0:
            sha256sum.update(block)
            block = fd.read(2**16)
    return sha256sum.hexdigest().zfill(64)


def base16tobase36(number):
    """
    Given a utf-8 string representing a 16-base (hexadecimal)
    number, return the equivalent string representation on
    base36, zero-filled, low case.
    """
    return base_repr(int(number, 16), 36).lower().zfill(31)


def base36tobase16(number):
    """
    Given a utf-8 string representing a 36-base number,
    return the equivalent string representation on base 16
    (hexadecimal), zero-filled, low case.
    """
    return base_repr(int(number, 36), 16).lower().zfill(40)


def mwdate2datetime(timestamp):
    """
    Converts a mediawiki-formatted date into a proper timestamp.
    Returns None if none is received, otherwise, if the format
    is incorrect, it will return epoch 0.
    """
    if timestamp is None:
        return None
    try:
        date = datetime.strptime(timestamp, '%Y%m%d%H%M%S')
    except ValueError:
        date = datetime.strptime('19700101000001', '%Y%m%d%H%M%S')
    return date
