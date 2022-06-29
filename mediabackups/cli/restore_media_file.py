#!/usr/bin/python3
"""
Interactive command line application to restore a previously backed up file
or files with a set given parameters (wiki, hash, title, etc.).
"""
import logging
import sys

from mediabackups.InteractiveQuery import InteractiveQuery
from mediabackups.MySQLMetadata import MySQLMetadata
from mediabackups.Util import read_yaml_config

METADATA_CONFIG_FILE = '/etc/mediabackup/mediabackups_db.conf'


def main():
    """
    Recover a file or a list of file from media backups
    and write it to the local filesystem.
    """
    action = 'recovery'
    logger = logging.getLogger(action)
    logging.basicConfig(format='[%(asctime)s] %(levelname)s:%(name)s %(message)s',
                        handlers=[logging.FileHandler(f'/var/log/mediabackups/{action}.log'),
                                  logging.StreamHandler()],
                        level=logging.INFO)
    metadata = MySQLMetadata(read_yaml_config(METADATA_CONFIG_FILE))
    metadata.connect_db()

    iq = InteractiveQuery(action)
    if len(sys.argv) == 1:
        options = iq.get_interactive_parameters(metadata)
    else:
        options = iq.get_commandline_parameters(metadata)

    file_list = iq.search_files(metadata, options)
    metadata.close_db()

    if len(file_list) == 0:
        logger.warning('No file was found that matched the given criteria, exiting.')
        sys.exit(4)
    iq.print_and_confirm_action(file_list)
    iq.recover_to_local(file_list)


if __name__ == "__main__":
    main()
