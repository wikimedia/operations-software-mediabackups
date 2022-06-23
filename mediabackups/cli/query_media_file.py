#!/usr/bin/python3
"""
Interactive command line application to query a file or set of
files with a set of given parameters (wiki, hash, title, etc.).
"""
import logging
import sys

from mediabackups.InteractiveQuery import InteractiveQuery
from mediabackups.MySQLMetadata import MySQLMetadata
from mediabackups.Util import read_yaml_config

METADATA_CONFIG_FILE = '/etc/mediabackup/mediabackups_db.conf'


def main():
    """
    Query a file or a list of files from media backups and print it on screen/logs.
    """
    action = 'query'
    logger = logging.getLogger(action)
    logging.basicConfig(format='[%(asctime)s] %(levelname)s:%(name)s %(message)s',
                        handlers=[logging.FileHandler('/var/log/mediabackups/recovery.log'),
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
    iq.print_and_finish(file_list)


if __name__ == "__main__":
    main()
