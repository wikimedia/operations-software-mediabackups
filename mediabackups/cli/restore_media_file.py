#!/usr/bin/python3
"""
Interactive command line application to restore a previously backed up file
or set of files with a given parameters (wiki, hash, title, etc.).
"""
import logging
import sys

import mediabackups.interactive_query as interactive_query
from mediabackups.MySQLMetadata import MySQLMetadata
from mediabackups.Util import read_yaml_config

METADATA_CONFIG_FILE = '/etc/mediabackup/mediabackups_db.conf'


def main():
    """
    Recover a file or a list of file from media backups
    and write it to the local filesystem.
    """
    logger = logging.getLogger('recovery')
    logging.basicConfig(format='[%(asctime)s] %(levelname)s:%(name)s %(message)s',
                        handlers=[logging.FileHandler('/var/log/mediabackups/recovery.log'),
                                  logging.StreamHandler()],
                        level=logging.INFO)
    metadata = MySQLMetadata(read_yaml_config(METADATA_CONFIG_FILE))
    metadata.connect_db()

    if len(sys.argv) == 1:
        options = interactive_query.get_interactive_parameters(metadata)
    else:
        options = interactive_query.get_commandline_parameters(metadata)

    file_list = interactive_query.search_files(metadata, options)
    metadata.close_db()

    if len(file_list) == 0:
        logger.warning('No file was found that matched the given criteria, exiting.')
        sys.exit(4)
    interactive_query.print_and_confirm_recovery(file_list)
    interactive_query.recover_to_local(file_list)


if __name__ == "__main__":
    main()
