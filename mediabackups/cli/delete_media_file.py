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
    action = 'deletion'
    dry_mode = True
    logger = logging.getLogger(action)
    logging.basicConfig(format='[%(asctime)s] %(levelname)s:%(name)s %(message)s',
                        handlers=[logging.FileHandler(f'/var/log/mediabackups/{action}.log'),
                                  logging.StreamHandler()],
                        level=logging.INFO)
    metadata = MySQLMetadata(read_yaml_config(METADATA_CONFIG_FILE))
    metadata.connect_db()

    # handle interactive, interactive + dry run, and non-interactive modes
    if len(sys.argv) == 1:
        iq = InteractiveQuery(action, dry_mode)
        options = iq.get_interactive_parameters(metadata)
    elif len(sys.argv) == 2 and sys.argv[1] == "--execute":
        dry_mode = False
        iq = InteractiveQuery(action, dry_mode)
        options = iq.get_interactive_parameters(metadata)
    else:
        iq = InteractiveQuery(action, dry_mode)
        options = iq.get_commandline_parameters(metadata)

    file_list = iq.search_files(metadata, options)
    metadata.close_db()  # close it as user can be a long time with session open

    if len(file_list) == 0:
        logger.warning('No file was found that matched the given criteria, exiting.')
        sys.exit(4)
    iq.print_and_confirm_action(file_list)
    physically_deleted_files = iq.delete_files(file_list)
    metadata = MySQLMetadata(read_yaml_config(METADATA_CONFIG_FILE))
    metadata.connect_db()
    metadata.mark_as_deleted(physically_deleted_files, dry_mode=dry_mode)
    metadata.close_db()
    iq.cleanup()


if __name__ == "__main__":
    main()
