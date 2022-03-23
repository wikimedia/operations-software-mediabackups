#!/usr/bin/python3
"""read list of files from mw database tables and update existing records on the internal mediabackups tables"""

import logging
import os

from mediabackups.MySQLMedia import MySQLMedia
from mediabackups.MySQLMetadata import MySQLMetadata
from mediabackups.Util import read_yaml_config, read_dblist

READ_CONFIG_FILE = '/etc/mediabackup/mw_db.conf'
WRITE_CONFIG_FILE = '/etc/mediabackup/mediabackups_db.conf'
DBLIST_PATH = '/srv/mediawiki-config/dblists'


def main():
    """Read configuration, connect to databases and start reading and writing in batches"""
    logger = logging.getLogger('backup')
    logging.basicConfig(format='[%(asctime)s] %(levelname)s:%(name)s %(message)s',
                        filename='backup_read.log', level=logging.INFO)
    config = read_yaml_config(READ_CONFIG_FILE)
    if 'wiki' in config.keys():
        wikis = [config['wiki']]
    elif 'dblist' in config.keys():
        wikis = read_dblist(os.path.join(DBLIST_PATH, config['dblist']))
    else:
        raise Exception('Config doesn\'t contain a valid wiki or dblist definition.')
    for wiki in wikis:
        config['wiki'] = wiki
        logger.info('------------------------------------------------')
        logger.info(' Gathering metadata from %s...', wiki)
        logger.info('------------------------------------------------')
        backup = MySQLMedia(config)
        metadata = MySQLMetadata(config=read_yaml_config(WRITE_CONFIG_FILE))
        backup.connect_db()
        metadata.connect_db()

        for table_source in ['image', 'oldimage', 'filearchive']:
            logger.info('')
            logger.info('=================== %s ===================', table_source)
            for batch in backup.list_files(table_source=table_source):
                for f in batch:
                    logger.info(f)
                metadata.update(wiki, batch)
        backup.close_db()
        metadata.close_db()
        logger.info('Finished processing %s', wiki)


if __name__ == "__main__":
    main()
