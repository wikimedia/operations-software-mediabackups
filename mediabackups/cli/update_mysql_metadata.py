#!/usr/bin/python3
"""read list of files from mw database tables and update existing records on the internal mediabackups tables"""

import logging
import os

from mediabackups.MySQLMedia import MySQLMedia
from mediabackups.MySQLMetadata import MySQLMetadata
from mediabackups.Util import read_yaml_config, read_dblist

READ_CONFIG_FILE = '/etc/mediabackup/mw_db.conf'
WRITE_CONFIG_FILE = '/etc/mediabackup/mediabackups_db.conf'
DEFAULT_DBLIST_PATH = '/srv/mediawiki-config/dblists'


def main():
    """Read configuration, connect to databases and start reading and writing in batches"""
    logger = logging.getLogger('backup')
    logging.basicConfig(format='[%(asctime)s] %(levelname)s:%(name)s %(message)s',
                        filename='backup_read.log', level=logging.INFO)
    config = read_yaml_config(READ_CONFIG_FILE)

    wikis = []
    # list all wikis
    for section, section_properties in config.get('sections', {}).items():
        wikis.extend(read_dblist(os.path.join(config.get('dblists_path', DEFAULT_DBLIST_PATH),
                                              section_properties.get('dblist', section + '.dblist'))))
    logger.info('About to process %s wikis.', str(len(wikis)))
    for wiki in wikis:
        logger.info('------------------------------------------------')
        logger.info(' Updating metadata from %s...', wiki)
        logger.info('------------------------------------------------')
        backup = MySQLMedia(config)
        metadata = MySQLMetadata(config=read_yaml_config(WRITE_CONFIG_FILE))
        backup.connect_db(wiki)
        if backup.wiki is None:
            logger.error("Skipping processing of wiki: %s", wiki)
            continue
        metadata.connect_db()

        for table_source in ['image', 'oldimage', 'filearchive']:
            logger.info('')
            logger.info('=================== %s ===================', table_source)
            for batch in backup.list_files(table_source=table_source):
                logger.info("Starting to process batch starting with file %s", str(batch[0]))
                for f in batch:
                    logger.debug(f)
                metadata.check_and_update(wiki, batch)
                logger.info("Batch processed until file %s", str(batch[-1]))
        backup.close_db()
        metadata.close_db()
        logger.info('Finished processing %s', wiki)
    logger.info('Finished processing all wikis')


if __name__ == "__main__":
    main()
