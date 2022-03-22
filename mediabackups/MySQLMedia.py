"""
This module implements the logic needed to query the list of available
production media files directly from the production wikis databases.
"""

import logging
from datetime import datetime

import pymysql

from mediabackups.File import File
from mediabackups.SwiftMedia import SwiftMedia
from mediabackups.Util import base36tobase16

DEFAULT_BATCH_SIZE = 100


class MySQLConnectionError(Exception):
    """Exception generated after MySQL has a connection problem"""
    pass


class MySQLQueryError(Exception):
    """Exception generated after MySQL has a query problem"""
    pass


class MySQLMedia:
    """Prepare and generate a media backup"""

    def __init__(self, config):
        """Constructor"""
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 3306)
        self.socket = config.get('socket', None)
        self.wiki = config.get('wiki', 'testwiki')
        self.user = config.get('user', 'root')
        self.password = config.get('password', '')
        self.ssl = config.get('ssl', None)
        self.batchsize = config.get('batchsize', DEFAULT_BATCH_SIZE)
        self.swift = SwiftMedia(config=config)
        self.db = None

    def _process_row(self, row):
        """
        Given the current files list processed, handle the row information
        in the array and return it, if successful
        """
        logger = logging.getLogger('backup')
        upload_name = (row['upload_name'].decode('utf-8')
                       if row['upload_name'] else None)
        size = row['size']
        type = (row['type'].decode('utf-8') if row['type'] else None)
        upload_timestamp = row['upload_timestamp']
        deleted_timestamp = row.get('deleted_timestamp', None)
        sha1 = (base36tobase16(row['sha1']) if row['sha1'] not in ('', b'')
                else None)
        md5 = None
        archived_date = None
        archived_timestamp = None
        storage_name = None
        if row.get('storage_path'):
            storage_name = row.get('storage_path').decode('utf-8')
        else:
            storage_name = upload_name
        if row['status'] == 'archived':
            if storage_name == '' or storage_name is None or '!' not in storage_name:
                archived_date = '19700101000001'
            else:
                archived_date = storage_name.split('!')[0]
            try:
                archived_timestamp = datetime.strptime(archived_date, '%Y%m%d%H%M%S')
            except ValueError:
                archived_timestamp = datetime.strptime('19700101000001', '%Y%m%d%H%M%S')
        storage_container, storage_path = self.swift.name2swift(image_name=upload_name,
                                                                status=row['status'],
                                                                archive_date=archived_date,
                                                                storage_name=storage_name,
                                                                sha1=sha1)
        # double check calculated name is the same as on the metadata db
        if (storage_path is not None and
                storage_name is not None and
                not storage_path.endswith(storage_name)):
            logger.warning('Retrieved storage name (%s) and calculated one (%s) '
                           'do not match',
                           storage_name,
                           storage_path)
        return File(wiki=self.wiki, upload_name=upload_name,
                    storage_container=storage_container,
                    storage_path=storage_path,
                    size=size, type=type, status=row['status'],
                    upload_timestamp=upload_timestamp,
                    deleted_timestamp=deleted_timestamp,
                    archived_timestamp=archived_timestamp,
                    sha1=sha1, md5=md5)

    def get_image_ranges(self):
        """Generate and return the ranges to obtain images in batches for wikis
           with large number of images, and iterate on them, returning at most
           self.batchsize images (less could be returned, depending how batches align)
           None skips the upper or lower bound.
        """
        if self.swift.isBigWiki():
            ranges = [None,
                      '0', '05',
                      '1', '15', '19',
                      '20', '2013', '2016', '2018', '2019', '2020',
                      '3', '4', '5', '6', '7', '8', '9']
            ranges.extend([x + y for x in list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
                           for y in list('0chmqt')])
            ranges.extend(['^', 'В', 'Л', 'С', 'Ե', '儀', None])
        else:
            ranges = [None, None]
        return ranges

    def list_files(self, table_source='public'):
        """
        Reads the list of all files on the given category of a wiki
        and returns an iterator of File objects
        """
        logger = logging.getLogger('backup')
        source = {
            'image':
                ("""SELECT 'public' as status,
                           img_name as upload_name,
                           img_name as storage_path,
                           img_size as size,
                           img_media_type as type,
                           STR_TO_DATE(img_timestamp, '%Y%m%d%H%i%s') as upload_timestamp,
                           NULL as archived_name,
                           NULL as deleted_timstamp,
                           img_sha1 as sha1
                    FROM image""",
                 ('img_name', )),
            'oldimage':
                ("""SELECT IF(oi_deleted, 'deleted', 'archived') as status,
                           oi_name as upload_name,
                           IF(oi_deleted,
                              CONCAT(oi_sha1, '.', SUBSTRING_INDEX(oi_name,'.',-1)),
                              oi_archive_name
                           ) as storage_path,
                           oi_size as size,
                           oi_media_type as type,
                           STR_TO_DATE(oi_timestamp, '%Y%m%d%H%i%s') as upload_timestamp,
                           oi_archive_name as archived_name,
                           NULL as deleted_timestamp,
                           oi_sha1 as sha1
                      FROM oldimage""",
                 ('oi_name', 'oi_archive_name')),
            'filearchive':
                ("""SELECT 'deleted' as status,
                           fa_name as upload_name,
                           fa_storage_key as storage_path,
                           fa_size as size,
                           fa_media_type as type,
                           STR_TO_DATE(fa_timestamp, '%Y%m%d%H%i%s') as upload_timestamp,
                           fa_archive_name as archived_name,
                           STR_TO_DATE(fa_deleted_timestamp, '%Y%m%d%H%i%s') as deleted_timestamp,
                           fa_sha1 as sha1
                      FROM filearchive""",
                 ('fa_name', 'fa_storage_key'))}

        ranges = self.get_image_ranges()
        general_query = source[table_source][0] + ' WHERE 1=1'
        ordering_cols = source[table_source][1]
        paging_col = ordering_cols[0]
        ordering = ' ORDER BY ' + ', '.join(['`' + col + '`' for col in ordering_cols])

        # MySQL query iteration (paging over title)
        for i in range(len(ranges) - 1):
            lower_limit = ranges[i]
            upper_limit = ranges[i + 1]
            lower_filter = (" AND `{}` >= '{}'".format(paging_col, lower_limit)
                            if lower_limit is not None else '')
            upper_filter = (" AND `{}` < '{}'".format(paging_col, upper_limit)
                            if upper_limit is not None else '')
            query = general_query + lower_filter + upper_filter + ordering
            cursor = self.db.cursor(pymysql.cursors.DictCursor)
            try:
                cursor.execute(query)
            # handle unexpected disconnects
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                logger.warning(('A MySQL error occurred while querying the table, '
                                'retrying connection'))
                self.connect_db()
                cursor = self.db.cursor(pymysql.cursors.DictCursor)
                try:
                    cursor.execute(query)
                except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                    logger.error(('A MySQL error occurred again while reconnecting '
                                 'and querying the table, aborting'))
                    break
            if cursor.rowcount is None or cursor.rowcount <= 0:
                continue
            # return results in batches of (at most) batchsize for processing
            while True:
                rows = cursor.fetchmany(self.batchsize)
                if not rows:
                    break
                files = list()
                for row in rows:
                    files.append(self._process_row(row))
                yield files
            cursor.close()

    def connect_db(self):
        """
        Connect to the database to read the file tables
        """
        logger = logging.getLogger('backup')
        try:
            self.db = pymysql.connect(host=self.host,
                                      port=self.port,
                                      unix_socket=self.socket,
                                      database=self.wiki,
                                      user=self.user,
                                      password=self.password,
                                      ssl=self.ssl)
        except pymysql.err.OperationalError as mysql_operational_error:
            logger.error('We could not connect to %s to retrieve the media '
                         'metainformation',
                         self.host)
            raise MySQLConnectionError from mysql_operational_error

    def close_db(self):
        """
        Close db connections
        """
        self.db.close()
