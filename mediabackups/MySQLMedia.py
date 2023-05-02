"""
This module implements the logic needed to query the list of available
production media files directly from the production wikis databases.
"""
import datetime
import logging

import pymysql

from mediabackups.File import File
from mediabackups.SwiftMedia import SwiftMedia
from mediabackups.Util import base36tobase16, mwdate2datetime

DEFAULT_BATCH_SIZE = 100


class MySQLConnectionError(Exception):
    """Exception generated after MySQL has a connection problem"""
    pass


class MySQLQueryError(Exception):
    """Exception generated after MySQL has a query problem"""
    pass


class MySQLNotConnected(Exception):
    """Exception generated after trying to use list_files without connecting first"""
    pass


class MySQLMedia:
    """Prepare and generate a media backup"""
    source = {
        'image':
            ("""SELECT 'public' as status,
                        img_name as upload_name,
                        img_name as storage_path,
                        img_size as size,
                        img_media_type as type,
                        STR_TO_DATE(img_timestamp, '%%Y%%m%%d%%H%%i%%s') as upload_timestamp,
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
                        STR_TO_DATE(oi_timestamp, '%%Y%%m%%d%%H%%i%%s') as upload_timestamp,
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
                        STR_TO_DATE(fa_timestamp, '%%Y%%m%%d%%H%%i%%s') as upload_timestamp,
                        fa_archive_name as archived_name,
                        STR_TO_DATE(fa_deleted_timestamp, '%%Y%%m%%d%%H%%i%%s') as deleted_timestamp,
                        fa_sha1 as sha1
                   FROM filearchive""",
             ('fa_name', 'fa_storage_key'))}

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
        self.swift = SwiftMedia(config={'wiki': self.wiki, 'batchsize': self.batchsize})
        self.db = None

    def _process_row(self, row):
        """
        Given the current files list processed, handle the row information
        in the array and return it, if successful
        """
        logger = logging.getLogger('backup')
        upload_name = (row['upload_name'].decode('utf-8')
                       if row['upload_name'] else None)
        status = row['status']
        size = row['size']
        type = (row['type'].decode('utf-8') if row['type'] else None)
        upload_timestamp = row.get('upload_timestamp')
        deleted_timestamp = row.get('deleted_timestamp')
        sha1 = (base36tobase16(row['sha1']) if row['sha1'] not in ('', b'', None)
                else None)
        md5 = None
        archived_date = None
        archived_timestamp = None
        storage_name = None
        # deleted files may or may not have been previously archived, in which case,
        # we will try several strategies to get its previously archived name
        if row.get('storage_path'):
            storage_path = row.get('storage_path').decode('utf-8')
            storage_name = storage_path.rpartition('/')[-1]
        else:
            storage_name = upload_name
        if status == 'public':
            archived_timestamp = None
        else:
            if row.get('archived_name') is None:
                if storage_name == '' or storage_name is None or '!' not in storage_name:
                    archived_timestamp = None
                else:
                    archived_timestamp = mwdate2datetime(storage_name.split('!')[0])
            else:
                archived_name = row.get('archived_name').decode('utf-8')
                if archived_name == '' or archived_name is None or '!' not in archived_name:
                    archived_date = '19700101000001'
                else:
                    archived_date = archived_name.split('!')[0]
                archived_timestamp = mwdate2datetime(archived_date)
        storage_container, storage_path = self.swift.name2swift(image_name=upload_name,
                                                                status=status,
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
                    size=size, type=type, status=status,
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

    def query(self, query, parameters=None):
        """
        Given a query for the object open database connection, query and fetch
        all records in memory. Retry once by reconnecting if the query fails.
        Return a list of dictionaries with all results
        """
        logger = logging.getLogger('backup')
        cursor = self.db.cursor(pymysql.cursors.DictCursor)
        try:
            _ = cursor.execute(query, parameters)
        # handle potential loss of connection
        except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
            logger.warning('A MySQL error occurred while inserting on the files, '
                           'retrying connection')
            self.connect_db()
            cursor = self.db.cursor(pymysql.cursors.DictCursor)
            try:
                _ = cursor.execute(query, parameters)
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError) as mysql_error:
                logger.error('A MySQL error occurred again while reconnecting '
                             'and querying the table, aborting')
                raise MySQLQueryError from mysql_error
        return cursor

    def calculate_query(self, table_source):
        """
        Returns one or a sequence of queries to execute to return all needed rows that
        cover all of the table_source table rows, but without making such a large query
        that we run out of memory (aproximatelly < 500K rows per query). For this,
        we iterate over titles in alphabetical order. In cases where the primary key
        is composed, we will need extra columns to have a strict order.
        """
        ranges = self.get_image_ranges()
        general_query = self.source[table_source][0] + ' WHERE 1=1'
        ordering_cols = self.source[table_source][1]
        paging_col = ordering_cols[0]
        ordering = ' ORDER BY ' + ', '.join(['`' + col + '`' for col in ordering_cols])

        # small wikis will query the full table at once
        # larger wikis will create ranges such as titles '3XXXXX' to '4XXXX',
        # '4XXXX' to '5XXXX', etc.
        for i in range(len(ranges) - 1):
            lower_limit = ranges[i]
            upper_limit = ranges[i + 1]
            lower_filter = (f" AND `{paging_col}` >= '{lower_limit}'"
                            if lower_limit is not None else '')
            upper_filter = (f" AND `{paging_col}` < '{upper_limit}'"
                            if upper_limit is not None else '')
            yield general_query + lower_filter + upper_filter + ordering

    def list_files(self, table_source='image'):
        """
        Reads the list of all files on the given category of a wiki
        and returns an iterator of File objects
        """
        logger = logging.getLogger('backup')
        if self.db is None:
            logger.error('You must connect to the database before attempting to read tables')
            raise MySQLNotConnected

        # MySQL query iteration (paging over title)
        for query in self.calculate_query(table_source):
            cursor = self.query(query, tuple())  # hack needed for missbehaviour of not escaping %%
            if cursor.rowcount is None or cursor.rowcount <= 0:
                continue
            # return results in batches of (at most) batchsize for processing
            while True:
                try:
                    rows = cursor.fetchmany(self.batchsize)
                    if len(rows) > 0:
                        files = list()
                        for row in rows:
                            files.append(self._process_row(row))
                        yield files
                    else:
                        raise StopIteration
                except StopIteration:
                    cursor.close()
                    break
        return

    def query_files(self, batch):
        """
        Given a batch (list of recently uploaded titles, timestamps and sha1 hashes), return the list of File objects
        to be updated.
        """
        logger = logging.getLogger('backup')
        files = []
        self.db.autocommit(True)
        for upload in batch:
            # query db
            query = self.source['image'][0]
            query += ' WHERE img_name = %s AND img_timestamp = %s AND img_sha1 = %s'
            parameters = (upload['title'],
                          datetime.datetime.strftime(upload['upload_timestamp'], '%Y%m%d%H%M%S') if
                          upload['upload_timestamp'] is not None else None,
                          upload['sha1'])
            cursor = self.query(query, parameters)
            if cursor.rowcount == 1:
                # return results as a File object
                row = cursor.fetchall()[0]
                recent_file = self._process_row(row)
                logger.info('Checking if file %s has to be inserted or updated into the existing backups',
                            upload['title'])
                files.append(recent_file)
                cursor.close()
            else:
                logger.warning('File %s was not found on the metadata database- is there lag or another issue?',
                               upload['title'])
        return files

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
        self.db = None
