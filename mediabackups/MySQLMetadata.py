"""
Implements the MySQLConnectionError, MySQLQueryError and MySQLMetadata classes
"""
import logging

import pymysql

from mediabackups.File import File


class MySQLConnectionError(Exception):
    """Exception generated after MySQL has a connection problem"""
    pass


class MySQLQueryError(Exception):
    """Exception generated after MySQL has a query problem"""
    pass


class MySQLMetadata:
    """Prepare and generate a media backup"""

    def __init__(self, config):
        """Constructor"""
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 3306)
        self.socket = config.get('socket', None)
        self.database = config.get('database', 'mediabackups')
        self.user = config.get('user', 'root')
        self.password = config.get('password', '')
        self.ssl = config.get('ssl', None)
        self.batchsize = int(config.get('batchsize', 1000))
        self.db = None

    def list_backups_from_title(self, wiki, title):
        """
        Returns a list of backups of a wiki with the given title
        from the mediabackups metadata db
        """
        query = "wiki_name = %s AND upload_name = %s"
        parameters = (wiki, title)
        return self._query_backups(query, parameters)

    def list_backups_from_sha1(self, wiki, sha1):
        """
        Returns a list of backups of a wiki with the given SHA1 hash
        from the mediabackups metadata db
        """
        query = "wiki_name = %s AND files.sha1 = %s"
        parameters = (wiki, sha1)
        return self._query_backups(query, parameters)

    def list_backups_from_sha256(self, wiki, sha256):
        """
        Returns a list of backups of a wiki with the given SHA256 hash
        from the mediabackups metadata db
        """
        query = "wiki_name = %s AND sha256 = %s"
        parameters = (wiki, sha256)
        return self._query_backups(query, parameters)

    def list_backups_from_path(self, wiki, container, path):
        """
        Returns a list of backups of a wiki with the given Swift
        container name and path from the mediabackups metadata db
        """
        query = "wiki_name = %s AND storage_container_name = %s AND storage_path = %s"
        parameters = (wiki, container, path)
        return self._query_backups(query, parameters)

    def list_backups_from_upload_date(self, wiki, date):
        """
        Returns a list of backups of a wiki that where uploaded
        on the given exact timestamp from the mediabackups metadata db
        """
        query = "wiki_name = %s AND upload_timestamp = %s"
        parameters = (wiki, date)
        return self._query_backups(query, parameters)

    def list_backups_from_archive_date(self, wiki, date):
        """
        Returns a list of backups of a wiki that where archived
        (that means, another file with the same name was uploaded)
        on the given exact timestamp from the mediabackups metadata db
        """
        query = "wiki_name = %s AND archive_timestamp = %s"
        parameters = (wiki, date)
        return self._query_backups(query, parameters)

    def list_backups_from_delete_date(self, wiki, date):
        """
        Returns a list of backups of a wiki that where soft-deleted
        on the given exact timestamp from the mediabackups metadata db
        """
        query = "wiki_name = %s AND deleted_timestamp = %s"
        parameters = (wiki, date)
        return self._query_backups(query, parameters)

    def _query_backups(self, query, parameters):
        """
        Returns a list of files with the given upload name in the mediabackups metadata
        database.
        """
        query_backups = """SELECT wiki_name, upload_name, storage_container_name, storage_path,
                                files.sha1, backups.sha256, size, status_name,
                                upload_timestamp,  archived_timestamp, deleted_timestamp,
                                backup_status_name, backup_time, endpoint_url
                           FROM backups
                           JOIN wikis ON backups.wiki = wikis.id
                           JOIN locations ON backups.location = locations.id
                           LEFT JOIN files ON files.wiki = backups.wiki AND files.sha1 = backups.sha1
                           LEFT JOIN storage_containers ON files.storage_container = storage_containers.id
                           LEFT JOIN file_status ON files.status = file_status.id
                           LEFT JOIN backup_status ON files.backup_status = backup_status.id
                           WHERE """
        end_where = " AND backup_status_name IN ('backedup', 'duplicate')"
        order_by = " ORDER BY upload_name, status, upload_timestamp, archived_timestamp, deleted_timestamp"
        query = query_backups + query + end_where + order_by
        logger = logging.getLogger('backup')
        logger.debug(query)
        cursor = self.db.cursor()
        try:
            cursor.execute(query, parameters)
        except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
            logger.warning('A MySQL error occurred while executing: %s, '
                           'trying to reconnect',
                           query)
            self.connect_db()
            try:
                cursor.execute(query, parameters)
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError) as mysql_error:
                logger.error('Query failed again after trying to reconnect')
                raise MySQLQueryError from mysql_error
        rows = cursor.fetchall()
        self.db.commit()
        cursor.close()
        backups = list()
        for row in rows:
            backups.append({
                'wiki': row[0].decode('utf-8'),
                'title': row[1].decode('utf-8'),
                'production_container': row[2].decode('utf-8'),
                'production_path': row[3].decode('utf-8'),
                'sha1': row[4].decode('utf-8'),
                'sha256': row[5].decode('utf-8'),
                'size': row[6],
                'production_status': row[7].decode('utf-8'),
                'upload_date': row[8],
                'archive_date': row[9],
                'delete_date': row[10],
                'backup_status': row[11].decode('utf-8'),
                'backup_date': row[12],
                'backup_location': row[13].decode('utf-8')
            })
        return backups

    def is_valid_wiki(self, wiki):
        """
        Returns True if the given string is a valid wiki on the mediabackups metadata
        database, False otherwise.
        """
        logger = logging.getLogger('backup')
        cursor = self.db.cursor()
        query = """SELECT 1 FROM wikis
                   WHERE wiki_name = %s"""
        try:
            cursor.execute(query, wiki)
        except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
            logger.warning('A MySQL error occurred while executing: %s, '
                           'trying to reconnect',
                           query)
            self.connect_db()
            try:
                cursor.execute(query, wiki)
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError) as mysql_error:
                logger.error('Query failed again after trying to reconnect')
                raise MySQLQueryError from mysql_error
        is_valid = (cursor.rowcount == 1)
        self.db.commit()
        cursor.close()
        return is_valid

    def get_non_public_wikis(self):
        """
        Returns a list of private, closed or deleted wikis which should use encryption when backed up.
        We add closed & deleted wikis to the list, as it is unclear if those are private or not.
        """
        logger = logging.getLogger('backup')
        cursor = self.db.cursor()
        query = """SELECT wiki_name FROM wikis
                   JOIN wiki_types ON wikis.type = wiki_types.id
                   WHERE type_name <> 'public' ORDER BY wiki_name"""
        try:
            cursor.execute(query)
        except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
            logger.warning('A MySQL error occurred while executing: %s, '
                           'trying to reconnect',
                           query)
            self.connect_db()
            try:
                cursor.execute(query)
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError) as mysql_error:
                logger.error('Query failed again after trying to reconnect')
                raise MySQLQueryError from mysql_error
        rows = cursor.fetchall()
        self.db.commit()
        cursor.close()
        return [row[0].decode('utf-8') for row in rows]

    def process_files(self):
        """
        Yields a list of, at each time (at most), batchsize files "pending" to
        process and marks them in state "processing". They are returned in a
        dictionary of Files, keyed by a unique identifier
        """
        logger = logging.getLogger('backup')
        (numeric_wiki, numeric_type, numeric_container,
         numeric_status, numeric_backup_status) = self.get_fks()
        string_wiki = {v: k for (k, v) in numeric_wiki.items()}
        string_container = {v: k for (k, v) in numeric_container.items()}
        string_status = {v: k for (k, v) in numeric_status.items()}

        select_query = """SELECT    id, wiki, upload_name,
                                    storage_container, storage_path,
                                    file_type, status, sha1, md5, size,
                                    upload_timestamp, archived_timestamp,
                                    deleted_timestamp
                          FROM      files
                          WHERE     backup_status = %s
                          ORDER BY  id ASC
                          LIMIT     {}
                          FOR UPDATE""".format(self.batchsize)
        while True:
            cursor = self.db.cursor(pymysql.cursors.DictCursor)
            try:
                cursor.execute(select_query, (numeric_backup_status['pending'], ))
            # handle potential loss of connection
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                logger.warning('A MySQL error occurred while selecting the list '
                               'of files, trying to reconnect.')
                self.connect_db()
                cursor = self.db.cursor(pymysql.cursors.DictCursor)
                try:
                    cursor.execute(select_query, (numeric_backup_status['pending'], ))
                except (pymysql.err.ProgrammingError, pymysql.err.InternalError) as mysql_error:
                    logger.error('A MySQL error occurred again while reconnecting '
                                 'and querying the table, aborting')
                    raise MySQLQueryError from mysql_error
            if cursor.rowcount is None or cursor.rowcount <= 0:
                break
            files = dict()
            rows = cursor.fetchall()
            for row in rows:
                files[row['id']] = File(wiki=string_wiki.get(row['wiki'], None),
                                        upload_name=(row['upload_name'].decode('utf-8')
                                                     if row['upload_name'] is not None else None),
                                        status=string_status.get(row['status'], None),
                                        storage_container=string_container.get(
                                            row['storage_container'], None
                                        ),
                                        storage_path=(row['storage_path'].decode('utf-8')
                                                      if row['storage_path'] is not None else None),
                                        sha1=(row['sha1'].decode('utf-8')
                                              if row['sha1'] is not None else None))
            cursor.close()
            update_query = """UPDATE files
                              SET backup_status = %s
                              WHERE id IN ({})""".format(', '.join([str(i) for i in files.keys()]))
            cursor = self.db.cursor(pymysql.cursors.DictCursor)
            try:
                result = cursor.execute(update_query, (numeric_backup_status['processing'], ))
            # handle potential loss of connection
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError) as mysql_error:
                logger.warning('A MySQL error occurred while making the list '
                               'of files "pending", trying to reconnect.')
                self.connect_db()
                cursor = self.db.cursor(pymysql.cursors.DictCursor)
                try:
                    result = cursor.execute(update_query, (numeric_backup_status['processing'], ))
                except (pymysql.err.ProgrammingError, pymysql.err.InternalError) as mysql_error_2:
                    logger.error('A MySQL error occurred again while reconnecting '
                                 'and updating the table, aborting')
                    raise MySQLQueryError from mysql_error_2
                if result != len(files):
                    logger.error('The number of rows updated (%s) was different '
                                 'than expected (%s)',
                                 result,
                                 len(files))
                    raise MySQLQueryError from mysql_error
            cursor.close()
            self.db.commit()  # unlock the rows

            yield files

    def update_status(self, file_list):
        """
        Updates the status of a file list, an array of dictionaries with the
        following structure:
        {id: numeric row id, file: file object, status: string with the new
        status ('pending', 'processing', etc.)}
        """
        logger = logging.getLogger('backup')
        (numeric_wiki, numeric_type, numeric_container,
         numeric_status, numeric_backup_status) = self.get_fks()
        update_query = 'UPDATE files SET backup_status = %s WHERE id = %s'
        insert_query = """INSERT into backups (location, wiki, sha256, sha1)
                          VALUES (%s, %s, %s, %s)"""
        for file_dictionary in file_list:
            file_id = file_dictionary['id']
            backup_status = file_dictionary['status']
            cursor = self.db.cursor(pymysql.cursors.DictCursor)
            try:
                result = cursor.execute(update_query,
                                        (numeric_backup_status[backup_status], file_id))
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                logger.error('A MySQL error occurred again while updating'
                             'the files table for file id %s',
                             file_id)
                return -1
            if result != 1:
                logger.error('Expecting to update 1 row, but %s were affected',
                             result)
                raise MySQLQueryError
            cursor.close()
            if backup_status == 'backedup':
                f = file_dictionary['file']
                cursor = self.db.cursor(pymysql.cursors.DictCursor)
                try:
                    result = cursor.execute(insert_query,
                                            (file_dictionary['location'],
                                             numeric_wiki[f.wiki],
                                             f.sha256,
                                             f.sha1))
                except pymysql.err.IntegrityError:
                    logger.warning('A file with the same sha256 (%s) was already uploaded '
                                   'at the same time to the same wiki', f.sha256)
                except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                    logger.error('A MySQL error occurred again while inserting the '
                                 'upload %s on the backups log for id %s',
                                 str(f),
                                 file_id)
                    return -1
                cursor.close()

        self.db.commit()
        return 0

    def read_dictionary_from_db(self, query):
        """
        Returns a dictionary from the 2-column query given, with the keys from
        the first row, and the values from the second
        """
        logger = logging.getLogger('backup')
        cursor = self.db.cursor()
        try:
            cursor.execute(query)
        except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
            logger.warning('A MySQL error occurred while executing: %s, '
                           'trying to reconnect',
                           query)
            self.connect_db()
            try:
                cursor.execute(query)
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError) as mysql_error:
                logger.error('Query failed again after trying to reconnect')
                raise MySQLQueryError from mysql_error
        rows = cursor.fetchall()
        self.db.commit()
        cursor.close()
        return {row[0].decode('utf-8'): int(row[1]) for row in rows}

    def get_fks(self):
        """
        Queries normalized tables to get the latest values of the foreign keys,
        so a more efficient storage can be achived.
        The following tables are loaded into memory:
        * wikis
        * file_types
        * storage_containers (swift container names)
        * file_status
        The results are returned as a list dictionaries, in the above order.
        """
        logger = logging.getLogger('backup')
        logger.info('Reading foreign key values for the files table from the database')
        wikis = self.read_dictionary_from_db('SELECT wiki_name, id FROM wikis')
        file_types = self.read_dictionary_from_db('SELECT type_name, id FROM file_types')
        storage_containers = self.read_dictionary_from_db(
            'SELECT storage_container_name, id FROM storage_containers'
        )
        file_status = self.read_dictionary_from_db('SELECT status_name, id FROM file_status')
        backup_status = self.read_dictionary_from_db(
            'SELECT backup_status_name, id FROM backup_status'
        )
        return wikis, file_types, storage_containers, file_status, backup_status

    def add(self, files):
        """
        Given a list of files, insert them into the metadata database
        in a single transaction.
        """
        logger = logging.getLogger('backup')
        if len(files) == 0:
            logger.warning('Zero files added, doing nothing')
            return None
        # build the optimized insert query
        (numeric_wiki, numeric_type, numeric_container,
         numeric_status, backup_status) = self.get_fks()
        fields = sorted(files[0].properties().keys())
        query = 'INSERT INTO files ({}) VALUES '.format(','.join(fields))
        inserts = list()
        parameters = list()
        for file in files:
            properties = file.properties()
            # override some strings with its numeric value
            properties['wiki'] = numeric_wiki[properties['wiki']]
            properties['file_type'] = numeric_type[properties['file_type']]
            properties['storage_container'] = numeric_container.get(
                properties['storage_container'],
                None
            )
            properties['status'] = numeric_status[properties['status']]
            inserts.append('(' + ', '.join(['%s'] * len(fields)) + ')')
            parameters.extend([properties[key] for key in sorted(properties.keys())])
        query += ', '.join(inserts)

        logger.info('About to insert %s files', len(files))
        cursor = self.db.cursor(pymysql.cursors.DictCursor)
        try:
            result = cursor.execute(query, parameters)
        # handle potential loss of connection
        except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
            logger.warning('A MySQL error occurred while inserting on the files, '
                           'retrying connection')
            self.connect_db()
            cursor = self.db.cursor(pymysql.cursors.DictCursor)
            try:
                cursor.execute(query)
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError) as mysql_error:
                logger.error('A MySQL error occurred again while reconnecting '
                             'and querying the table, aborting')
                raise MySQLQueryError from mysql_error
        if result != len(files):
            logger.error('Expecting to insert %s rows, '
                         'but %s were affected',
                         len(files),
                         result)
            raise MySQLQueryError
        self.db.commit()
        cursor.close()
        return result

    def connect_db(self):
        """
        Connect to the database to read the file tables
        """
        logger = logging.getLogger('backup')
        try:
            self.db = pymysql.connect(host=self.host,
                                      port=self.port,
                                      unix_socket=self.socket,
                                      database=self.database,
                                      user=self.user,
                                      password=self.password,
                                      ssl=self.ssl)
        except pymysql.err.OperationalError as mysql_connection_error:
            logger.error('We could not connect to %s to store the stats', self.host)
            raise MySQLConnectionError from mysql_connection_error

    def close_db(self):
        """
        Close db connections
        """
        self.db.close()
