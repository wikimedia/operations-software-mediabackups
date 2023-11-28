"""
Implements the MySQLConnectionError, MySQLQueryError and MySQLMetadata classes
"""
import logging
import os
import urllib.parse

import pymysql

from mediabackups.File import File


class ReadDictionaryException(Exception):
    """
    Exception generated after a dictionary was attempted to be filled in
    with MySQL data and it failed to do so, or returned 0 rows
    """


class MySQLConnectionError(Exception):
    """Exception generated after MySQL has a connection problem"""


class MySQLQueryError(Exception):
    """Exception generated after MySQL has a query problem"""


DEFAULT_CONFIG_FILE = '/etc/mediabackup/mediabackups_db.ini'
DEFAULT_BATCH_SIZE = 1000


class MySQLMetadata:
    """Create, update and query MySQL backup metadata to manage media backups"""

    def __init__(self, config):
        """Constructor"""
        self.config_file = config.get('config_file', DEFAULT_CONFIG_FILE)
        self.batchsize = int(config.get('batchsize', DEFAULT_BATCH_SIZE))
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

    def _swift2url(self, status, container, path):
        """
        Returns the public url of a given file with the production status, container name and path.
        if there is some kind of error (e.g. a deleted file is provided, or a malformed or impossible
        container), None is returned. Otherwise a string with the https of the public download
        is returned.
        """
        if status == 'deleted':
            return None
        base_url = 'https://upload.wikimedia.org/'
        container_tokens = container.split('-')
        if len(container_tokens) < 2:
            return None
        return (base_url + urllib.parse.quote(container_tokens[0]) + '/' + urllib.parse.quote(container_tokens[1])
                + '/' + urllib.parse.quote(path))

    def _query_backups(self, query, parameters):
        """
        Returns a list of files with the given upload name in the mediabackups metadata
        database.
        """
        query_backups = """SELECT wiki_name, upload_name, storage_container_name, storage_path,
                                files.id as file_id, files.sha1 as sha1, file_types.Type_name as file_type,
                                backups.sha256 as sha256, size, status_name,
                                upload_timestamp,  archived_timestamp, deleted_timestamp,
                                backup_status_name, backup_time, endpoint_url
                           FROM backups
                           JOIN wikis ON backups.wiki = wikis.id
                           JOIN locations ON backups.location = locations.id
                           LEFT JOIN files ON files.wiki = backups.wiki AND files.sha1 = backups.sha1
                           LEFT JOIN storage_containers ON files.storage_container = storage_containers.id
                           LEFT JOIN file_status ON files.status = file_status.id
                           LEFT JOIN backup_status ON files.backup_status = backup_status.id
                           LEFT JOIN file_types ON files.file_type = file_types.id
                           WHERE """
        end_where = " AND backup_status_name IN ('backedup', 'duplicate')"
        order_by = " ORDER BY upload_name, status, upload_timestamp, archived_timestamp, deleted_timestamp"
        query = query_backups + query + end_where + order_by
        logger = logging.getLogger('backup')
        logger.debug(query)
        _, rows = self.query_and_fetchall(query, parameters)
        backups = list()
        non_public_wikis = self.get_non_public_wikis()
        for row in rows:
            wiki = row['wiki_name'].decode('utf-8')
            sha256 = row['sha256'].decode('utf-8')
            backup_path = os.path.join(wiki, sha256[:3], sha256)
            if wiki in non_public_wikis:
                backup_path += ".age"
            backups.append({
                '_file_id': row['file_id'],
                'wiki': wiki,
                'title': row['upload_name'].decode('utf-8'),
                'production_container': row['storage_container_name'].decode('utf-8'),
                'production_path': row['storage_path'].decode('utf-8'),
                'sha1': row['sha1'].decode('utf-8'),
                'sha256': row['sha256'].decode('utf-8'),
                'size': row['size'],
                'type': row['file_type'].decode('utf-8') if row['file_type'] is not None else None,
                'production_status': row['status_name'].decode('utf-8'),
                'production_url': self._swift2url(row['status_name'].decode('utf-8'),
                                                  row['storage_container_name'].decode('utf-8'),
                                                  row['storage_path'].decode('utf-8')),
                'upload_date': row['upload_timestamp'],
                'archive_date': row['archived_timestamp'],
                'delete_date': row['deleted_timestamp'],
                'backup_status': row['backup_status_name'].decode('utf-8'),
                'backup_date': row['backup_time'],
                'backup_location': row['endpoint_url'].decode('utf-8'),
                'backup_container': 'mediabackups',
                'backup_path': backup_path
            })
        return backups

    def is_valid_wiki(self, wiki):
        """
        Returns True if the given string is a valid wiki on the mediabackups metadata
        database, False otherwise.
        """
        query = """SELECT 1 FROM wikis
                   WHERE wiki_name = %s"""
        _, rows = self.query_and_fetchall(query, (wiki, ))
        return len(rows) == 1

    def get_non_public_wikis(self):
        """
        Returns a list of private, closed or deleted wikis which should use encryption when backed up.
        We add closed & deleted wikis to the list, as it is unclear if those are private or not.
        """
        query = """SELECT wiki_name FROM wikis
                   JOIN wiki_types ON wikis.type = wiki_types.id
                   WHERE type_name <> 'public' ORDER BY wiki_name"""
        _, rows = self.query_and_fetchall(query)

        return [row['wiki_name'].decode('utf-8') for row in rows]

    def process_files(self):
        """
        Yields a list of, at each time (at most), batchsize files "pending" to
        process and marks them in state "processing". They are returned in a
        dictionary of Files, keyed by a unique identifier
        """
        logger = logging.getLogger('backup')
        (_, _, _, _,
         numeric_backup_status,
         string_wiki, string_type, string_status, string_container,
         _) = self.get_fks()

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
            result, rows = self.query_and_fetchall(select_query, (numeric_backup_status['pending'], ), commit=False)
            if len(rows) <= 0:
                break
            files = dict()
            for row in rows:
                files[row['id']] = File.row2File(row, string_wiki, string_type, string_status,
                                                 string_container)
            update_query = """UPDATE files
                              SET backup_status = %s
                              WHERE id IN ({})""".format(', '.join([str(i) for i in files]))
            result, rows = self.query_and_fetchall(update_query, (numeric_backup_status['processing'], ))
            if result != len(files):
                logger.error('The number of rows updated (%s) was different '
                             'than expected (%s)',
                             result,
                             len(files))
                raise MySQLQueryError
            yield files

    def update_status(self, file_list):
        """
        Updates the status of a file list, an array of dictionaries with the
        following structure:
        {'id': unique numeric row id,
         'file': file object,
         'status': string with the new status ('pending', 'processing', etc.),
         'location': numeric id where the backup was sent to (usually, the id of a server)-
                     only for the backedup transition
        }
        """
        logger = logging.getLogger('backup')
        (numeric_wiki, _, _, _,
         numeric_backup_status,
         _, _, _, _, _) = self.get_fks()
        update_query = 'UPDATE files SET backup_status = %s WHERE id = %s'
        insert_query = """INSERT into backups (location, wiki, sha256, sha1)
                          VALUES (%s, %s, %s, %s)"""
        for file_dictionary in file_list:
            file_id = file_dictionary['id']
            backup_status = file_dictionary['status']
            parameters = (numeric_backup_status[backup_status], file_id)
            result, _ = self.query_and_fetchall(update_query, parameters)
            if result != 1:
                logger.error('Expecting to update 1 row, but %s were affected',
                             result)
                raise MySQLQueryError
            if backup_status == 'backedup':
                f = file_dictionary['file']
                parameters = (file_dictionary['location'], numeric_wiki[f.wiki], f.sha256, f.sha1)
                try:
                    result, _ = self.query_and_fetchall(insert_query, parameters)
                except pymysql.err.IntegrityError:
                    logger.warning('A file with the same sha256 (%s) was already uploaded '
                                   'at the same time to the same wiki', f.sha256)
                except MySQLQueryError:
                    logger.error('A MySQL error occurred again while inserting the '
                                 'upload %s on the backups log for id %s',
                                 str(f),
                                 file_id)
                    return -1
        return 0

    def read_dictionary_from_db(self, query):
        """
        Returns a dictionary from the 2-column query given, with the keys from
        the first row, and the values from the second. An exception if the
        dictionary contains no rows.
        """
        try:
            result, rows = self.query_and_fetchall(query)
        except MySQLQueryError:
            raise ReadDictionaryException from MySQLQueryError
        if len(rows) <= 0:
            raise ReadDictionaryException
        return {row['name'].decode('utf-8'): int(row['id']) for row in rows}

    def get_fks(self):
        """
        Queries normalized tables to get the latest values of the foreign keys,
        so a more efficient storage can be achived.
        The following tables are loaded into memory:
        * wikis
        * file_types
        * storage_containers (swift container names)
        * file_status
        * backup_status
        as well as the inverted dictionaries (string -> numeric)
        The results are returned as a list dictionaries, in the above order.
        """
        logger = logging.getLogger('backup')
        logger.info('Reading foreign key values for the files table from the database')
        wikis = self.read_dictionary_from_db('SELECT wiki_name as name, id FROM wikis')
        file_types = self.read_dictionary_from_db('SELECT type_name as name, id FROM file_types')
        file_status = self.read_dictionary_from_db('SELECT status_name as name, id FROM file_status')
        storage_containers = self.read_dictionary_from_db(
            'SELECT storage_container_name as name, id FROM storage_containers'
        )
        backup_status = self.read_dictionary_from_db(
            'SELECT backup_status_name as name, id FROM backup_status'
        )
        string_wiki = {v: k for (k, v) in wikis.items()}
        string_file_types = {v: k for (k, v) in file_types.items()}
        string_status = {v: k for (k, v) in file_status.items()}
        string_container = {v: k for (k, v) in storage_containers.items()}
        string_backup_status = {v: k for (k, v) in backup_status.items()}

        return (wikis, file_types, file_status, storage_containers, backup_status,
                string_wiki, string_file_types, string_status, string_container,
                string_backup_status)

    def query_and_fetchall(self, query, parameters=None, commit=True):
        """
        Given a query for the object open database connection, query and fetch
        all records into memory. Retry once by reconnecting if the query fails.
        Return a list of dictionaries with all results
        """
        logger = logging.getLogger('backup')
        cursor = self.db.cursor(pymysql.cursors.DictCursor)
        logger.debug('Executing the query: "%s" with parameters: %s', query, parameters)
        try:
            result = cursor.execute(query, parameters)
        # handle potential loss of connection
        except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
            logger.warning('A MySQL error occurred while executing query, '
                           'retrying connection')
            self.connect_db()
            cursor = self.db.cursor(pymysql.cursors.DictCursor)
            try:
                result = cursor.execute(query, parameters)
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError) as mysql_error:
                logger.error('A MySQL error occurred again after reconnecting '
                             'and querying the table, aborting')
                raise MySQLQueryError from mysql_error
        rows = cursor.fetchall()
        if commit:
            self.db.commit()
        cursor.close()
        return result, rows

    def update(self, files):
        """
        Given a dictionary of files {id: File, }, update the records containing
        them on the database.
        """
        logger = logging.getLogger('backup')
        if len(files) == 0:
            logger.warning('Zero files to update, doing nothing')
            return 0
        (_, numeric_type, numeric_status,
         numeric_container,
         numeric_backup_status,
         _, _, _, string_container,
         string_backup_status) = self.get_fks()

        fields = sorted(files[list(files.keys())[0]].properties().keys())
        field_string_with_backup_status = ', '.join(fields + ['backup_status'])
        field_string_with_file_id = ', '.join(['file_id'] + fields)
        field_string_with_id = ', '.join(['id'] + fields)
        success = dict()
        for file_id in files:
            f = files.get(file_id)
            # check existing file record
            query = f'SELECT {field_string_with_backup_status} FROM files WHERE id = %s'
            parameters = (file_id, )
            result, rows = self.query_and_fetchall(query, parameters)
            if len(rows) != 1:
                logger.error('File %s with id %s not found on the list of files', str(f), str(file_id))
                continue
            row = rows[0]
            old_storage_container = string_container.get(row.get('storage_container'))
            old_storage_path = (row['storage_path'].decode('utf-8')
                                if row.get('storage_path') is not None else None)
            old_backup_status = string_backup_status.get(row.get('backup_status'))
            # copy to history
            query = (f'INSERT INTO file_history ({field_string_with_file_id}) '
                     f'SELECT {field_string_with_id} FROM files WHERE id = %s')
            parameters = (file_id)
            result, _ = self.query_and_fetchall(query, parameters)
            if result != 1:
                logger.error('File %s with id %s was unable to be copied to file_history table',
                             str(f), str(file_id))
                continue
            # update to latest properties
            if ((old_storage_container != f.storage_container or
                    old_storage_path != f.storage_path) and
                    old_backup_status == 'error'):
                query = """UPDATE files
                           SET    upload_name = %s,
                                  file_type = %s,
                                  status = %s,
                                  deleted_timestamp = %s,
                                  archived_timestamp = %s,
                                  storage_container = %s,
                                  storage_path = %s,
                                  backup_status = %s
                           WHERE  id = %s"""
                parameters = (f.upload_name, numeric_type.get(f.type), numeric_status.get(f.status),
                              f.deleted_timestamp, f.archived_timestamp,
                              numeric_container.get(f.storage_container) if f.storage_container else None,
                              f.storage_path,
                              numeric_backup_status.get('pending'), file_id)
            else:
                query = """UPDATE files
                           SET    upload_name = %s,
                                  file_type = %s,
                                  status = %s,
                                  deleted_timestamp = %s,
                                  archived_timestamp = %s,
                                  storage_container = %s,
                                  storage_path = %s
                           WHERE  id = %s"""
                parameters = (f.upload_name, numeric_type.get(f.type), numeric_status.get(f.status),
                              f.deleted_timestamp, f.archived_timestamp,
                              numeric_container.get(f.storage_container) if f.storage_container else None,
                              f.storage_path,
                              file_id)
            result, _ = self.query_and_fetchall(query, parameters)
            if result != 1:
                logger.error('File %s with id %s was unable to be updated on the files table',
                             str(f), str(file_id))
                continue
            logger.info('File %s was updated correctly and its old metadata moved to history', str(f))
            success[file_id] = True
        return len(success)

    def check_and_update(self, wiki, files):
        """
        Given a list of files from a given wiki, update existing records from
        the metadata database in a single transaction.
        """
        logger = logging.getLogger('backup')
        if len(files) == 0:
            logger.warning('Zero files to check, doing nothing')
            return 0
        (numeric_wiki, _, _, _, _,
         string_wiki, string_type, string_status, string_container, _) = self.get_fks()

        fields = ['id'] + sorted(files[0].properties().keys())
        sha1list = [f.sha1 for f in files]
        parameters = [numeric_wiki[wiki], ]
        parameters.extend(sha1list)
        field_string = ', '.join(fields)
        sha1list_string = ', '.join(['%s'] * len(sha1list))
        query = f"""SELECT {field_string} FROM files
                    WHERE wiki = %s AND sha1 is not NULL AND sha1 IN ({sha1list_string})"""
        _, rows = self.query_and_fetchall(query, parameters)
        matches = dict()
        # organize files to lookup by sha1
        for row in rows:
            existing_sha1 = row['sha1'].decode('utf-8') if row.get('sha1') is not None else None
            if existing_sha1 not in matches:
                matches[existing_sha1] = list()
            matches[existing_sha1].append({
                'id': row['id'],
                'file': File.row2File(row, string_wiki, string_type, string_status, string_container)
            })
        # check every file and see if it needs insertion or update
        files_to_add = list()
        files_to_update = dict()
        for f in files:
            matches_found = []
            if f.sha1 is None:
                continue
            if f.sha1 not in matches:
                files_to_add.append(f)
                continue
            for match in matches[f.sha1]:
                m = match['file']
                if (m.sha1 == f.sha1 and
                        m.size == f.size and
                        f.upload_timestamp is not None and
                        m.upload_timestamp == f.upload_timestamp):
                    matches_found.append(match)
            if len(matches_found) == 0:
                logger.warning('sha1 hash for %s is on the database, '
                               'but not matching record found', str(f))
                files_to_add.append(f)
            elif len(matches_found) > 1:
                logger.error('%s possible matches were found for %s, not updating it',
                             str(len(matches_found)),
                             str(f))
            else:
                match = matches_found[0]
                m = match['file']
                if ((m.status != f.status) or
                        (m.upload_name != f.upload_name) or
                        (m.type != f.type) or
                        (m.archived_timestamp != f.archived_timestamp) or
                        (m.deleted_timestamp != f.deleted_timestamp) or
                        (m.storage_container != f.storage_container) or
                        (m.storage_path != f.storage_path)):
                    files_to_update[match['id']] = f
                    logger.info('Scheduling update of %s (id %s) with %s', str(m), str(match['id']), str(f))
                else:
                    logger.debug('File %s is unchanged, not doing anything', str(f))
        logger.info('%s new files found on this batch', str(len(files_to_add)))
        logger.info('%s files that need update on this batch', str(len(files_to_update)))
        return self.update(files_to_update) + self.add(files_to_add)

    def add(self, files):
        """
        Given a list of files, insert them into the metadata database
        in a single transaction.
        """
        logger = logging.getLogger('backup')
        if len(files) == 0:
            logger.warning('Zero files added, doing nothing')
            return 0
        # build the optimized insert query
        (numeric_wiki, numeric_type, numeric_status, numeric_container, _,
         _, _, _, _, _) = self.get_fks()
        fields = sorted(files[0].properties().keys())
        query = 'INSERT INTO files ({}) VALUES '.format(', '.join(fields))
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
        result, _ = self.query_and_fetchall(query, parameters)
        if result != len(files):
            logger.error('Expecting to insert %s rows, '
                         'but %s were affected',
                         len(files),
                         result)
            raise MySQLQueryError
        logger.info('%s files were inserted correctly', str(result))
        return result

    def mark_as_deleted(self, files, dry_mode=True):
        """
        Given a list of files that had been just pysically deleted from backups, update
        its MySQL metadata to:
        * remove its entry from the backups table
        * Set its file entry as hard-deleted (it assumes all references for the wiki have
          to be removed)
        """
        logger = logging.getLogger('deletion')
        (numeric_wiki, _, file_status, _, _, _, _, _, _, _) = self.get_fks()
        errors = 0
        for f in files:
            if dry_mode:
                query = "SELECT 1 FROM backups WHERE wiki = %s AND sha256 = %s"
            else:
                query = "DELETE FROM backups WHERE wiki = %s AND sha256 = %s"
            parameters = (numeric_wiki[f['wiki']], f['sha256'])
            result, rows = self.query_and_fetchall(query, parameters)
            if (dry_mode and len(rows) != 1) or (not dry_mode and result != 1):
                logger.error('%s:%s failed to be deleted from backups metadata', f['wiki'], f['sha256'])
                errors += 1
            if dry_mode:
                query = "SELECT 1 FROM files WHERE id = %s"
                parameters = (f['_file_id'], )
            else:
                query = "UPDATE files SET status = %s WHERE id = %s"
                parameters = (file_status['hard-deleted'], f['_file_id'])
            result, rows = self.query_and_fetchall(query, parameters)
            if (dry_mode and len(rows) != 1) or (not dry_mode and result != 1):
                logger.error('Row file.id: %s failed to update its file metadata', f['_file_id'])
                errors += 1
        if errors > 0:
            logger.error('%s error(s) found while trying to update metadata', str(errors))
        elif dry_mode:
            logger.warning('Metadata update completed correctly, '
                           'but database not actually touched because we are in DRY MODE!')
        else:
            logger.info('Metadata update completed correctly- no database errors.')
        return errors

    def get_latest_upload_time(self, wiki):
        """
        Given a wiki db string, query the files table to search for the upload_timestamp
        of the latest file registered on metadata. Useful to start a new backup process from
        there.
        """
        logger = logging.getLogger('deletion')
        (numeric_wiki, _, file_status, _, _, _, _, _, _, _) = self.get_fks()
        query = """
        SELECT max(upload_timestamp) AS upload_timestamp
        FROM files
        WHERE wiki = %s
        AND status = %s
        """
        parameters = (numeric_wiki[wiki], file_status['public'])
        result, rows = self.query_and_fetchall(query, parameters)
        if len(rows) != 1:
            logger.error('Failed to query the time of the latest succesful backup for %s', wiki)
            return None
        latest_succesful_timestamp = rows[0]['upload_timestamp']
        logger.info('The latest upload time for a succesful backup of a public file in %s happened at: %s',
                    wiki, str(latest_succesful_timestamp))
        return latest_succesful_timestamp

    def connect_db(self):
        """
        Connect to the database to read the file tables
        """
        logger = logging.getLogger('backup')
        try:
            self.db = pymysql.connect(read_default_file=self.config_file)
        except pymysql.err.OperationalError as mysql_connection_error:
            self.db = None
            logger.error('We could not connect to mediabackups metadata db with config %s', self.config_file)
            raise MySQLConnectionError from mysql_connection_error

    def close_db(self):
        """
        Close db connections
        """
        self.db.close()
        self.db = None
