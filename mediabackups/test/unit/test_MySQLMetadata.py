"""
Tests the MySQLMetadata.py classes and methods
"""
import datetime
from unittest import TestCase
from unittest.mock import patch  # , MagicMock

import pymysql

from mediabackups.MySQLMetadata import MySQLMetadata, MySQLQueryError, ReadDictionaryException, MySQLConnectionError
from mediabackups.File import File


class Test_MySQLMetadata(TestCase):
    """test module for the implementation of the read and write of backup metadata in MySQL"""

    empty_config = {}
    normal_config = {
        'host': 'db1001.eqiad.wmnet',
        'port': 3314,
        'database': 'mediabackups',
        'user': 'mediabackup',
        'password': 'a_password',
        'batchsize': 5
    }

    def setUp(self):
        """Set up the tests."""
        self.mysql_metadata = MySQLMetadata(self.normal_config)
        self.maxDiff = None

    def test___init__(self):
        """Test constructor"""
        # check defaults
        mysql_metadata = MySQLMetadata(self.empty_config)
        self.assertEqual(vars(mysql_metadata), {
            'batchsize': 1000,
            'host': 'localhost',
            'port': 3306,
            'password': '',
            'database': 'mediabackups',
            'socket': None,
            'ssl': None,
            'user': 'root',
            'db': None
        })

        # check normal config
        mysql_metadata = MySQLMetadata(self.normal_config)
        self.assertEqual(vars(mysql_metadata), {
            'batchsize': 5,
            'host': 'db1001.eqiad.wmnet',
            'port': 3314,
            'password': 'a_password',
            'database': 'mediabackups',
            'socket': None,
            'ssl': None,
            'user': 'mediabackup',
            'db': None
        })

    def return_parameters(query, parameters):
        return (query, parameters)

    def test_list_backups_from_title(self):
        """test querying backups using a title"""
        with patch.object(self.mysql_metadata, '_query_backups', side_effect=Test_MySQLMetadata.return_parameters):
            query, parameters = self.mysql_metadata.list_backups_from_title('wiki', 'title')
            self.assertEqual(query, "wiki_name = %s AND upload_name = %s")
            self.assertEqual(parameters, ('wiki', 'title'))

    def test_list_backups_from_sha1(self):
        """test querying backups using a sha1 hash"""
        with patch.object(self.mysql_metadata, '_query_backups', side_effect=Test_MySQLMetadata.return_parameters):
            query, parameters = self.mysql_metadata.list_backups_from_sha1('wiki', 'sha1')
            self.assertEqual(query, "wiki_name = %s AND files.sha1 = %s")
            self.assertEqual(parameters, ('wiki', 'sha1'))

    def test_list_backups_from_sha256(self):
        """test querying backups using a sha256 hash"""
        with patch.object(self.mysql_metadata, '_query_backups', side_effect=Test_MySQLMetadata.return_parameters):
            query, parameters = self.mysql_metadata.list_backups_from_sha256('wiki', 'sha256')
            self.assertEqual(query, "wiki_name = %s AND sha256 = %s")
            self.assertEqual(parameters, ('wiki', 'sha256'))

    def test_list_backups_from_path(self):
        """test querying backups using a container name and path"""
        with patch.object(self.mysql_metadata, '_query_backups', side_effect=Test_MySQLMetadata.return_parameters):
            query, parameters = self.mysql_metadata.list_backups_from_path('wiki', 'container', 'path')
            self.assertEqual(query, "wiki_name = %s AND storage_container_name = %s AND storage_path = %s")
            self.assertEqual(parameters, ('wiki', 'container', 'path'))

    def test_list_backups_from_upload_date(self):
        """test querying backups using a date of upload"""
        with patch.object(self.mysql_metadata, '_query_backups', side_effect=Test_MySQLMetadata.return_parameters):
            query, parameters = self.mysql_metadata.list_backups_from_upload_date('wiki', 'date')
            self.assertEqual(query, "wiki_name = %s AND upload_timestamp = %s")
            self.assertEqual(parameters, ('wiki', 'date'))

    def test_list_backups_from_archive_date(self):
        """test querying backups using a date of archival"""
        with patch.object(self.mysql_metadata, '_query_backups', side_effect=Test_MySQLMetadata.return_parameters):
            query, parameters = self.mysql_metadata.list_backups_from_archive_date('wiki', 'date')
            self.assertEqual(query, "wiki_name = %s AND archive_timestamp = %s")
            self.assertEqual(parameters, ('wiki', 'date'))

    def test_list_backups_from_delete_date(self):
        """test querying backups using a date of deletion"""
        with patch.object(self.mysql_metadata, '_query_backups', side_effect=Test_MySQLMetadata.return_parameters):
            query, parameters = self.mysql_metadata.list_backups_from_delete_date('wiki', 'date')
            self.assertEqual(query, "wiki_name = %s AND deleted_timestamp = %s")
            self.assertEqual(parameters, ('wiki', 'date'))

    def test__swift2url(self):
        """test transforming a container path into a public url for public/archived files"""
        self.assertIsNone(self.mysql_metadata._swift2url('deleted', 'container', 'path'))
        self.assertIsNone(self.mysql_metadata._swift2url('public', 'nonSeparatedByHyphens', 'path'))
        self.assertIsNone(self.mysql_metadata._swift2url('archived', 'nonSeparatedByHyphens', 'path'))
        self.assertEqual(self.mysql_metadata._swift2url('public', 'only1-hyphen', 'path'),
                         'https://upload.wikimedia.org/only1/hyphen/path')
        self.assertEqual(self.mysql_metadata._swift2url('archived', 'only1-hyphen', 'path'),
                         'https://upload.wikimedia.org/only1/hyphen/path')
        self.assertEqual(self.mysql_metadata._swift2url('public', 'wikipedia-commons-local-public.70',
                                                        '7/70/Wikimedia_logo_family.png'),
                         'https://upload.wikimedia.org/wikipedia/commons/7/70/Wikimedia_logo_family.png')
        self.assertEqual(self.mysql_metadata._swift2url('archived', 'wikipedia-commons-local-public.14',
                                                        'archive/1/14/20091107214303!Wikimedia-logo-circle.svg'),
                         ('https://upload.wikimedia.org/wikipedia/commons/archive/'
                          '1/14/20091107214303%21Wikimedia-logo-circle.svg'))

    def test__query_backups(self):
        # empty result
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows:
            mock_rows.return_value = (0, [])
            self.assertEqual(self.mysql_metadata._query_backups('', ()), [])

        # public file on public wiki
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows:
            with patch.object(self.mysql_metadata, 'get_non_public_wikis') as mock_private:
                mock_private.return_value = ['privatewiki']
                mock_rows.return_value = (0, [{'upload_name': b'Test.jpg', 'wiki_name': b'commonswiki',
                                               'sha256': b'0000', 'file_id': -1,
                                               'storage_container_name': b'wikipedia-commons-0',
                                               'storage_path': b'Test.jpg', 'sha1': b'0', 'size': 0,
                                               'status_name': b'public',
                                               'file_type': b'image',
                                               'upload_timestamp': datetime.datetime(2023, 1, 31, 21, 34, 56),
                                               'archived_timestamp': None, 'deleted_timestamp': None,
                                               'backup_status_name': b'backedup',
                                               'backup_time': datetime.datetime(2023, 1, 31, 21, 39, 12),
                                               'endpoint_url': b'https://backup1004.eqiad.wmnet:9000',
                                               'backup_container': b'mediabackups',
                                               'backup_path': 'commonswiki/0000/0'}])
                self.assertEqual(self.mysql_metadata._query_backups('', ()),
                                 [{'title': 'Test.jpg', 'wiki': 'commonswiki', 'sha256': '0000', '_file_id': -1,
                                   'production_container': 'wikipedia-commons-0', 'production_path': 'Test.jpg',
                                   'sha1': '0', 'size': 0, 'production_status': 'public',
                                   'upload_date': datetime.datetime(2023, 1, 31, 21, 34, 56), 'archive_date': None,
                                   'delete_date': None, 'backup_status': 'backedup',
                                   'type': 'image',
                                   'backup_date': datetime.datetime(2023, 1, 31, 21, 39, 12),
                                   'backup_location': 'https://backup1004.eqiad.wmnet:9000',
                                   'backup_container': 'mediabackups', 'backup_path': 'commonswiki/000/0000',
                                   'production_url': 'https://upload.wikimedia.org/wikipedia/commons/Test.jpg'}])

        # archived file on private wiki
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows:
            with patch.object(self.mysql_metadata, 'get_non_public_wikis') as mock_private:
                mock_private.return_value = ['privatewiki']
                mock_rows.return_value = (0, [{'upload_name': b'Test.jpg', 'wiki_name': b'privatewiki',
                                               'sha256': b'1000', 'file_id': -1,
                                               'storage_container_name': b'wikipedia-commons-0',
                                               'storage_path': b'archive/20230131220211!Test.jpg',
                                               'sha1': b'1', 'size': 0, 'status_name': b'public',
                                               'upload_timestamp': datetime.datetime(2023, 1, 31, 21, 34, 56),
                                               'archived_timestamp': datetime.datetime(2023, 1, 31, 22, 2, 11),
                                               'deleted_timestamp': None, 'file_type': b'image',
                                               'backup_status_name': b'backedup',
                                               'backup_time': datetime.datetime(2023, 1, 31, 21, 39, 12),
                                               'endpoint_url': b'https://backup1004.eqiad.wmnet:9000',
                                               'backup_container': b'mediabackups'}])
                self.assertEqual(self.mysql_metadata._query_backups('', ()),
                                 [{'title': 'Test.jpg', 'wiki': 'privatewiki', 'sha256': '1000', '_file_id': -1,
                                   'production_container': 'wikipedia-commons-0',
                                   'production_path': 'archive/20230131220211!Test.jpg',
                                   'sha1': '1', 'size': 0, 'production_status': 'public',
                                   'type': 'image',
                                   'upload_date': datetime.datetime(2023, 1, 31, 21, 34, 56),
                                   'archive_date': datetime.datetime(2023, 1, 31, 22, 2, 11),
                                   'delete_date': None, 'backup_status': 'backedup',
                                   'backup_date': datetime.datetime(2023, 1, 31, 21, 39, 12),
                                   'backup_location': 'https://backup1004.eqiad.wmnet:9000',
                                   'backup_container': 'mediabackups', 'backup_path': 'privatewiki/100/1000.age',
                                   'production_url': ('https://upload.wikimedia.org/wikipedia/commons/archive/'
                                                      '20230131220211%21Test.jpg')}])

    def test_is_valid_wiki(self):
        """Test the uncached way to ensure a wiki is valid"""
        # true if 1 row is returned
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows:
            mock_rows.return_value = (0, [{'1': 1}])
            self.assertTrue(self.mysql_metadata.is_valid_wiki('testwiki'))
        # false if 0 rows are returned
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows:
            mock_rows.return_value = (0, [])
            self.assertFalse(self.mysql_metadata.is_valid_wiki('nonexistentwiki'))
        # false if 2 or more rows are returned
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows:
            mock_rows.return_value = (0, [{'1': 1}, {'1': 1}])
            self.assertFalse(self.mysql_metadata.is_valid_wiki('weirdwiki'))

    def test_get_non_public_wikis(self):
        """Test the way to get the non-public wiki list"""
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows:
            mock_rows.return_value = (0, [{'wiki_name': b'privatewiki'}, {'wiki_name': b'closedwiki'}])
            self.assertEqual(self.mysql_metadata.get_non_public_wikis(), ['privatewiki', 'closedwiki'])

    def test_process_files(self):
        """Test the way to get a batch of pending files and mark them as being processed"""
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks:
            mock_fks.return_value = ({}, {}, {}, {}, {'pending': 1, 'processing': 2},
                                     {1: 'commonswiki'}, {1: 'image', 2: 'video'},
                                     {1: 'public', 2: 'deleted'}, {1: 'container'}, {})
            mock_rows.side_effect = [(0, [{'id': 1, 'wiki': 1, 'upload_name': b'Test1.jpg', 'status': 1,
                                           'md5': b'0', 'sha1': b'0', 'sha256': b'0', 'storage_container': 1,
                                           'storage_path': b'Test1.jpg', 'file_type': 1},
                                          {'id': 2, 'wiki': 1, 'upload_name': b'Test2.jpg', 'status': 2,
                                           'md5': b'0', 'sha1': b'0', 'sha256': b'0', 'storage_container': 1,
                                           'storage_path': b'0.jpg', 'file_type': 1}]),
                                     (2, []),  # update
                                     (0, [{'id': 3, 'wiki': 1, 'upload_name': b'Test3.jpg', 'status': 1,
                                           'md5': b'0', 'sha1': b'0', 'sha256': b'0', 'storage_container': 1,
                                           'storage_path': b'Test3.jpg', 'file_type': 1}]),
                                     (1, []),  # update
                                     (0, [{'id': 4, 'wiki': 1, 'upload_name': b'Test4.jpg', 'status': 2,
                                           'md5': b'0', 'sha1': b'0', 'sha256': b'0', 'storage_container': 1,
                                           'storage_path': b'Test3.jpg', 'file_type': 1}]),
                                     (0, []),  # failed update
                                     (0, [])]  # end
            self.assertEqual(File(wiki='commonswiki', upload_name='Test1.jpg', status='public'),
                             File(wiki='commonswiki', upload_name='Test1.jpg', status='public'))
            self.assertEqual(next(self.mysql_metadata.process_files()),
                             {1: File(wiki='commonswiki',
                                      upload_name='Test1.jpg',
                                      status='public',
                                      md5='0',
                                      sha1='0',
                                      sha256='0',
                                      storage_container='container',
                                      storage_path='Test1.jpg',
                                      type='image'),
                              2: File(wiki='commonswiki',
                                      upload_name='Test2.jpg',
                                      status='deleted',
                                      md5='0',
                                      sha1='0',
                                      sha256='0',
                                      storage_container='container',
                                      storage_path='0.jpg',
                                      type='image')})
            self.assertEqual(next(self.mysql_metadata.process_files()),
                             {3: File(wiki='commonswiki',
                                      upload_name='Test3.jpg',
                                      status='public',
                                      md5='0',
                                      sha1='0',
                                      sha256='0',
                                      storage_container='container',
                                      storage_path='Test3.jpg',
                                      type='image')})
            self.assertRaises(MySQLQueryError, next, self.mysql_metadata.process_files())
            self.assertRaises(StopIteration, next, self.mysql_metadata.process_files())

    def test_update_status(self):
        """Test the update of a status batch"""
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks:
            mock_fks.return_value = ({'commonswiki': 1}, {}, {}, {},
                                     {'pending': 1, 'processing': 2, 'backedup': 3, 'error': 4},
                                     {}, {}, {}, {}, {})
            # empty list
            parameters = []
            self.assertEqual(self.mysql_metadata.update_status(parameters), 0)

            # regular run case
            mock_rows.side_effect = [(1, []),  # update to backed up status
                                     (1, []),  # insert to backup table
                                     (1, [])]  # update as error
            parameters = [{'id': 1,
                           'file': File(wiki='commonswiki',
                                        upload_name='Test1.jpg',
                                        sha1='0',
                                        sha256='0',
                                        status='public'),
                           'status': 'backedup',
                           'location': 1},
                          {'id': 2,
                           'file': File(wiki='commonswiki',
                                        upload_name='Test2.jpg',
                                        sha1='0',
                                        sha256='0',
                                        status='public'),
                           'status': 'error'}]
            self.assertEqual(self.mysql_metadata.update_status(parameters), 0)

            # one row was not updated correctly case
            mock_rows.side_effect = [(0, []),  # 0 rows updated, problem!
                                     (1, []),
                                     (1, [])]
            self.assertRaises(MySQLQueryError, self.mysql_metadata.update_status, parameters)

            # one row was not updated correctly case
            mock_rows.side_effect = [(2, []),  # two many rows updated, problem!
                                     (1, []),
                                     (1, [])]
            self.assertRaises(MySQLQueryError, self.mysql_metadata.update_status, parameters)

            # inserting returns a duplicate key
            mock_rows.side_effect = [(1, []),
                                     pymysql.err.IntegrityError,  # duplicate key, warning!
                                     (1, [])]
            self.assertEqual(self.mysql_metadata.update_status(parameters), 0)

            # inserting fails
            mock_rows.side_effect = [(1, []),  # two many rows updated, problem!
                                     MySQLQueryError,
                                     (1, [])]
            self.assertEqual(self.mysql_metadata.update_status(parameters), -1)

    def test_read_dictionary_from_db(self):
        """test loading a table into memory for fast access & storage optimization"""
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows:
            query = "SELECT id, name FROM my_table"

            # regular result
            mock_rows.return_value = (0, [{'name': b'name1', 'id': 1},
                                          {'name': b'name2', 'id': 2}])
            self.assertEqual(self.mysql_metadata.read_dictionary_from_db(query),
                             {'name1': 1, 'name2': 2})

            # empty result
            mock_rows.return_value = (0, [])
            self.assertRaises(ReadDictionaryException, self.mysql_metadata.read_dictionary_from_db, query)

            # db query error
            mock_rows.side_effect = MySQLQueryError
            self.assertRaises(ReadDictionaryException, self.mysql_metadata.read_dictionary_from_db, query)

    def test_get_fks(self):
        """test loading all relevant backup and file foreign keys into memory"""
        # empty case
        with patch.object(self.mysql_metadata, 'read_dictionary_from_db') as mock_dictionary:
            mock_dictionary.side_effect = [{}, {}, {}, {}, {}]
            self.assertEqual(self.mysql_metadata.get_fks(),
                             ({}, {}, {}, {}, {},
                              {}, {}, {}, {}, {}))

        # regular case
        with patch.object(self.mysql_metadata, 'read_dictionary_from_db') as mock_dictionary:
            mock_dictionary.side_effect = [{'commonswiki': 1, 'testwiki': 2},
                                           {'image': 1, 'video': 2},
                                           {'container1': 1, 'container2': 2},
                                           {'public': 1, 'archived': 2, 'deleted': 3},
                                           {'pending': 1, 'processing': 2, 'backedup': 3, 'error': 4}]
            self.assertEqual(self.mysql_metadata.get_fks(),
                             ({'commonswiki': 1, 'testwiki': 2},
                              {'image': 1, 'video': 2},
                              {'container1': 1, 'container2': 2},
                              {'public': 1, 'archived': 2, 'deleted': 3},
                              {'pending': 1, 'processing': 2, 'backedup': 3, 'error': 4},
                              {1: 'commonswiki', 2: 'testwiki'},
                              {1: 'image', 2: 'video'},
                              {1: 'container1', 2: 'container2'},
                              {1: 'public', 2: 'archived', 3: 'deleted'},
                              {1: 'pending', 2: 'processing', 3: 'backedup', 4: 'error'}))

    def test_query_and_fetchall(self):
        """Test the db query wrapper"""
        query = 'SELECT 1 as column'
        query_return = [{'column': 1}, ]

        # normal query
        with patch.object(self.mysql_metadata, 'db') as mock_db:
            mock_db.cursor.return_value.execute.return_value = 0  # affected rows
            mock_db.cursor.return_value.fetchall.return_value = query_return
            mock_db.cursor.return_value.commit.return_value = 0
            self.assertEqual(self.mysql_metadata.query_and_fetchall(query), (0, query_return))

        # connection lost once
        with patch.object(self.mysql_metadata, 'db') as mock_db, \
             patch.object(self.mysql_metadata, 'connect_db') as mock_connect:
            mock_connect.return_value = 0
            mock_db.cursor.return_value.execute.side_effect = [pymysql.err.ProgrammingError, 0]
            mock_db.cursor.return_value.fetchall.return_value = query_return
            mock_db.cursor.return_value.commit.return_value = 0
            mock_db.cursor.return_value.close.return_value = 0
            self.assertEqual(self.mysql_metadata.query_and_fetchall(query), (0, query_return))

        # query still fails after connection retry
        with patch.object(self.mysql_metadata, 'db') as mock_db, \
             patch.object(self.mysql_metadata, 'connect_db') as mock_connect:
            mock_connect.return_value = 0
            mock_db.cursor.return_value.execute.side_effect = [pymysql.err.ProgrammingError,
                                                               pymysql.err.ProgrammingError]
            self.assertRaises(MySQLQueryError, self.mysql_metadata.query_and_fetchall, query)

    def test_update(self):
        """Test updating backups from a given batch"""
        fks = ({}, {}, {},
               {'container1': 1, 'container2': 2},
               {'pending': 1, 'processing': 2, 'backedup': 3, 'error': 4},
               {}, {}, {},
               {1: 'container1', 2: 'container2'},
               {1: 'pending', 2: 'processing', 3: 'backedup', 4: 'error'})

        # empty list
        self.assertEqual(self.mysql_metadata.update({}), 0)

        # the file to update doesn't exist (no found - no updates)
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks:
            mock_fks.return_value = fks
            mock_rows.return_value = (0, [])
            files = {1: File(wiki='commonswiki',
                             upload_name='Test1.jpg',
                             status='public')}
            self.assertEqual(self.mysql_metadata.update(files), 0)

        # normal execution of 1 file update
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks:
            mock_fks.return_value = fks
            mock_rows.side_effect = [(0, [{'id': 2, 'wiki': 1, 'upload_name': b'Test2.jpg', 'status': 1}]),  # select
                                     (1, []),  # insert
                                     (1, [])]  # update
            files = {2: File(wiki='commonswiki',
                             upload_name='Test2.jpg',
                             status='public')}
            self.assertEqual(self.mysql_metadata.update(files), 1)

        # unable to archive old file value to the history table, but otherwise it works
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks:
            mock_fks.return_value = fks
            mock_rows.side_effect = [(0, [{'id': 3, 'wiki': 1, 'upload_name': b'Test3.jpg', 'status': 1}]),  # select
                                     (0, []),  # insert
                                     (1, [])]  # update
            files = {2: File(wiki='commonswiki',
                             upload_name='Test3.jpg',
                             status='public')}
            self.assertEqual(self.mysql_metadata.update(files), 0)

        # archived, but failed to update the latest value
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks:
            mock_fks.return_value = fks
            mock_rows.side_effect = [(0, [{'id': 4, 'wiki': 1, 'upload_name': b'Test4.jpg', 'status': 1}]),  # select
                                     (1, []),  # insert
                                     (0, [])]  # update

            files = {2: File(wiki='commonswiki',
                             upload_name='Test3.jpg',
                             status='public')}
            self.assertEqual(self.mysql_metadata.update(files), 0)

        # sets backup to pending if it has a new backup location and old status was an error
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks:
            mock_fks.return_value = fks
            mock_rows.side_effect = [(0, [{'id': 5, 'wiki': 1, 'upload_name': b'Test4.jpg',  # select
                                           'status': 1, 'backup_status': 4, 'storage_container': 1,
                                           'storage_path': b'Test4.jpg'}]),
                                     (1, []),  # insert
                                     (1, [])]  # update

            files = {5: File(wiki='commonswiki',
                             upload_name='Test4.jpg',
                             status='public',
                             storage_container='container2',
                             storage_path='Test5.jpg')}
            self.assertEqual(self.mysql_metadata.update(files), 1)

    def return_length(files):
        return len(files)

    def test_check_and_update(self):
        """tests the logic to update/add new files"""
        # no files to update
        self.assertEqual(self.mysql_metadata.check_and_update('commonswiki', []), 0)

        fks = ({'commonswiki': 1},
               {}, {}, {}, {},
               {1: 'commonswiki'},
               {1: 'image', 2: 'video', 3: 'audio'},
               {1: 'public', 2: 'archived', 3: 'deleted'},
               {1: 'container1', 2: 'container2'},
               {})
        files = [File(wiki='commonswiki',
                      upload_name='Test6.jpg',
                      status='public',
                      sha1='a',
                      upload_timestamp=datetime.datetime(2023, 2, 14, 11, 17, 1),
                      size=100,
                      type='image',
                      storage_container='container1',
                      storage_path='Test6.jpg'),
                 File(wiki='commonswiki',
                      upload_name='Test6.jpg',
                      status='public',
                      sha1=None)]

        # already existing, unchanged
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks:
            mock_fks.return_value = fks
            mock_rows.side_effect = [(0, [{'id': 1, 'sha1': b'a', 'wiki': 1, 'upload_name': b'Test6.jpg',
                                           'upload_timestamp': datetime.datetime(2023, 2, 14, 11, 17, 1),
                                           'archived_timestamp': None, 'deleted_timestamp': None,
                                           'status': 1, 'size': 100, 'md5': None, 'sha256': b'b',
                                           'file_type': 1,
                                           'storage_container': 1, 'storage_path': b'Test6.jpg'}])]
            self.assertEqual(self.mysql_metadata.check_and_update('commonswiki', files), 0)

        # existing under a different name
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks, \
             patch.object(self.mysql_metadata, 'update', new=Test_MySQLMetadata.return_length):
            mock_fks.return_value = fks
            mock_rows.side_effect = [(0, [{'id': 2, 'sha1': b'a', 'wiki': 1, 'upload_name': b'Another.jpg',
                                           'upload_timestamp': datetime.datetime(2023, 2, 14, 11, 17, 1),
                                           'archived_timestamp': None, 'deleted_timestamp': None,
                                           'status': 1, 'size': 100, 'md5': None, 'sha256': b'b',
                                           'file_type': 1,
                                           'storage_container': 1, 'storage_path': b'Test6.jpg'}])]
            self.assertEqual(self.mysql_metadata.check_and_update('commonswiki', files), 1)

        # already existing, updated status
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks, \
             patch.object(self.mysql_metadata, 'update', new=Test_MySQLMetadata.return_length):
            mock_fks.return_value = fks
            # archived -> public
            mock_rows.side_effect = [(0, [{'id': 1, 'sha1': b'a', 'wiki': 1, 'upload_name': b'Test6.jpg',
                                           'upload_timestamp': datetime.datetime(2023, 2, 14, 11, 17, 1),
                                           'archived_timestamp': datetime.datetime(2023, 2, 14, 11, 35, 1),
                                           'deleted_timestamp': None, 'file_type': 1,
                                           'status': 2, 'size': 100, 'md5': None, 'sha256': b'b',
                                           'storage_container': 1, 'storage_path': b'Test6.jpg'}])]
            self.assertEqual(self.mysql_metadata.check_and_update('commonswiki', files), 1)

        # more than 1 match with the same sha1, size and upload_date
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks:
            mock_fks.return_value = fks
            # multiple matches
            mock_rows.side_effect = [(0, [{'id': 1, 'sha1': b'a', 'wiki': 1, 'upload_name': b'Test6.jpg',
                                           'upload_timestamp': datetime.datetime(2023, 2, 14, 11, 17, 1),
                                           'archived_timestamp': datetime.datetime(2023, 2, 14, 11, 35, 1),
                                           'deleted_timestamp': None, 'file_type': 1,
                                           'status': 2, 'size': 100, 'md5': None, 'sha256': b'b',
                                           'storage_container': 1, 'storage_path': b'Test6.jpg'},
                                          {'id': 1, 'sha1': b'a', 'wiki': 1, 'upload_name': b'TestA.jpg',
                                           'upload_timestamp': datetime.datetime(2023, 2, 14, 11, 17, 1),
                                           'archived_timestamp': datetime.datetime(2023, 2, 14, 11, 35, 1),
                                           'deleted_timestamp': None, 'file_type': 1,
                                           'status': 2, 'size': 100, 'md5': None, 'sha256': b'c',
                                           'storage_container': 1, 'storage_path': b'TestB.jpg'}])]
            self.assertEqual(self.mysql_metadata.check_and_update('commonswiki', files), 0)

        # a file with the same sha1 hash exists but has a different size
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks, \
             patch.object(self.mysql_metadata, 'add', new=Test_MySQLMetadata.return_length):
            mock_fks.return_value = fks
            # different size!
            mock_rows.side_effect = [(0, [{'id': 1, 'sha1': b'a', 'wiki': 1, 'upload_name': b'Test6.jpg',
                                           'upload_timestamp': datetime.datetime(2023, 2, 14, 11, 17, 1),
                                           'archived_timestamp': None, 'deleted_timestamp': None,
                                           'file_type': 1,
                                           'status': 1, 'size': 200, 'md5': None, 'sha256': b'b',
                                           'storage_container': 1, 'storage_path': b'Test6.jpg'}])]
            self.assertEqual(self.mysql_metadata.check_and_update('commonswiki', files), 1)

        # new file
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks, \
             patch.object(self.mysql_metadata, 'add', new=Test_MySQLMetadata.return_length):
            mock_fks.return_value = fks
            # different sha1
            mock_rows.side_effect = [(0, [{'id': 1, 'sha1': b'b', 'wiki': 1, 'upload_name': b'Test6.jpg',
                                           'upload_timestamp': datetime.datetime(2023, 2, 14, 11, 17, 1),
                                           'archived_timestamp': None, 'deleted_timestamp': None,
                                           'status': 1, 'size': 100, 'md5': None, 'sha256': b'b',
                                           'file_type': 1,
                                           'storage_container': 1, 'storage_path': b'Test6.jpg'}])]
            self.assertEqual(self.mysql_metadata.check_and_update('commonswiki', files), 1)

    def test_add(self):
        """Test adding a new file to backup metadata"""
        fks = ({'commonswiki': 1},
               {'image': 1, 'video': 2, 'audio': 3},
               {'public': 1, 'archived': 2, 'deleted': 3},
               {'container1': 1, 'container2': 2},
               {}, {}, {}, {}, {}, {})

        files = [File(wiki='commonswiki',
                      upload_name='Test7.jpg',
                      status='public',
                      sha1='a',
                      type='image',
                      upload_timestamp=datetime.datetime(2023, 2, 14, 11, 17, 1),
                      size=100,
                      storage_container='container1',
                      storage_path='Test7.jpg')]

        # empty list
        self.assertEqual(self.mysql_metadata.add([]), 0)

        # add one file
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks:
            mock_fks.return_value = fks
            mock_rows.side_effect = [(1, [])]
            self.assertEqual(self.mysql_metadata.add(files), 1)

        # one file failed to be inserted
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks:
            mock_fks.return_value = fks
            mock_rows.side_effect = [(0, [])]
            self.assertRaises(MySQLQueryError, self.mysql_metadata.add, files)

    def test_mark_as_deleted(self):
        """Test marking a file as hard-deleted on metadata"""
        fks = ({'commonswiki': 1},
               {},
               {'public': 1, 'archived': 2, 'deleted': 3, 'hard-deleted': 4},
               {}, {}, {}, {}, {}, {}, {})
        files = [{'_file_id': 1,
                  'wiki': 'commonswiki',
                  'upload_name': 'Test8.jpg',
                  'status': 'public',
                  'sha1': 'a',
                  'sha256': 'b',
                  'type': 'image',
                  'upload_timestamp': datetime.datetime(2023, 2, 14, 11, 17, 1),
                  'size': 100,
                  'storage_container': 'container1',
                  'storage_path': 'Test8.jpg'}]

        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks:
            mock_fks.return_value = fks
            # one file gets deleted successfully
            mock_rows.side_effect = [(1, []), (1, [])]
            self.assertEqual(self.mysql_metadata.mark_as_deleted(files, dry_mode=False), 0)

            # dry_run
            mock_rows.side_effect = [(0, [{'id': 1}]), (0, [{'id': 1}])]
            self.assertEqual(self.mysql_metadata.mark_as_deleted(files), 0)

            # one file fails to be deleted from the backups table
            mock_rows.side_effect = [(0, []), (1, [])]
            self.assertEqual(self.mysql_metadata.mark_as_deleted(files, dry_mode=False), 1)

            # one file fails to be updated in the files table
            mock_rows.side_effect = [(1, []), (0, [])]
            self.assertEqual(self.mysql_metadata.mark_as_deleted(files, dry_mode=False), 1)

    def test_connect_db(self):
        """test db connection interface"""
        with patch('pymysql.connect') as mock_connect:
            # succesful connect
            mock_connect.return_value = 1
            self.mysql_metadata.connect_db()
            self.assertEqual(self.mysql_metadata.db, 1)

            # connection failed
            mock_connect.side_effect = pymysql.err.OperationalError
            self.assertRaises(MySQLConnectionError, self.mysql_metadata.connect_db)

    def test_close_db(self):
        """test db connection closing interface"""
        with patch.object(self.mysql_metadata, 'db') as mock_db:
            mock_db.close.return_value = 0
            self.mysql_metadata.close_db()
            self.assertIsNone(self.mysql_metadata.db)

    def test_get_latest_upload_time(self):
        """test querying the latest upload time"""
        fks = ({'commonswiki': 1},
               {},
               {'public': 1, 'archived': 2, 'deleted': 3, 'hard-deleted': 4},
               {}, {}, {}, {}, {}, {}, {})
        with patch.object(self.mysql_metadata, 'query_and_fetchall') as mock_rows, \
             patch.object(self.mysql_metadata, 'get_fks') as mock_fks:
            # regular query
            latest_date = datetime.datetime(2023, 12, 31, 13, 45, 55)
            mock_fks.return_value = fks
            # one file gets deleted successfully
            mock_rows.return_value = (0, [{'upload_timestamp': latest_date}, ])
            self.assertEqual(self.mysql_metadata.get_latest_upload_time('commonswiki'), latest_date)

            # no rows returned
            mock_rows.return_value = (0, [])
            self.assertIsNone(self.mysql_metadata.get_latest_upload_time('commonswiki'))
