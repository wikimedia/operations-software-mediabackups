"""
Tests the MySQLMedia.py classes and methods
"""
import datetime

from unittest import TestCase
from unittest.mock import patch, MagicMock

from mediabackups.MySQLMedia import MySQLMedia
from mediabackups.File import File


class Test_MySQLMedia(TestCase):
    """test module implementing the raw read of mediawiki file-related MySQL tables"""

    empty_config = {}
    normal_config = {
        'host': 'db1001.eqiad.wmnet',
        'port': 3314,
        'wiki': 'commonswiki',
        'user': 'mediabackup',
        'password': 'a_password',
        'batchsize': 5
    }

    def setUp(self):
        """Set up the tests."""
        self.mysql_media = MySQLMedia(self.normal_config)

    def test___init__(self):
        """Test constructor"""
        mysql_media = MySQLMedia(self.empty_config)
        default_parameters = vars(mysql_media)
        default_parameters['swift'] = vars(default_parameters.get('swift'))
        self.assertEqual(vars(mysql_media), {
            'batchsize': 100,
            'host': 'localhost',
            'port': 3306,
            'password': '',
            'db': None,
            'socket': None,
            'ssl': None,
            'swift': {'wiki': 'testwiki', 'batchsize': 100},
            'user': 'root',
            'wiki': 'testwiki'
        })
        mysql_media = self.mysql_media
        default_parameters = vars(mysql_media)
        default_parameters['swift'] = vars(default_parameters.get('swift'))
        self.assertEqual(vars(mysql_media), {
            'batchsize': 5,
            'host': 'db1001.eqiad.wmnet',
            'port': 3314,
            'password': 'a_password',
            'db': None,
            'socket': None,
            'ssl': None,
            'swift': {'wiki': 'commonswiki', 'batchsize': 5},
            'user': 'mediabackup',
            'wiki': 'commonswiki'
        })

    def test__process_row(self):
        """Test converting a MySQL row into a File object"""
        # public with no storage path
        row = {'upload_name': b'Test.jpeg', 'size': 12, 'type': b'image/jpeg',
               'upload_timestamp': datetime.datetime(2022, 11, 30, 11, 25, 56),
               'sha1': b'0', 'status': 'public', 'wiki': 'commonswiki',
               'storage_path': b'/path/Test.jpeg'}
        container = ('container', '/path/Test.jpeg')
        f = File(wiki='commonswiki', upload_name='Test.jpeg',
                 storage_container=container[0],
                 storage_path=container[1],
                 size=12, type='image/jpeg', status='public',
                 upload_timestamp=datetime.datetime(2022, 11, 30, 11, 25, 56),
                 deleted_timestamp=None,
                 archived_timestamp=None,
                 sha1='0'*40, md5=None)
        swift_mock = MagicMock()
        swift_mock.return_value = container
        with patch.object(self.mysql_media.swift, 'name2swift', swift_mock):
            self.assertEqual(vars(self.mysql_media._process_row(row)), vars(f))
        # archived non-deleted with storage path
        row = {'upload_name': b'Test.jpeg', 'size': 12, 'type': b'image/jpeg',
               'upload_timestamp': datetime.datetime(2022, 11, 30, 11, 25, 56),
               'sha1': b'56le7dx4g21ssp3jyb0xc8a5vlk4fjt', 'status': 'archived',
               'wiki': 'commonswiki', 'storage_path': b'/path/20221130132556!Test.jpeg',
               'archived_name': b'20221130132556!Test.jpeg'}
        container = ('container', '/path/20221130132556!Test.jpeg')
        f = File(wiki='commonswiki', upload_name='Test.jpeg',
                 storage_container=container[0],
                 storage_path=container[1],
                 size=12, type='image/jpeg', status='archived',
                 upload_timestamp=datetime.datetime(2022, 11, 30, 11, 25, 56),
                 deleted_timestamp=None,
                 archived_timestamp=datetime.datetime(2022, 11, 30, 13, 25, 56),
                 sha1='2c5f4c5ff0e57ffcea85c1da92b4599336d75fb9', md5=None)
        swift_mock.return_value = container
        with patch.object(self.mysql_media.swift, 'name2swift', swift_mock):
            self.assertEqual(vars(self.mysql_media._process_row(row)), vars(f))
        # archived deleted with storage path
        row = {'upload_name': b'Test.jpeg', 'size': 12, 'type': b'image/jpeg',
               'upload_timestamp': datetime.datetime(2022, 11, 30, 11, 25, 56),
               'deleted_timestamp': datetime.datetime(2022, 11, 30, 14, 25, 56),
               'sha1': b'56le7dx4g21ssp3jyb0xc8a5vlk4fjt', 'status': 'deleted',
               'wiki': 'commonswiki', 'archived_name': b'20221130132556!Test.jpeg',
               'storage_path': b'/path/2c5f4c5ff0e57ffcea85c1da92b4599336d75fb9.jpeg'}
        container = ('container', '/path/2c5f4c5ff0e57ffcea85c1da92b4599336d75fb9.jpeg')
        f = File(wiki='commonswiki', upload_name='Test.jpeg',
                 storage_container=container[0],
                 storage_path=container[1],
                 size=12, type='image/jpeg', status='deleted',
                 upload_timestamp=datetime.datetime(2022, 11, 30, 11, 25, 56),
                 archived_timestamp=datetime.datetime(2022, 11, 30, 13, 25, 56),
                 deleted_timestamp=datetime.datetime(2022, 11, 30, 14, 25, 56),
                 sha1='2c5f4c5ff0e57ffcea85c1da92b4599336d75fb9', md5=None)
        swift_mock.return_value = container
        with patch.object(self.mysql_media.swift, 'name2swift', swift_mock):
            self.assertEqual(vars(self.mysql_media._process_row(row)), vars(f))
        # deleted non-archived
        row = {'upload_name': b'Test.jpeg', 'size': 12, 'type': b'image/jpeg',
               'upload_timestamp': datetime.datetime(2022, 11, 30, 11, 25, 56),
               'deleted_timestamp': datetime.datetime(2022, 11, 30, 14, 25, 56),
               'sha1': b'56le7dx4g21ssp3jyb0xc8a5vlk4fjt', 'status': 'deleted',
               'wiki': 'commonswiki', 'archived_name': None,
               'storage_path': b'/path/2c5f4c5ff0e57ffcea85c1da92b4599336d75fb9.jpeg'}
        container = ('container', '/path/2c5f4c5ff0e57ffcea85c1da92b4599336d75fb9.jpeg')
        f = File(wiki='commonswiki', upload_name='Test.jpeg',
                 storage_container=container[0],
                 storage_path=container[1],
                 size=12, type='image/jpeg', status='deleted',
                 upload_timestamp=datetime.datetime(2022, 11, 30, 11, 25, 56),
                 archived_timestamp=None,
                 deleted_timestamp=datetime.datetime(2022, 11, 30, 14, 25, 56),
                 sha1='2c5f4c5ff0e57ffcea85c1da92b4599336d75fb9', md5=None)
        swift_mock.return_value = container
        with patch.object(self.mysql_media.swift, 'name2swift', swift_mock):
            self.assertEqual(vars(self.mysql_media._process_row(row)), vars(f))

    def test_get_image_ranges(self):
        """Test that the ranges to query the file tables are reasonable"""
        swift_mock = MagicMock()
        # small wiki
        swift_mock.return_value = False
        with patch.object(self.mysql_media.swift, 'isBigWiki', swift_mock):
            self.assertEqual(self.mysql_media.get_image_ranges(), [None, None])
        # large wiki
        swift_mock.return_value = True
        with patch.object(self.mysql_media.swift, 'isBigWiki', swift_mock):
            ranges = self.mysql_media.get_image_ranges()
        self.assertEqual(ranges[0], None)
        self.assertEqual(ranges[1], '0')
        self.assertEqual(ranges[-1], None)
        self.assertEqual(ranges[-2], '儀')
        self.assertTrue('2020' in ranges)
        for limit in ranges:
            if limit is not None and limit[0] >= 'A' and limit[0] <= 'Z':
                self.assertTrue(limit[1] in '0chmqt')
        self.assertTrue('^' in ranges)

    def test_calculate_query(self):
        """Check queries used to retrieve (and maybe paging) all file data"""
        source = {'my_table': ('SELECT 1 FROM my_table', ('my_column', ))}
        with patch.object(self.mysql_media, 'source', source):
            with patch.object(self.mysql_media, 'get_image_ranges', return_value=[None, None]):
                query_iterable = self.mysql_media.calculate_query(table_source='my_table')
                self.assertEqual(next(query_iterable), "SELECT 1 FROM my_table WHERE 1=1 ORDER BY `my_column`")
                self.assertIsNone(next(query_iterable, None))
            with patch.object(self.mysql_media, 'get_image_ranges', return_value=[None, 'A', 'M', 'Z', '^', None]):
                query_iterable = self.mysql_media.calculate_query(table_source='my_table')
                self.assertEqual(next(query_iterable),
                                 "SELECT 1 FROM my_table WHERE 1=1 AND " +
                                 "`my_column` < 'A' ORDER BY `my_column`")
                self.assertEqual(next(query_iterable),
                                 "SELECT 1 FROM my_table WHERE 1=1 AND " +
                                 "`my_column` >= 'A' AND `my_column` < 'M' ORDER BY `my_column`")
                self.assertEqual(next(query_iterable),
                                 "SELECT 1 FROM my_table WHERE 1=1 AND " +
                                 "`my_column` >= 'M' AND `my_column` < 'Z' ORDER BY `my_column`")
                self.assertEqual(next(query_iterable),
                                 "SELECT 1 FROM my_table WHERE 1=1 AND " +
                                 "`my_column` >= 'Z' AND `my_column` < '^' ORDER BY `my_column`")
                self.assertEqual(next(query_iterable),
                                 "SELECT 1 FROM my_table WHERE 1=1 AND `my_column` >= '^' ORDER BY `my_column`")
                self.assertIsNone(next(query_iterable, None))

    def test_list_files(self):
        """Test a valid list of results are returned from the database"""
        pass
