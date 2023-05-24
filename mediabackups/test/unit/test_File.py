"""
Tests the File.py classes and methods
"""
import datetime
import unittest

from mediabackups.File import File


class Test_File(unittest.TestCase):
    """test module implementing utility functions"""

    def setUp(self):
        """Set up the tests."""
        pass

    def test___init__(self):
        """Test constructor"""
        f = File('commonswiki', 'Test.jpg', 'public')
        self.assertEqual(f.wiki, 'commonswiki')
        self.assertEqual(f.upload_name, 'Test.jpg')
        self.assertEqual(f.status, 'public')

        f = File(wiki='testwiki',
                 upload_name='Test.png',
                 status='deleted',
                 size=12,
                 type='IMAGE',
                 upload_timestamp=datetime.datetime(2023, 11, 23, 13, 46, 55),
                 deleted_timestamp=datetime.datetime(2023, 11, 23, 13, 46, 56),
                 sha1='a94a8fe5ccb19ba61c4c0873d391e987982fbbd3',
                 sha256='9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08',
                 md5=None,
                 storage_container='wikipedia-test-local-deleted',
                 storage_path='/a/a9/a94/a94a8fe5ccb19ba61c4c0873d391e987982fbbd3.png',
                 archived_timestamp=None)
        self.assertEqual(f.storage_container, 'wikipedia-test-local-deleted')
        self.assertIsNone(f.archived_timestamp)

    def test___eq__(self):
        """test equality"""
        f1 = File('commonswiki', 'Test.jpg', 'public')
        f2 = File('commonswiki', 'Test2.jpg', 'public')
        f2.upload_name = 'Test.jpg'
        self.assertEqual(f1, f2)

    def test___hash__(self):
        """test hashing"""
        sha1 = 'a94a8fe5ccb19ba61c4c0873d391e987982fbbd3'
        f = File('commonswiki', 'Test.jpg', 'public', sha1=sha1)
        self.assertEqual(f.__hash__(), hash(sha1))

    def test_row2File(self):
        """test changing database row to File instance"""
        row = {'wiki': 2, 'upload_name': b'Test.jpg', 'status': 1, 'md5': None, 'sha1': None,
               'storage_container': 1, 'storage_path': b'/Test.jpg'}
        string_wiki = {1: 'commonswiki', 2: 'testwiki'}
        string_file_type = {1: 'IMAGE', 2: 'VIDEO'}
        string_status = {1: 'public', 2: 'archived', 3: 'deleted'}
        string_container = {1: 'wikipedia-test-local-public', 2: 'wikipedia-test-local-deleted'}
        f = File.row2File(row, string_wiki, string_file_type, string_status, string_container)
        self.assertEqual(f, File(wiki='testwiki', upload_name='Test.jpg', status='public',
                                 storage_container='wikipedia-test-local-public',
                                 storage_path='/Test.jpg'))

    def test_properties(self):
        """test the properties method returning the right keys"""
        f = File('commonswiki', 'Test.jpg', 'public')
        file_dict = {
            'wiki': 'commonswiki',
            'upload_name': 'Test.jpg',
            'size': None,
            'file_type': 'ERROR',
            'status': 'public',
            'upload_timestamp': None,
            'archived_timestamp': None,
            'deleted_timestamp': None,
            'md5': None,
            'sha1': None,
            'storage_container': None,
            'storage_path': None
        }
        self.assertEqual(f.properties(), file_dict)

    def __repr__(self):
        """test the file output in string format"""
        f = File('commonswiki', 'Test.jpg', 'public')
        string_file = 'commonswiki Test.jpg None None'
        self.assertEqual(f.__repr__(), string_file)
