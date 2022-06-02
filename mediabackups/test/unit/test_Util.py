"""
Tests the Util.py classes and methods
"""
from testfixtures import log_capture
import unittest
from unittest.mock import patch, mock_open

import mediabackups.Util as Util
from mediabackups.Util import EncounteredDBListExpression


class Test_Util(unittest.TestCase):
    """test module implementing utility functions"""

    def setUp(self):
        """Set up the tests."""
        pass

    def test_read_dblist(self):
        # typical list
        data = """
            # list of wikis
            enwiki
            frwiki
            testwiki # this is a production wiki!
        """
        with patch('builtins.open', mock_open(read_data=data)):
            self.assertEqual(Util.read_dblist(None), ['enwiki', 'frwiki', 'testwiki'])

        # extra meaningless characters and comments
        data = """
            # list of wikis
            ## that are to be backed # up
            enwiki
            \t\n\n\n
            frwiki
            testwiki ## this is a production wiki!
            \n
        """
        with patch('builtins.open', mock_open(read_data=data)):
            self.assertEqual(Util.read_dblist(None), ['enwiki', 'frwiki', 'testwiki'])

        # File not found
        mock = mock_open()
        mock.side_effect = FileNotFoundError
        with patch('builtins.open', mock):
            with self.assertRaises(FileNotFoundError):
                Util.read_dblist(None)

        # Not a concrete list of wikis
        data = """
            # list of wikis
            enwiki
            frwiki
            %%testwikilist
        """
        with patch('builtins.open', mock_open(read_data=data)):
            with self.assertRaises(EncounteredDBListExpression):
                Util.read_dblist(None)

    @log_capture()
    def test_read_yaml_config(self, log):
        """Tests the reading and loading of yaml configuration for backup execution"""
        # most boring configuration
        data = """
            host: db1001.eqiad.wmnet
            port: 3306
            database: 'mediabackups'
            user: 'mediabackup'
            password: 'a_password'
            batchsize: 1000
        """
        dictionary = {'host': 'db1001.eqiad.wmnet', 'port': 3306, 'database': 'mediabackups',
                      'user': 'mediabackup', 'password': 'a_password', 'batchsize': 1000}
        with patch('builtins.open', mock_open(read_data=data)):
            self.assertEqual(dictionary, Util.read_yaml_config(''))

        # invalid yaml
        data = ":"
        with patch('builtins.open', mock_open(read_data=data)):
            self.assertIsNone(Util.read_yaml_config(''))
            log.check(('backup', 'ERROR', 'Yaml configuration "" could not be loaded'))

    def test_sha1sum(self):
        """test the shasum algoritm- which makes sure it is not computed by loading the full file on memory"""
        # checksum the empty string
        data = b''
        sha1sum = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'
        with patch('builtins.open', mock_open(read_data=data)):
            self.assertEqual(sha1sum, Util.sha1sum(None))
        # checksum with leading zeros (we zerofill)
        data = b'hello2'
        sha1sum = '0f1defd5135596709273b3a1a07e466ea2bf4fff'
        with patch('builtins.open', mock_open(read_data=data)):
            self.assertEqual(sha1sum, Util.sha1sum(None))
        # not trivial size file
        data = bytes.fromhex("""
           89504e470d0a1a0a0000000d494844520000002000000025080600000023
           b7eb4700000211494441545885ed97316bdb401480bf538d39d490a143c9
           12c8d80ea194d2c164b57f47e87f08fd11a1ff21f87768361e4a289dd2d1
           e0255387e08843a8f73af89d7376645b2272bcf8c1e32cfbd0f7dde949d6
           332202c06030e80009d0d5b143bb51021e28009f655909604424c0bb80d5
           ecaa40d212dcab400138cd22cbb2d2f4fbfd004f81631d2dbbd90107e4c0
           838e4558a555f87b1119b70c5e0a634c4f3f7aa08cb73e1591b13bbbc4a7
           a7883d6917ecee49f22976321c1b63ced1cb10179e05760207107b827f3a
           0c7596846a0fb913782ca1b1608642ab55ed171fdff2e3dbe9c639573753
           46778f754e97d40687d806af3be799c53e63ef028d1f36bdef7f968ec7d7
           1f5e57e0a5c01709acaefed505eaaebe89e8a106f67e1b1e040e02078146
           025737d356e6c4111e447ee32c8dd1dd639bff071ee63b50468971f76d01
           9e4574ee0533e1a95d7200493edd8944782dd770caf49d089e1b637a7632
           ac6c4cdcd925ffde7dad057bf3f7277632ac1699372639515fe0f5e04127
           9cb3dc9a250076321c39d82a11e0c6980bfd2ad4d76a6be6005fa7395dbc
           c38bc8eda69d88e05f58aeadf5cde996f63c8c71fbf6ab4a22827f8eb778
           45a0ba3ddf142a668123cd54447ec71211fc93c2679a2e80d6c556813512
           4761278078e5331ac06b0b54481cabc42d10aef98c7971d5863712589148
           35bbfa53c17cebf326f0c602914428ca70ab865bac680207f80fd8d91211
           edcaae7c0000000049454e44ae426082""")  # CC BY-SA 3.0 Nicolas Mollet
        sha1sum = '000001d93b4cfd2df055c77815f8efae13a131e2'
        with patch('builtins.open', mock_open(read_data=data)):
            self.assertEqual(sha1sum, Util.sha1sum(None))

    def test_base16tobase36(self):
        """Test converting base16 strings to base36, in the Wikimedia style
           ( https://www.mediawiki.org/wiki/Base_convert ).
        """
        tests = (
            ('0', '0' * 31),
            ('10', '000000000000000000000000000000g'),
            ('2c5f4c5ff0e57ffcea85c1da92b4599336d75fb9', '56le7dx4g21ssp3jyb0xc8a5vlk4fjt'),
            ('1d93b4cfd2df055c77815f8efae13a131e2', '00005j87okqh6okafuoz8j0aa2dj4de'),
        )
        for testcase in tests:
            self.assertEqual(Util.base16tobase36(testcase[0]), testcase[1])

    def test_base36tobase16(self):
        """Test converting base36 strings to base16, in the Wikimedia style
           ( https://www.mediawiki.org/wiki/Base_convert ).
        """
        tests = (
            ('0', '0' * 40),
            ('z', '0000000000000000000000000000000000000023'),
            ('56le7dx4g21ssp3jyb0xc8a5vlk4fjt', '2c5f4c5ff0e57ffcea85c1da92b4599336d75fb9'),
            ('5j87okqh6okafuoz8j0aa2dj4de', '000001d93b4cfd2df055c77815f8efae13a131e2'),
        )
        for testcase in tests:
            self.assertEqual(Util.base36tobase16(testcase[0]), testcase[1])
