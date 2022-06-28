'''mediabackups.'''
from setuptools import setup

with open('README.md', 'r') as fh:
    long_description = fh.read()

setup(
    name='mediabackups',
    description='mediabackups',
    version='0.1.2',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://phabricator.wikimedia.org/diffusion/OSMB/",
    packages=['mediabackups'],
    install_requires=['boto3',
                      'botocore',
                      'numpy',
                      'pymysql>=0.9.3',
                      'python-swiftclient',
                      'pyyaml'],
    entry_points={
        'console_scripts': [
           'gather-mysql-metadata = mediabackups.cli.gather_mysql_metadata:main',
           'update-mysql-metadata = mediabackups.cli.update_mysql_metadata:main',
           'backup-wiki = mediabackups.cli.backup_wiki:main',
           'restore-media-file = mediabackups.cli.restore_media_file:main',
           'query-media-file = mediabackups.cli.query_media_file:main',
           'delete-media-file = mediabackups.cli.delete_media_file:main',
        ]
    },
    test_suite='mediabackups.test',
)
