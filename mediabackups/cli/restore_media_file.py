#!/usr/bin/python3
"""
Interactive command line application to restore a previously backed up file
or set of files with a given parameters (wiki, hash, title, etc.).
"""

import datetime
import logging
import os
import sys

from mediabackups.Encryption import Encryption
from mediabackups.MySQLMetadata import MySQLMetadata
from mediabackups.S3 import S3
from mediabackups.Util import read_yaml_config, base36tobase16


DEFAULT_WIKI = 'commonswiki'
METADATA_CONFIG_FILE = '/etc/mediabackup/mediabackups_db.conf'
RECOVERY_CONFIG_FILE = '/etc/mediabackup/mediabackups_recovery.conf'
GREEN = '\033[92m'
UNDERLINE = '\033[4m'
END = '\033[0m'
IDENTIFICATION_METHODS = [
    {'identifier': 'upload_title',
     'description': f'{UNDERLINE}Title{END} of the file on upload (or after rename)',
     'type': 'title'},
    {'identifier': 'sha1',
     'description': f'{UNDERLINE}sha1sum{END} hash of the file contents, in {UNDERLINE}hexadecimal{END}',
     'type': 'hex_string'},
    {'identifier': 'sha1_base36',
     'description': f'{UNDERLINE}sha1sum{END} hash of the file contents, in Mediawiki\'s {UNDERLINE}base 36{END}',
     'type': 'base36_string'},
    # {'identifier': 'md5_base36',
    # 'description': f'{UNDERLINE}md5sum{END} hash of the title of the file, in Mediawiki\'s {UNDERLINE}base 36{END}',
    # 'type': 'base36_string'},
    {'identifier': 'swift_path',
     'description': f'Original {UNDERLINE}container name and full path{END} as was stored on Swift',
     'type': 'swift_location'},
    {'identifier': 'sha256',
     'description': f'{UNDERLINE}sha256sum{END} hash of the file contents, in hexadecimal',
     'type': 'hex_string'},
    {'identifier': 'upload_date',
     'description': f'Exact {UNDERLINE}date{END} of the original file '
                    f'{UNDERLINE}upload{END}, as registered on the metadata',
     'type': 'datetime'},
    {'identifier': 'archive_date',
     'description': f'Exact {UNDERLINE}date{END} of the latest file '
                    f'{UNDERLINE}archival{END}, as registered on the metadata',
     'type': 'datetime'},
    {'identifier': 'delete_date',
     'description': f'Exact {UNDERLINE}date{END} of the latest file '
                    f'{UNDERLINE}deletion{END}, as registered on the metadata',
     'type': 'datetime'}
]


def get_wiki_interactively(metadata):
    """
    Asks and validates against the database for a provided wiki name (eg.
    "enwiki", "commonswiki", ...)
    """
    logger = logging.getLogger('recovery')
    while True:
        wiki = input(f'{UNDERLINE}Wiki{END} to recover [default: {DEFAULT_WIKI}]: ')
        if wiki == '':
            wiki = DEFAULT_WIKI
        if not metadata.is_valid_wiki(wiki):
            logger.error('"%s" is not a recognized wiki in the metadata database', wiki)
            print()
            continue
        return wiki


def get_identification_interactively():
    """
    Asks for a method to identify a media file (title on upload, sha1, sha265, ...)
    """
    logger = logging.getLogger('recovery')
    while True:
        print()
        i = 0
        for option in IDENTIFICATION_METHODS:
            print(f"{GREEN}{i}){END} {option['description']}")
            i += 1
        print()
        choice = input(f'Chose method to identify the media file to '
                       f'recover (0-{len(IDENTIFICATION_METHODS) - 1}) [default: 0]: ')
        if choice == '':
            choice = 0
        try:
            if int(choice) < 0:  # python allows to index lists with negative values
                raise ValueError
            method = IDENTIFICATION_METHODS[int(choice)]['identifier']
            id_type = IDENTIFICATION_METHODS[int(choice)]['type']
        except (ValueError, IndexError):
            logger.error('"%s" is not a valid choice', choice)
            print()
            continue
        return method, id_type


def get_identifier(id_type):
    """
    Given a certain identification type ('string', 'datetime'), ask the user for
    the given type and validate it
    """
    logger = logging.getLogger('recovery')
    print()
    if id_type == 'datetime':
        while True:
            date_string = input(f'{UNDERLINE}Date{END} in format YYYY-MM-DD hh:mm:ss or YYYYMMDDhhmmss: ').strip()
            try:
                if '-' in date_string:
                    identifier = datetime.datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
                else:
                    identifier = datetime.datetime.strptime(date_string, '%Y%m%d%H%M%S')
            except ValueError:
                logger.error('"%s" is not a valid date format', date_string)
                print()
                continue
            break
    elif id_type == 'title':
        identifier = input(f'{UNDERLINE}Title{END} (without "File:", '
                           f'spaces will be converted to underscores, '
                           f'first letter in uppercase: ').strip().replace(' ', '_')
    elif id_type == 'hex_string':
        identifier = input(f'{UNDERLINE}Hexadecimal{END} string (e.g. "182dd70b9c"): ').strip()
    elif id_type == 'base36_string':
        identifier = input(f'{UNDERLINE}Base 36{END} string (e.g. "2toegxnxd"): ').strip()
    elif id_type == 'swift_location':
        container = input(f'Name of the {UNDERLINE}container{END} '
                          f'(e.g.: "wikipedia-commons-local-public.02"): ').strip()
        path = input(f'File {UNDERLINE}path{END} within the container (e.g.: "2/t/o/2toe.jpeg"): ').strip()
        identifier = {'container': container, 'path': path}
    return identifier


def get_interactive_parameters(metadata):
    """
    Ask for the wiki, the recovery method and its parameters interactivelly on command line
    """
    wiki = get_wiki_interactively(metadata)
    identification, id_type = get_identification_interactively()
    identifier = get_identifier(id_type)
    return {'wiki': wiki, 'method': identification, 'value': identifier}


def get_commandline_parameters(metadata):
    """TODO: get parameters from command like (non-interactively)"""
    logger = logging.getLogger('recovery')
    logger.debug(metadata)
    return {}


def print_and_confirm_recovery(file_list):
    """
    Prints the list of files and its properties for user examination
    and prompt for confirmation
    """
    logger = logging.getLogger('recovery')

    print()
    print(f'This is the list of {UNDERLINE}{len(file_list)} files found{END} with the given criteria:')
    print()
    i = 0
    for backup_file in file_list:
        print()
        print(f'{GREEN}{i}){END}')
        for key, value in backup_file.items():
            print(f'{key.ljust(20)} | {value}')
        i += 1
        print()

    key = None
    while key not in ('y', 'Y', 'n', 'N'):
        key = input(f'{UNDERLINE}Confirm{END} recovery of {len(file_list)} file(s)? ({UNDERLINE}y/n{END}) ')
    if key.lower() == 'n':
        logger.warning('Recovery aborted due to user input')
        sys.exit(3)


def recover_to_local(files):
    """Download and save given backed up files into the local filesystem"""
    logger = logging.getLogger('recovery')
    logger.info('About to recover %s files...', str(len(files)))

    storage_config = read_yaml_config(RECOVERY_CONFIG_FILE)
    s3api = S3(storage_config)
    encryption = Encryption(storage_config['identity_file'])

    recovered_files = 0
    for f in files:
        download_path = f['production_path'].split('/')[-1] if f['production_path'] is not None else 'unnamed_file'
        while os.path.exists(download_path):
            download_path += '~'
        backup_name = f['backup_path']
        backup_shard = f['backup_location']
        logger.info('Attempting to recover "%s" from "%s" into "%s"...', backup_name, backup_shard, download_path)
        if not s3api.check_file_exists(backup_name):
            logger.error('"%s" was not found on the backup storage "%s"', backup_name, backup_shard)
            continue
        if s3api.download_file(backup_shard, backup_name, download_path) == -1:
            logger.error('"%s" failed to be downloaded from "%s" '
                         'and saved into "%s"',
                         backup_name, backup_shard, download_path)
            continue
        logger.info('"%s" was successfully downloaded from "%s" '
                    'and saved into "%s"',
                    backup_name, backup_shard, download_path)
        if backup_name.endswith('.age'):
            if encryption.decrypt(download_path) != 0:
                logger.error('Decryption of "%s" failed', download_path)
                continue
        recovered_files += 1
    logger.info('%s out of %s files were successfully written '
                'to the local filesystem.',
                str(recovered_files), str(len(files)))


def search_files(metadata, options):
    """Given a search method, query the database in search of the matching files"""
    logger = logging.getLogger('recovery')

    if options['method'] == 'upload_title':
        file_list = metadata.list_backups_from_title(options['wiki'], options['value'])
    elif options['method'] == 'sha1':
        file_list = metadata.list_backups_from_sha1(options['wiki'], options['value'].zfill(40))
    elif options['method'] == 'sha256':
        file_list = metadata.list_backups_from_sha256(options['wiki'], options['value'].zfill(64))
    elif options['method'] == 'sha1_base36':
        file_list = metadata.list_backups_from_sha1(options['wiki'], base36tobase16(options['value']))
    elif options['method'] == 'swift_path':
        file_list = metadata.list_backups_from_path(options['wiki'],
                                                    options['value']['container'],
                                                    options['value']['path'])
    elif options['method'] == 'upload_date':
        file_list = metadata.list_backups_from_upload_date(options['wiki'], options['value'])
    elif options['method'] == 'archive_date':
        file_list = metadata.list_backups_from_archive_date(options['wiki'], options['value'])
    elif options['method'] == 'delete_date':
        file_list = metadata.list_backups_from_delete_date(options['wiki'], options['value'])
    else:
        logger.error('Invalid method to search a recovery file provided.')
        sys.exit(5)
    return file_list


def main():
    """
    Recover a file or a list of file from media backups
    and write it to the local filesystem.
    """
    logger = logging.getLogger('recovery')
    logging.basicConfig(format='[%(asctime)s] %(levelname)s:%(name)s %(message)s',
                        # filename='recovery.log',
                        level=logging.INFO)
    metadata = MySQLMetadata(read_yaml_config(METADATA_CONFIG_FILE))
    metadata.connect_db()

    if len(sys.argv) == 1:
        options = get_interactive_parameters(metadata)
    else:
        options = get_commandline_parameters(metadata)

    file_list = search_files(metadata, options)
    metadata.close_db()

    if len(file_list) == 0:
        logger.warning('No file was found that matched the given criteria, exiting.')
        sys.exit(4)
    print_and_confirm_recovery(file_list)
    recover_to_local(file_list)


if __name__ == "__main__":
    main()
