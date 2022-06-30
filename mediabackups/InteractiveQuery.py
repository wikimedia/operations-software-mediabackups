"""Interactive query library: search and query to find, download and remove existing backups
through a command line interface"""

import datetime
import logging
import os
import requests
import sys

from mediabackups.Encryption import Encryption
from mediabackups.S3 import S3
from mediabackups.Util import read_yaml_config, base36tobase16

DEFAULT_WIKI = 'commonswiki'
RECOVERY_CONFIG_FILE = '/etc/mediabackup/mediabackups_recovery.conf'
GREEN = '\033[92m'
RED = '\033[31m'
UNDERLINE = '\033[4m'
END = '\033[0m'
VALID_ACTIONS = ['deletion', 'query', 'recovery']
HTTP_HEADERS = {'User-agent': 'mediabackups/InteractiveQuery https://phabricator.wikimedia.org/diffusion/OSWB/'}
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


class InteractiveQuery:
    """
    Implementes methods needed to query, recover and delete media backups from a
    unix command line interface
    """
    def __init__(self, action, dry_mode=True):
        """Initializes what is the intended action for the interactive query:
           A recovery, a query or a deletion"""
        if action not in VALID_ACTIONS:
            logger = logging.getLogger('query')
            logger.critical(f'Action should be one of: {VALID_ACTIONS}, fatal error, exiting')
            sys.exit(-1)
        self.action = action
        self.dry_mode = dry_mode
        logger = logging.getLogger(self.action)
        if self.action == 'deletion':
            # deletions are an usafe operation, provide the user an extra warning
            if self.dry_mode:
                print(f'{RED}This is a dry run deletion- '
                      f'no actual file or metadata will be affected, '
                      f'even if the script will follow the same steps and confirmation.{END}')
            else:
                print(f'{RED}An actual backup file deletion will be performed- '
                      f'these actions are undoable- although you will be given the '
                      f'chance of a final confirmation.{END}')
        logger.info('Starting an interactive %s session', self.action)

    def get_wiki_interactively(self, metadata):
        """
        Asks and validates against the database for a provided wiki name (eg.
        "enwiki", "commonswiki", ...)
        """
        logger = logging.getLogger(self.action)
        while True:
            wiki = input(f'{UNDERLINE}Wiki{END} for {self.action} [default: {DEFAULT_WIKI}]: ')
            if wiki == '':
                wiki = DEFAULT_WIKI
            if not metadata.is_valid_wiki(wiki):
                logger.error('"%s" is not a recognized wiki in the metadata database', wiki)
                print()
                continue
            return wiki

    def get_identification_interactively(self):
        """
        Asks for a method to identify a media file (title on upload, sha1, sha265, ...)
        """
        logger = logging.getLogger(self.action)
        while True:
            print()
            i = 0
            for option in IDENTIFICATION_METHODS:
                print(f"{GREEN}{i}){END} {option['description']}")
                i += 1
            print()
            choice = input(f'Chose method to identify the media file to '
                           f'{self.action} (0-{len(IDENTIFICATION_METHODS) - 1}) [default: 0]: ')
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

    def get_identifier(self, id_type):
        """
        Given a certain identification type ('string', 'datetime'), ask the user for
        the given type and validate it
        """
        logger = logging.getLogger(self.action)
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
            identifier = input(f'{UNDERLINE}Title{END} ('
                               f'spaces will be converted to underscores, '
                               f'first letter normally in uppercase): ').strip().replace(' ', '_').removeprefix('File:')
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

    def get_interactive_parameters(self, metadata):
        """
        Ask for the wiki, the recovery method and its parameters interactivelly on command line
        """
        wiki = self.get_wiki_interactively(metadata)
        identification, id_type = self.get_identification_interactively()
        identifier = self.get_identifier(id_type)
        return {'wiki': wiki, 'method': identification, 'value': identifier}

    def get_commandline_parameters(self, metadata):
        """TODO: get parameters from command like (non-interactively)"""
        logger = logging.getLogger(self.action)
        logger.debug(metadata)
        return {}

    def print_files(self, file_list):
        """
        Prints the given list of files' properties for user examination
        """
        print()
        print(f'This is the list of {UNDERLINE}{len(file_list)} files found{END} with the given criteria:')
        print()
        i = 0
        for backup_file in file_list:
            print()
            print(f'{GREEN}{i}){END}')
            for key, value in backup_file.items():
                if not key.startswith('_'):  # hide some keys (internal ids)
                    print(f'{key.ljust(20)} | {value}')
            i += 1
            print()

    def print_and_finish(self, file_list):
        """
        Prints the list of files and its properties for user examination
        and prompts for confirmation
        """
        logger = logging.getLogger(self.action)
        if self.action != 'query':
            logger.critical(f'query action requested from a {self.action} operation, exiting')
            sys.exit(-1)

        self.print_files(file_list)

        logger.info(f'Printed {str(len(file_list))} file(s) and finished execution')

    def print_and_confirm_action(self, file_list):
        """
        Prints the list of files and its properties for user examination
        and prompts for confirmation
        """
        logger = logging.getLogger(self.action)
        if self.action not in ('recovery', 'deletion'):
            logger.critical(f'query action requested from a {self.action} operation, exiting')
            sys.exit(-1)

        self.print_files(file_list)
        if self.action == 'deletion':
            if self.dry_mode:
                print(f"{RED}Executing deletion in dry mode, so files will not be actually deleted{END}")
            else:
                print(f"{RED}WARNING! File deletion cannot be reverted{END}")

        key = None
        while key not in ('y', 'Y', 'n', 'N'):
            key = input(f'{UNDERLINE}Confirm{END} {self.action} of {len(file_list)} file(s)? ({UNDERLINE}y/n{END}) ')
        if key.lower() == 'n':
            logger.warning(f'{self.action} aborted due to user input')
            sys.exit(3)

    def recover_to_local(self, files):
        """Download and save given backed up files into the local filesystem"""
        logger = logging.getLogger(self.action)
        if self.action != 'recovery':
            logger.critical(f'recovery action requested from a {self.action} operation, exiting')
            sys.exit(-1)

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

    def check_deleted_from_production(self, files):
        """
        Makes sure that all files provided have been removed from production
        before attempting to delete them
        """
        logger = logging.getLogger(self.action)
        for f in files:
            if f['production_url'] is not None:
                response = requests.head(f['production_url'], headers=HTTP_HEADERS)
                if response.status_code != 404:
                    logger.error('We got an HTTP status code of %s when we '
                                 'tried querying %s from production, we expected a 404.',
                                 str(response.status_code), f['production_url'])
                    logger.error('Aborting deletion process.')
                    sys.exit(6)
        logger.info('All files were queried from production '
                    'and none were found publicly available: %s', f['production_url'])

    def delete_files(self, files):
        """Delete and update metadata for given backed up files"""
        logger = logging.getLogger(self.action)
        if self.action != 'deletion':
            logger.critical(f'deletion action requested from a {self.action} operation, exiting')
            sys.exit(-1)

        logger.info('About to delete %s files...', str(len(files)))

        storage_config = read_yaml_config(RECOVERY_CONFIG_FILE)
        s3api = S3(storage_config)

        # failsafe- we check all files intended to be deleted to make sure none are currently publicly available
        self.check_deleted_from_production(files)

        deleted_files = []
        for f in files:
            backup_name = f['backup_path']
            backup_shard = f['backup_location']
            logger.info('Attempting to delete "%s" from "%s"', backup_name, backup_shard)
            if not s3api.check_file_exists(backup_name):
                # check if it was deleted on this batch (common storage location)
                if len([x for x in deleted_files if x['wiki'] == f['wiki'] and x['sha256'] == f['sha256']]) > 0:
                    logger.info('"%s" was a duplicate of a previous file and already deleted', backup_name)
                else:
                    logger.error('"%s" was not found on the backup storage "%s"', backup_name, backup_shard)
                    continue
            if self.dry_mode:
                logger.warning("Not actually deleting %s from %s because we are in DRY MODE - but otherwise succesful.",
                               backup_name, backup_shard)
            else:
                if s3api.delete_file(backup_shard, backup_name) == -1:
                    logger.error('"%s" failed to be deleted from "%s" ',
                                 backup_name, backup_shard)
                    continue
                logger.info('"%s" was successfully deleted from "%s" ',
                            backup_name, backup_shard)
            deleted_files.append(f)
        logger.info('%s out of %s files were successfully deleted from backup storage.',
                    str(len(deleted_files)), str(len(files)))
        return deleted_files

    def cleanup(self):
        """
        Latest chanages done at the end of a session (for now, just print a warning)
        """
        logger = logging.getLogger(self.action)
        if self.action in ['recovery', 'deletion']:
            # warn to perform the same action again on other dcs for recoveries and deletions
            print(f'{UNDERLINE}Remember to perform the same {self.action} on the other datacenter too '
                  f'(only data from one site was affected for the current session!).{END}')
        logger.info('Finishing the interactive %s session', self.action)

    def search_files(self, metadata, options):
        """Given a search method, query the database in search of the matching files"""
        logger = logging.getLogger(self.action)

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
