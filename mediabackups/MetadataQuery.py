"""Metadata query library: search and query to find, download and remove existing backups
through a command line interface"""

import datetime
import logging
import os
import re
import sys

import requests

from mediabackups.Encryption import Encryption
from mediabackups.File import File
from mediabackups.MySQLMetadata import MySQLMetadata
from mediabackups.S3 import S3
from mediabackups.Util import read_yaml_config, base36tobase16, base16tobase36, mwdate2datetime

DEFAULT_WIKI = 'commonswiki'
METADATA_CONFIG_FILE = '/etc/mediabackup/mediabackups_db.conf'
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


class MetadataQuery:
    """
    Implementes methods needed to query, recover and delete media backups from a
    unix command line interface or batch file.
    """
    def __init__(self, action, dry_mode=True, batch_file=None):
        """Initializes what is the intended action for the interactive query:
           A recovery, a query or a deletion"""
        if action not in VALID_ACTIONS:
            logger = logging.getLogger('query')
            logger.critical('Action should be one of: %s, fatal error, exiting', str(VALID_ACTIONS))
            sys.exit(-1)
        self.action = action
        self.dry_mode = dry_mode
        self.batch_file = batch_file
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
        if self.batch_file:
            logger.info('Starting a batch %s session', self.action)
        else:
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

    def print_and_confirm_action_if_not_query(self, file_list, files_missing=[], files_multiple=[]):
        """
        Prints the list of files and its properties for user examination
        and, if it is a query, finishes; otherwise prompts for confirmation
        of the action.
        """
        logger = logging.getLogger(self.action)

        self.print_files(file_list)
        if len(files_missing) != 0:
            logger.warning('%s searches returned no files!', str(len(files_missing)))
        if len(files_multiple) != 0:
            logger.warning('%s searches returned multiple results!', str(len(files_multiple)))
        if self.action == 'query':
            logger.info('Printed %s file(s) and finished execution', str(len(file_list)))
            return
        if self.action == 'deletion':
            if self.dry_mode:
                print(f"{RED}Executing deletion in dry mode, so files will not be actually deleted{END}")
            else:
                print(f"{RED}WARNING! File deletion cannot be reverted{END}")

        key = None
        while key not in ('y', 'Y', 'n', 'N'):
            key = input(f'{UNDERLINE}Confirm{END} {self.action} of {len(file_list)} file(s)? ({UNDERLINE}y/n{END}) ')
        if key.lower() == 'n':
            logger.warning('%s aborted due to user input', self.action)
            sys.exit(3)

    def recover_to_local(self, files):
        """Download and save given backed up files into the local filesystem"""
        logger = logging.getLogger(self.action)
        if self.action != 'recovery':
            logger.critical('Recovery action requested from a %s operation, exiting', self.action)
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
        timeout = 30  # seconds
        for f in files:
            if f['production_url'] is not None:
                try:
                    response = requests.head(f['production_url'], headers=HTTP_HEADERS, timeout=timeout)
                except requests.exceptions.Timeout:
                    logger.error("Querying %s timed out after %s seconds.", f['production_url'], timeout)
                    sys.exit(7)
                if response.status_code != 404:
                    logger.error('We got an HTTP status code of %s when we '
                                 'tried querying %s from production, we expected a 404.',
                                 str(response.status_code), f['production_url'])
                    logger.error('Aborting deletion process.')
                    sys.exit(6)
        logger.info('All files were queried from production '
                    'and none were found publicly available.')

    def delete_files(self, files):
        """Delete and update metadata for given backed up files"""
        logger = logging.getLogger(self.action)
        if self.action != 'deletion':
            logger.critical('Deletion action requested from a %s operation, exiting', self.action)
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

    def get_file_list_from_file(self, metadata):
        """
        Search the list of deleted files found on backup metadata from the batch file log and return
        a tuple with a list of those, as well as the list of files not found, and files that returned
        multiple results
        """
        logger = logging.getLogger(self.action)
        files_found = []
        files_missing = []
        files_multiple = []
        wiki = None
        regex = r".*mwscript\s+eraseArchivedFile\.php\s+\-\-wiki\s*=?\s*[\"\']?([a-zA-Z0-9\-_]+)[\"\']?\s.*\-\-delete"
        pattern_wiki = re.compile(regex)
        regex = r"Deleted\sversion\s\'([a-z0-9]*)\..*\'\s\(([0-9]{14})\)\sof\sfile\s\'(.+)'"
        pattern_deletion = re.compile(regex)
        try:
            with open(self.batch_file, mode="r", encoding="utf-8") as read_file:
                for line in read_file:
                    wiki_match = pattern_wiki.match(line)
                    if wiki_match:
                        if metadata.is_valid_wiki(wiki_match.group(1)):
                            wiki = wiki_match.group(1)
                            continue
                    deletion_match = pattern_deletion.match(line)
                    if deletion_match:
                        if wiki is None:
                            continue
                        sha1 = base36tobase16(deletion_match.group(1))
                        try:
                            date = mwdate2datetime(deletion_match.group(2))
                        except ValueError:
                            logger.error("Bad date found on file: %s", deletion_match.group(2))
                            continue
                        title = deletion_match.group(3)
                        found = metadata.list_backups_from_title_upload_date_and_sha1(wiki, title, date, sha1)
                        if found is None:
                            sys.exit(4)
                        if len(found) == 0:
                            logger.warning("No files found for file: %s %s %s %s",
                                           wiki, title, date.strftime("%m%d%Y%H%M%S"), base16tobase36(sha1))
                            files_missing.append(File(wiki, title, 'deleted', upload_timestamp=date, sha1=sha1))
                        if len(found) == 1:
                            files_found.extend(found)
                        elif len(found) >= 2:
                            files_found.extend(found)
                            logger.warning("Multiple files found for file: %s %s %s %s",
                                           wiki, title, date.strftime("%m%d%Y%H%M%S"), base16tobase36(sha1))
                            files_multiple.append(File(wiki, title, 'deleted', upload_timestamp=date, sha1=sha1))
        except FileNotFoundError:
            logger.error("File %s was not found", self.batch_file)
        except PermissionError:
            logger.error("Got a permission error while trying to read: %s", self.batch_file)
        except IOError:
            logger.error("An error happened while trying to open file: %s", self.batch_file)

        return files_found, files_missing, files_multiple

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

    def handle_query(self):
        """
        Asks the user for information interactivelly or from a file
        and then executes the right action on retrieved files-
        after printing and asking for user confirmation, if it is a
        deletion or a recovery.
        """
        logger = logging.getLogger(self.action)
        metadata = MySQLMetadata(read_yaml_config(METADATA_CONFIG_FILE))
        metadata.connect_db()
        if self.batch_file:
            file_list, files_missing, files_multiple = self.get_file_list_from_file(metadata)
        else:
            options = self.get_interactive_parameters(metadata)
            file_list = metadata.search_files(options)
            files_missing = []
            files_multiple = []
        metadata.close_db()  # close it as user can be a long time with session open

        if len(file_list) == 0:
            logger.warning('No file was found that matched the given criteria, exiting.')
            sys.exit(4)
        self.print_and_confirm_action_if_not_query(file_list, files_missing, files_multiple)
        # we do nothing -except cleanup- for query
        if self.action == "recovery":
            self.recover_to_local(file_list)
        elif self.action == "deletion":
            physically_deleted_files = self.delete_files(file_list)
            metadata = MySQLMetadata(read_yaml_config(METADATA_CONFIG_FILE))
            metadata.connect_db()
            metadata.mark_as_deleted(physically_deleted_files, dry_mode=self.dry_mode)
            metadata.close_db()
        self.cleanup()

    @staticmethod
    def execute_action(action):
        """
        Static method that parses the command line arguments and executes the right
        interactive or batch query.
        """
        if action not in VALID_ACTIONS:
            print(f"Wrong action requested: {action}", file=sys.stderr)
            sys.exit(-1)
        logger = logging.getLogger(action)
        logging.basicConfig(format='[%(asctime)s] %(levelname)s:%(name)s %(message)s',
                            handlers=[logging.FileHandler(f'/var/log/mediabackups/{action}.log'),
                                      logging.StreamHandler()],
                            level=logging.INFO)
        dry_mode = True

        # handle dry mode, interactive, and non-interactive modes
        if (len(sys.argv) == 1) or ((len(sys.argv) == 2) and (sys.argv[1] == "--execute")):
            if len(sys.argv) == 2:
                dry_mode = False
            mq = MetadataQuery(action, dry_mode)
            mq.handle_query()
        elif (len(sys.argv) == 2) or ((len(sys.argv) == 3) and (sys.argv[1] == "--execute")):
            if sys.argv[1] == "--execute":
                batch_file = sys.argv[2]
                dry_mode = False
            else:
                batch_file = sys.argv[1]
            mq = MetadataQuery(action, dry_mode, batch_file)
            mq.handle_query()
        else:
            logger.error("Wrong number or type of parameters. Usage: %s [--execute] [logfile].", sys.argv[0])
