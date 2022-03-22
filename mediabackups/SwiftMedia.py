"""The module SwiftMedia implements the class SwiftMedia"""
import hashlib
import logging

from swiftclient.service import SwiftService, SwiftError

from mediabackups.File import File
from mediabackups.Util import base36tobase16

DEFAULT_BATCH_SIZE = 100
# copied from mediawiki-config/wmf-config/filebackend.php
wmfSwiftBigWikis = [
    'commonswiki', 'dewiki', 'enwiki', 'fiwiki', 'frwiki', 'hewiki', 'huwiki', 'idwiki',
    'itwiki', 'jawiki', 'rowiki', 'ruwiki', 'thwiki', 'trwiki', 'ukwiki', 'zhwiki'
]
# for base36 and base16 hashes looping
BASE36_CHARACTERS = '0123456789abcdefghijklmnopqrstuvwxyz'
BASE16_CHARACTERS = '0123456789abcdef'


class SwiftMedia:
    """Prepare and generate a media backup"""

    def __init__(self, config):
        """Constructor"""
        self.wiki = config.get('wiki', None)
        self.batchsize = config.get('batchsize', DEFAULT_BATCH_SIZE)

    def isBigWiki(self):
        '''
        Returns true if the current wiki is in the list of big wikis
        (that means the container is hashed)
        '''
        return self.wiki in wmfSwiftBigWikis

    @staticmethod
    def getProjectTypes():
        '''
        returns a dictionary with keys: project types on db and elements: project
        types on containers.
        '''
        return {'wiki': 'wikipedia', 'wikiquote': 'wikiquote',
                'wikibooks': 'wikibooks', 'wikimedia': 'wikimedia',
                'wikisource': 'wikisource', 'wikinews': 'wikinews',
                'wikiversity': 'wikiversity', 'wikivoyage': 'wikivoyage',
                'wiktionary': 'wiktionary'}

    def wiki2container(self, status):
        '''
        Returns the container name (except the hashing) for a given
        wiki and status of the file
        '''
        logger = logging.getLogger('backup')
        if status == 'archived':  # archived images are stored on public containers
            status = 'public'
        for postfix, container in SwiftMedia.getProjectTypes().items():
            if self.wiki.endswith(postfix):
                return (container + '-' + self.wiki[:-len(postfix)].replace('_', '-') +
                        '-local-' + status)
        logger.exception('Container not found for wiki: %s', self.wiki)
        return None

    @staticmethod
    def container2wiki(container_name):
        '''
        Returns the wiki name (in dblist format) for a given container name
        '''
        logger = logging.getLogger('backup')
        container = container_name.split('.')[0]
        postfixes = ['-local-public', '-local-deleted']
        for postfix in postfixes:
            if container.endswith(postfix):
                container = container[:-len(postfix)]
                break
        for wiki_type, prefix in SwiftMedia.getProjectTypes().items():
            if container.startswith(prefix):
                return container[len(prefix) + 1:].replace('-', '_') + wiki_type
        logger.exception('Wiki not found for container: %s', container_name)
        return None

    def name2swift(self, image_name, status, archive_date=None, storage_name=None, sha1=None):
        '''
        returns a list with the actual container name (including the hashing) and
        the expected location with the virtual path. archive_date (string timestamp
        in mediawiki format) is only needed for archived images. md5sum is only
        required for deleted status files.
        '''
        if status != 'deleted':
            if image_name is None:  # gap on metadata if public and the image title is null
                return None, None
            title_md5 = hashlib.md5(image_name.encode()).hexdigest().zfill(32)
        container = self.wiki2container(status)
        if storage_name is None or storage_name == '':
            return container, None
        if self.isBigWiki():
            if status == 'deleted':
                container = container + '.' + storage_name[:2]
            else:
                container = container + '.' + title_md5[:2]
        if status == 'public':
            path = title_md5[:1] + '/' + title_md5[:2] + '/' + storage_name
        if status == 'archived':
            path = ('archive/' + title_md5[:1] + '/' + title_md5[:2] +
                    '/' + storage_name)
        if status == 'deleted':
            path = (storage_name[0] + '/' + storage_name[1] + '/' + storage_name[2] +
                    '/' + storage_name)
        return container, path

    def list_container(self, container, options, status, hierarchy_depth):
        """
        Yields, in batches of given size, the list of
        files in the given container
        """
        logger = logging.getLogger('backup')
        files = list()
        with SwiftService() as swift:
            try:
                list_parts_gen = swift.list(container=container, options=options)
            except SwiftError as swift_error:
                logger.error(swift_error.value)
                return files
            for page in list_parts_gen:
                if not page['success']:
                    logger.exception(page['error'])
                for item in page['listing']:
                    # Ignore archived items for public requests
                    if status == 'public' and item['name'].startswith('archive/'):
                        continue
                    storage_path = item['name']
                    try:
                        swift_name = storage_path.split('/', hierarchy_depth)[hierarchy_depth]
                    except IndexError:
                        logger.info('Ignoring file: %s', storage_path)
                        continue
                    if status == 'public':
                        upload_name = swift_name
                    elif status == 'deleted':
                        upload_name = None
                    elif status == 'archived':
                        try:
                            upload_name = swift_name.split('!', 1)[1]
                        except IndexError:
                            logger.info('Ignoring file: %s', swift_name)
                            continue
                    size = int(item['bytes'])
                    type = item['content_type']
                    upload_timestamp = None
                    deleted_timestamp = item['last_modified']
                    sha1 = (base36tobase16(swift_name.split('.', 1)[0])
                            if status == 'deleted' else None)
                    md5 = item['hash']  # This is actually the md5sum
                    files.append(File(wiki=self.wiki, upload_name=upload_name,
                                      size=size, type=type, status=status,
                                      upload_timestamp=upload_timestamp,
                                      deleted_timestamp=deleted_timestamp, sha1=sha1,
                                      md5=md5, storage_container=container,
                                      storage_path=storage_path))
                    if len(files) >= self.batchsize:
                        yield files
                        files = list()
            yield files

    def list_files(self, status='public'):
        """
        Reads the list of all files on the given category of a wiki
        and returns an iterator of File objects
        """
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger('backup')

        container = self.wiki2container(status)
        options = {}
        if status == 'archived':
            options['prefix'] = 'archive'

        if status == 'public':
            # last uploaded files are found under a/ab/<name>
            # a, and ab being the first letters of the md5sum of the title
            hierarchy_depth = 2
        else:
            # archived files are found under archive/a/ab/timestamp!<name>
            # deleted files are found under a/b/c/<sha1 base36>.<original extension>
            hierarchy_depth = 3

        if self.isBigWiki():
            # big wikis have multiple containers, hashed
            if status == 'deleted':
                hash_characters = BASE36_CHARACTERS
            else:
                hash_characters = BASE16_CHARACTERS
            for hash_string in [first + second
                                for first in list(hash_characters)
                                for second in list(hash_characters)]:
                hashed_container = container + '.' + hash_string
                for f in self.list_container(hashed_container, options, status, hierarchy_depth):
                    yield f
        else:
            for f in self.list_container(container, options, status, hierarchy_depth):
                yield f
        logger.info('End of list of files for %s reached', self.wiki)

    @staticmethod
    def download(a_file, local_path):
        """
        Given a an object of type File, download it directly from Swift to
        the absolute path directory, removing any prefix on the virtual
        directory. Returns False if succesful, True if error.
        """
        logger = logging.getLogger('backup')
        return_result = True
        with SwiftService() as swift:
            try:
                for result in swift.download(container=a_file.storage_container,
                                             objects=[a_file.storage_path],
                                             options={'out_file': local_path}):
                    if result['success']:
                        logger.info("'%s' downloaded", result['object'])
                        return_result = False
                    else:
                        logger.error("'%s' failed to be downloaded", result['object'])
            except SwiftError as ex:
                logger.error(ex.value)
                return True
        return return_result
