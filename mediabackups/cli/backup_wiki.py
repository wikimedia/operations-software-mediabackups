#!/usr/bin/python3

"""
Command line utility that performs the reading of image metadata
from a mediawiki database and performs a full backup of all the media
files from a wiki or a section.
"""

import logging
import os

import mediabackups.Encryption
import mediabackups.SwiftMedia
import mediabackups.MySQLMetadata
from mediabackups.Util import read_yaml_config, sha1sum, sha256sum
import mediabackups.S3

METADATA_CONFIG_FILE = '/etc/mediabackup/mediabackups_db.conf'
STORAGE_CONFIG_FILE = '/etc/mediabackup/mediabackups_storage.conf'
TMPDIR = '/srv/mediabackup'


class DownloadException(Exception):
    """Raised on errors while trying to download the file from production"""
    pass


class UploadException(Exception):
    """Raised on errors while trying to uplaod the file into the backup storage"""
    pass


class DuplicateException(Exception):
    """Raised in case the file already exists on the backup storage"""
    pass


class EncryptionException(Exception):
    """Raised in case attempting to encrypt a file fails"""
    pass


def download_file_from_production(f, tmp_dir):
    """
    Downloads the file object given as a parameters from production into
    the local filesystem. Returns the download_path if sucessful, otherwise
    returns None.
    """
    basename = f.storage_path.split('/')[-1] if f.storage_path is not None else ''
    download_path = os.path.join(tmp_dir, basename)

    if mediabackups.SwiftMedia.SwiftMedia.download(f, download_path):
        raise DownloadException

    return download_path


def handle_checksums(f, download_path):
    """
    Calculate the actual sha1 and sha256 checksums of the download file.
    if the calculated and gathered from metadata sha1 sum is different,
    throw a warning, but back it up anyway.
    """
    logger = logging.getLogger('backup')
    sha1 = sha1sum(download_path)
    if f.sha1 != sha1:
        logger.warning('Calculated (%s) and queried (%s) sha1 checksums '
                       'are not the same for "%s"',
                       sha1, f.sha1, f.upload_name)
        f.sha1 = sha1
    f.sha256 = sha256sum(download_path)
    logger.info('sha256 sum of %s is %s', f.upload_name, f.sha256)


def calculate_backup_storage_path(f, non_public_wikis):
    """
    Return the target path on backup storage of a file after backup.
    Currently it will stored under wiki / hash-index / hash, with an
    additioanl '.age' extension for those files that are encripted.
    """
    backup_name = os.path.join(f.wiki, f.sha256[:3], f.sha256)  # e.g. enwiki/9f8/9f86d..8
    if f.wiki in non_public_wikis:
        backup_name += ".age"
    return backup_name


def handle_encryption(f, download_path, encryption, non_public_wikis):
    """Encrypt the file using the Encryption library if it is a private wiki"""
    if f.wiki in non_public_wikis:
        if encryption.encrypt(download_path) != 0:
            raise EncryptionException
        download_path += '.age'
    return download_path


def check_file_exists(backup_name, s3api):
    """Raises the right exception in case a file with the same calculated storage
       path on backups already exists."""
    if s3api.check_file_exists(backup_name):
        raise DuplicateException


def upload_file(download_path, backup_name, s3api):
    """Uploads the file into backup storage"""
    location = s3api.upload_file(download_path, backup_name)
    if location < 0:
        raise UploadException
    return location


def main():
    """Read list of pending files to backup, download, process them and upload to S3"""
    logger = logging.getLogger('backup')
    logging.basicConfig(format='[%(asctime)s] %(levelname)s:%(name)s %(message)s',
                        filename='backup_store.log', level=logging.WARNING)
    metadata = mediabackups.MySQLMetadata.MySQLMetadata(read_yaml_config(METADATA_CONFIG_FILE))
    storage_config = read_yaml_config(STORAGE_CONFIG_FILE)
    s3api = mediabackups.S3.S3(storage_config)
    encryption = mediabackups.Encryption.Encryption(storage_config['identity_file'])

    tmp_dir = os.path.join(storage_config.get('tmpdir', TMPDIR), str(os.getpid()))
    os.mkdir(tmp_dir)  # Ignore failures creating the dir
    metadata.connect_db()
    non_public_wikis = metadata.get_non_public_wikis()
    for batch in metadata.process_files():
        status_list = list()
        for file_id, f in batch.items():
            location = None
            try:
                download_path = download_file_from_production(f, tmp_dir)
                handle_checksums(f, download_path)
                backup_name = calculate_backup_storage_path(f, non_public_wikis)
                check_file_exists(backup_name, s3api)
                download_path = handle_encryption(f, download_path, encryption, non_public_wikis)
                location = upload_file(download_path, backup_name, s3api)
                new_status = 'backedup'
                logger.info('Backup of "%s" completed correctly', str(f))
            except DownloadException:
                logger.error('Download of "%s" failed', str(f))
                new_status = 'error'
            except EncryptionException:
                logger.error('Encryption of "%s" failed', str(f))
                new_status = 'error'
            except DuplicateException:
                logger.warning('A file with the same sha265 as "%s" was already '
                               'uploaded, skipping.', str(f))
                new_status = 'duplicate'
            except UploadException:
                logger.error('Upload of "%s" failed', str(f))
                new_status = 'error'
            finally:
                status_list.append({
                    'id': file_id, 'file': f, 'status': new_status, 'location': location}
                )
                try:
                    os.remove(download_path)
                except OSError:
                    pass  # ignoring errors as the file may not exist
        metadata.update_status(status_list)
    metadata.close_db()


if __name__ == "__main__":
    main()
