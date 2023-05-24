#!/usr/bin/python3

"""
Command line utility that performs the reading of image metadata
from a mediawiki database and performs a full backup of all the media
files from a wiki or a section.
"""

import logging
import os
import sys

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


class UploadException(Exception):
    """Raised on errors while trying to uplaod the file into the backup storage"""


class DuplicateException(Exception):
    """Raised in case the file already exists on the backup storage"""


class EncryptionException(Exception):
    """Raised in case attempting to encrypt a file fails"""


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


def download_and_backup(f, tmp_dir, download_path, non_public_wikis, s3api, encryption):
    """
    Given a file, download it from Swift to tmp_dir locally, process it (hash, encrypt, etc)
    and upload to backup through S3 api. Return the backup status ("backed up") and its
    final upload path (location), or rise an exception of type DownloadException,
    EncryptionException, DuplicateException or UploadException.
    """
    logger = logging.getLogger('backup')
    download_path = download_file_from_production(f, tmp_dir)
    handle_checksums(f, download_path)
    backup_name = calculate_backup_storage_path(f, non_public_wikis)
    check_file_exists(backup_name, s3api)
    download_path = handle_encryption(f, download_path, encryption, non_public_wikis)
    location = upload_file(download_path, backup_name, s3api)
    logger.info('Backup of "%s" completed correctly', str(f))
    return 'backedup', location


def remove_tmp_dir(tmp_dir):
    """Remove the temporary local dir created to download backups (log but ignore errors)"""
    logger = logging.getLogger('backup')
    try:
        os.rmdir(tmp_dir)
    except FileNotFoundError:
        logger.error('Temporary download directory %s could not be deleted: it was not found', tmp_dir)
    except OSError:
        logger.error('Temporary download directory %s could not be deleted: it is not empty', tmp_dir)


def create_tmp_dir(storage_config):
    """
    Creates a unique (for the process) subdirectory inside the path given in the storage
    configuration, otherwise use TMPDIR as default; and return it; exit if it fails.
    We do not use a real temporary disk because we need it a) to make sure it is on disk,
    not a ramdisk or other virtual mapping of the filesystem to memory, and b) the partition
    is large enough to contain (potentially multiple) 4GB video files.
    """
    # use the process PID to guarantee uniqueness
    logger = logging.getLogger('backup')
    tmp_dir = os.path.join(storage_config.get('tmpdir', TMPDIR), str(os.getpid()))
    try:
        os.mkdir(tmp_dir)
    except PermissionError:
        logger.error("The download dir %s could not be created due to a permission problem", tmp_dir)
        sys.exit(255)
    except FileExistsError:
        logger.error("The download dir %s could not be created because it already exists", tmp_dir)
        sys.exit(254)
    except FileNotFoundError:
        logger.error("The download dir %s could not be created because its parent dir does not exist", tmp_dir)
        sys.exit(253)
    return tmp_dir


def main():
    """Read list of pending files to backup, download, process them and upload to S3"""
    # Setup all needed interfaces
    logger = logging.getLogger('backup')
    logging.basicConfig(format='[%(asctime)s] %(levelname)s:%(name)s %(message)s',
                        filename='backup_store.log', level=logging.WARNING)
    metadata = mediabackups.MySQLMetadata.MySQLMetadata(read_yaml_config(METADATA_CONFIG_FILE))
    storage_config = read_yaml_config(STORAGE_CONFIG_FILE)
    s3api = mediabackups.S3.S3(storage_config)
    encryption = mediabackups.Encryption.Encryption(storage_config['identity_file'])
    tmp_dir = create_tmp_dir(storage_config)
    metadata.connect_db()
    non_public_wikis = metadata.get_non_public_wikis()

    # loop over pending files to be backed up
    for batch in metadata.process_files():
        status_list = list()
        for file_id, f in batch.items():
            location = None
            download_path = None
            try:
                new_status, location = download_and_backup(f, tmp_dir, download_path, non_public_wikis,
                                                           s3api, encryption)
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
                    if download_path is not None:
                        os.remove(download_path)
                except OSError:
                    pass  # ignoring errors as the file may not exist
        metadata.update_status(status_list)

    # cleanup and finish
    metadata.close_db()
    remove_tmp_dir(tmp_dir)


if __name__ == "__main__":
    main()
