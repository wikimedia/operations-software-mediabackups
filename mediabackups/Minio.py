"""
Implements the Minio class, equivalent in functionality to the S3, but using
the specific minio module, in case some Minio-only feature is needed in the
future.
"""
import logging
import os

from minio import Minio as minioapi
from minio.error import S3Error

from mediabackups.Util import sha1sum


class Minio:
    """
    Implements a simplified api to access, manage, download and upload
    files into a Minio server
    """

    def __init__(self, config):
        """Constructor"""
        self.bucket = config.get('bucket', 'mediabackups')
        # TODO: read client ids from the database, not file
        self.endpoints = config.get('endpoints', list())
        self.clients = []

        for endpoint in self.endpoints:
            self.clients.append(minioapi(
                endpoint.lstrip('https://'),
                access_key=config.get('access_key'),
                secret_key=config.get('secret_key'),
                secure=True,
            ))

    def check_file_exists(self, upload_name, shard=None):
        """
        Returns true if a file already exist on the bucket with the same exact name
        (virtual path) without downloading it fully, regardless of content.
        Otherwise, return false.
        """
        if shard is not None:
            client = self.clients[self.endpoints.index(shard)]
        else:
            _, client = self.find_shard(upload_name)
        try:
            result = client.list_objects(self.bucket, prefix=upload_name)
        except S3Error:
            return False
        return result

    def find_shard(self, upload_name):
        """
        Given a proposed full path of a file to upload, return the shard
        for the file to be uploaded to
        """
        num_shards = len(self.clients)
        # hashes starting with 0, 1, 2, 3 go to shard 0; 4, 5, 6, 7 -> 1;
        # 8, 9, a, b -> 2; c, d, e, f -> 3
        shard = int(upload_name.split('/')[-1][0], 16) // num_shards
        return shard + 1, self.clients[shard]

    def upload_file(self, file_path, upload_name):
        """
        Uploads given local file file_path into the s3 location with the
        upload_name virtual path/identifier.
        Returns -1 on failures, a 0/positive numeric indentifier of the shard
        used if successful.
        """
        location_id, client = self.find_shard(upload_name)
        try:
            client.fput_object(self.bucket, upload_name, file_path)
        except S3Error as ex:
            logging.error(ex)
            return -1
        return location_id

    def download_file(self, endpoint_url, download_name, local_path):
        """
        Downloads the given s3 location (download_name) from the given
        server (endpoint_url) and saves it into the local file name,
        local_path.
        Returns 0 on failure, -1 on error.
        """
        client = self.clients[self.endpoints.index(endpoint_url)]
        try:
            client.fget_object(self.bucket, download_name, local_path)
        except S3Error as ex:
            logging.error(ex)
            return -1
        return 0

    def upload_dir(self, parent_dir, wiki):
        """
        Uploads of files within the directory named as the wiki, located in the
        parent_dir directory, into the S3-backed-api service, with
        the wiki/filename/sha1 virtual file structure.
        Returns different from 0 if at least one upload failed.
        """
        file_dir = os.path.join(parent_dir, wiki)
        for filename in os.listdir(file_dir):
            print(filename)
            path = os.path.join(file_dir, filename)
            sha1 = sha1sum(path)
            result = self.upload_file(path, os.path.join(wiki, filename, sha1))
            if result != 0:
                exit_code = -1
        return exit_code
