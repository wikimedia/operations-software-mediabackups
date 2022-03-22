"""The File module implements the File class."""


class File:
    """
    A File object representas and stores the metadata for an individual image,
    video, document or any other object that can be uploaded to a WMF site.
    """
    def __init__(self, wiki, upload_name, status, size=None, type=None,
                 upload_timestamp=None, deleted_timestamp=None, sha1=None,
                 sha256=None, md5=None, storage_container=None, storage_path=None,
                 archived_timestamp=None):
        self.wiki = wiki
        self.upload_name = upload_name
        self.size = size
        self.type = type if type else 'ERROR'
        self.status = status
        self.upload_timestamp = upload_timestamp
        self.deleted_timestamp = deleted_timestamp
        self.archived_timestamp = archived_timestamp
        self.sha1 = sha1
        self.sha256 = sha256
        self.md5 = md5
        self.storage_container = storage_container
        self.storage_path = storage_path

    def properties(self):
        """
        Returns a list with the file properties, in the expected
        persisting (database) format
        """
        return {'wiki': self.wiki,
                'upload_name': self.upload_name,
                'file_type': self.type,
                'status': self.status,
                'sha1': self.sha1,
                'md5': self.md5,
                'size': self.size,
                'upload_timestamp': self.upload_timestamp,
                'archived_timestamp': self.archived_timestamp,
                'deleted_timestamp': self.deleted_timestamp,
                'storage_container': self.storage_container,
                'storage_path': self.storage_path}

    def __repr__(self):
        return (str(self.wiki or '') + ' ' + str(self.upload_name or '') +
                ' ' + str(self.sha1 or ''))
