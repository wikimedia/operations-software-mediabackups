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

    def __eq__(self, other):
        """Define """
        return vars(self) == vars(other)

    def __hash__(self):
        """
        Return a consistend hash of a file using its sha1 hash.
        The sha1 hash should always be present and unmutable.
        """
        return hash(self.sha1)

    @staticmethod
    def row2File(row, string_wiki, string_file_type, string_status, string_container):
        """Converts a dictionary containing the property keys (usually obtained
           from querying a database) and returns a corresponding File object"""
        return File(wiki=string_wiki.get(row['wiki']),
                    upload_name=(row['upload_name'].decode('utf-8')
                                 if row['upload_name'] is not None else None),
                    size=row.get('size'),
                    type=string_file_type.get(row.get('file_type')),
                    status=string_status.get(row['status']),
                    upload_timestamp=row.get('upload_timestamp'),
                    deleted_timestamp=row.get('deleted_timestamp'),
                    archived_timestamp=row.get('archived_timestamp'),
                    md5=(row['md5'].decode('utf-8')
                         if row['md5'] is not None else None),
                    sha1=(row['sha1'].decode('utf-8')
                          if row['sha1'] is not None else None),
                    sha256=(row['sha256'].decode('utf-8')
                            if row.get('sha256') is not None else None),
                    storage_container=string_container.get(row['storage_container']),
                    storage_path=(row['storage_path'].decode('utf-8')
                                  if row['storage_path'] is not None else None))

    def properties(self):
        """
        Returns a list with the file properties, in the expected
        persisting (database) format
        """
        return {'wiki': self.wiki,
                'upload_name': self.upload_name,
                'size': self.size,
                'file_type': self.type,
                'status': self.status,
                'upload_timestamp': self.upload_timestamp,
                'archived_timestamp': self.archived_timestamp,
                'deleted_timestamp': self.deleted_timestamp,
                'md5': self.md5,
                'sha1': self.sha1,
                'storage_container': self.storage_container,
                'storage_path': self.storage_path}

    def __repr__(self):
        return (str(self.wiki or '') + ' ' + str(self.upload_name or '') +
                ' ' + str(self.sha1 or '') + ' ' + str(self.upload_timestamp))
