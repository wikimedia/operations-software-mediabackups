#!/usr/bin/python3
"""
Interactive command line application to restore a previously backed up file
or files with a set given parameters (wiki, hash, title, etc.).
"""


from mediabackups.MetadataQuery import MetadataQuery


def main():
    """
    Recover a file or a list of file from media backups
    and write it to the local filesystem.
    """
    MetadataQuery.execute_action('recovery')


if __name__ == "__main__":
    main()
