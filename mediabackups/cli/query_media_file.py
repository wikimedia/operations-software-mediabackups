#!/usr/bin/python3
"""
Interactive command line application to query a file or set of
files with a set of given parameters (wiki, hash, title, etc.).
"""

from mediabackups.MetadataQuery import MetadataQuery


def main():
    """
    Query a file or a list of files from media backups and print it on screen/logs.
    """
    MetadataQuery.execute_action('query')


if __name__ == "__main__":
    main()
