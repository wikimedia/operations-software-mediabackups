#!/usr/bin/python3

import argparse
import datetime
import json
import logging
import requests
import time

from mediabackups.MySQLMedia import MySQLMedia
from mediabackups.MySQLMetadata import MySQLMetadata
from mediabackups.Util import read_yaml_config


READ_CONFIG_FILE = '/etc/mediabackup/mw_db.conf'
WRITE_CONFIG_FILE = '/etc/mediabackup/mediabackups_db.conf'
HTTP_HEADERS = {'User-agent': 'mediabackups/recentuploads https://phabricator.wikimedia.org/diffusion/OSWB/'}
DEFAULT_WAIT_TIME_API_REQUESTS = 10
DEFAULT_WAIT_TIME_BETWEEN_BATCHES = 1
DEFAULT_WIKI = 'commonswiki'


def get_latest_uploaded_files_since(date):
    logger = logging.getLogger('backup')

    # API endpoint for recent file uploads
    api_url = 'https://commons.wikimedia.org/w/api.php'
    request = {'action': 'query', 'list': 'logevents', 'letype': 'upload',
               'leprop': 'title|timestamp|user|comment|details',
               'format': 'json',
               'lestart': date.isoformat(),
               'ledir': 'newer',
               'lelimit': 'max'}

    lastContinue = {}

    while True:
        req = request.copy()
        req.update(lastContinue)
        try:
            result = requests.get(api_url, params=req, headers=HTTP_HEADERS).json()
        except json.decoder.JSONDecodeError as ex:
            # This will happen on HTTP errors (e.g. we receive a 50X error)
            logger.error("Error while decoding the json response: %s", ex)
            break
        if 'error' in result:
            logger.error("Error returned by the API call: %s", result['error'])
        if 'warnings' in result:
            logger.warning(result['warnings'])
        if 'query' in result:
            yield result['query']
        if 'continue' not in result:
            break
        lastContinue = result['continue']


def format_api_result(upload):
    """
    Returns a result from query the log api into an easy to work with
    dictionary
    """
    title = upload['title'].strip().replace(' ', '_').removeprefix('File:')
    params = upload.get('params')
    sha1 = None
    upload_timestamp = None
    if params is not None:
        sha1 = params.get('img_sha1')
        if params.get('img_timestamp') is None:
            upload_timestamp = None
        else:
            upload_timestamp = datetime.datetime.strptime(params.get('img_timestamp'), "%Y-%m-%dT%H:%M:%SZ")
    return {'title': title, 'sha1': sha1, 'upload_timestamp': upload_timestamp}


def parse_arguments():
    """
    Reads the input arguments and returns them in the form of an object
    """
    parser = argparse.ArgumentParser(description=('Starts monitoring recentchanges of a given wiki since the last '
                                                  'backed up file and inserts metadata from newly uploaded files '
                                                  'into the pending list of files to backup. Send a SIGINT to the '
                                                  'process (or ctrl-c, if in an interactive session) to stop it.'))
    parser.add_argument('--wiki', '-w', required=True,
                        help=('Wiki name, as it appears on dblist files, to monitor for updates. '
                              'Example: --wiki=commonswiki'))
    parser.add_argument('--api-wait-time', '-a', default=DEFAULT_WAIT_TIME_API_REQUESTS,
                        help=('Pause the given amount of time, given in seconds, between each new API request.'
                              f'By default, {DEFAULT_WAIT_TIME_API_REQUESTS} second(s).'))
    parser.add_argument('--batch-wait-time', '-b', default=DEFAULT_WAIT_TIME_BETWEEN_BATCHES,
                        help=('Pause the given amount of time, given in seconds, between each batch request.'
                              f'By default, {DEFAULT_WAIT_TIME_BETWEEN_BATCHES} second(s).'))
    arguments = parser.parse_args().__dict__
    return arguments


def main():
    """
    Queries the api for recentchanges and keep monitoring it so last uploaded files
    get checked as potential new backups
    """
    logger = logging.getLogger('backup')
    logging.basicConfig(format='[%(asctime)s] %(levelname)s:%(name)s %(message)s',
                        filename='backup_quick_update.log', level=logging.INFO)

    arguments = parse_arguments()
    config = read_yaml_config(READ_CONFIG_FILE)
    wiki = arguments.get('wiki', DEFAULT_WIKI)
    api_wait_time = arguments.get('api_wait_time', DEFAULT_WAIT_TIME_API_REQUESTS)
    batch_wait_time = arguments.get('batch_wait_time', DEFAULT_WAIT_TIME_BETWEEN_BATCHES)

    mw = MySQLMedia(config)
    metadata = MySQLMetadata(config=read_yaml_config(WRITE_CONFIG_FILE))

    while True:
        mw.connect_db(wiki)
        metadata.connect_db()
        last_timestamp = metadata.get_latest_upload_time(wiki)
        for events in get_latest_uploaded_files_since(last_timestamp):
            batch = []
            uploads = events['logevents']
            # Loop through the list of changes and print the relevant information
            for upload in uploads:
                result = format_api_result(upload)
                batch.append(result)
            files = mw.query_files(batch)
            for f in files:
                logger.debug(f)
            metadata.check_and_update(wiki, files)
            time.sleep(batch_wait_time)
        mw.close_db()
        metadata.close_db()
        time.sleep(api_wait_time)


if __name__ == "__main__":
    main()
