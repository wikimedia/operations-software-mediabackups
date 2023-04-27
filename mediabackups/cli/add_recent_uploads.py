import datetime
import logging
import requests
import time

from mediabackups.MySQLMedia import MySQLMedia
from mediabackups.MySQLMetadata import MySQLMetadata
from mediabackups.Util import read_yaml_config


READ_CONFIG_FILE = '/etc/mediabackup/mw_db.conf'
WRITE_CONFIG_FILE = '/etc/mediabackup/mediabackups_db.conf'
HTTP_HEADERS = {'User-agent': 'mediabackups/recentuploads https://phabricator.wikimedia.org/diffusion/OSWB/'}


def get_latest_uploaded_files_since(date):
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
        result = requests.get(api_url, params=req, headers=HTTP_HEADERS).json()
        if 'error' in result:
            raise Exception(result['error'])
        if 'warnings' in result:
            print(result['warnings'])
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


def main():
    """
    Queries the api for recentchanges and keep monitoring it so last uploaded files
    get checked as potential new backups
    """
    logger = logging.getLogger('backup')
    logging.basicConfig(format='[%(asctime)s] %(levelname)s:%(name)s %(message)s',
                        filename='backup_quick_update.log', level=logging.DEBUG)

    config = read_yaml_config(READ_CONFIG_FILE)
    if 'wiki' not in config.keys():
        return
    wiki = config['wiki']

    mw = MySQLMedia(config)
    metadata = MySQLMetadata(config=read_yaml_config(WRITE_CONFIG_FILE))

    while True:
        mw.connect_db()
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
        mw.close_db()
        metadata.close_db()
        time.sleep(10)


if __name__ == "__main__":
    main()
