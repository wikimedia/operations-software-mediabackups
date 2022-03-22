#!/usr/bin/python3

"""
Downloads all files of the given wiki and status to the local dir
"""
import sys

import mediabackups.SwiftMedia

wiki = sys.argv[1]
status = sys.argv[2]
config = {'wiki': wiki}
backup = mediabackups.SwiftMedia.SwiftMedia(config)

for batch in backup.list_files(status):
    for f in batch:
        print(f)
        mediabackups.SwiftMedia.SwiftMedia.download(f, '')
