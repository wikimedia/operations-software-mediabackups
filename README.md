Collection of Python classes to implement Wikimedia project's multimeda file's (Commons originals and other local project files on OpenStack Swift) recovery system (backups and restores).

More info: https://wikitech.wikimedia.org/wiki/Media_storage/Backups

## Run tests

Tests are located under *mediabackups/test*. They are split between unit and integration tests. To run unit tests:

```
tox -e unit
```

## Code style compliance

To check the code style compliance:

```
tox -e flake8
```

## Packaging

To create debian packages:

```
dch  # to add new changelog entry
debuild -b -us -uc
```

## Database creation

To create the database objects and users to track/check the metadata:

```
mysql -e "CREATE DATABASE mediabackups DEFAULT CHARACTER SET binary"
mysql mediabackups < sql/mediabackups.sql
# user to update the backup metadata
mysql -e "CREATE USER backupuser IDENTIFIED BY 'a_password'; GRANT SELECT, INSERT, UPDATE, DELETE ON mediabackups.* TO backupuser"
```

Adapt your mysql connection properties, passwords, database name and user accounts, that you later will configure at /etc/mediabackups/*.cnf and nagios options.
