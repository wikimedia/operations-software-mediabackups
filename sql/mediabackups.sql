--
-- Table structure for table `backup_status`
--

CREATE TABLE `backup_status` (
  `id` tinyint(3) unsigned NOT NULL AUTO_INCREMENT,
  `backup_status_name` varbinary(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

--
-- Table structure for table `backups`
--

CREATE TABLE `backups` (
  `location` tinyint(3) unsigned NOT NULL,
  `wiki` int(10) unsigned NOT NULL,
  `sha256` varbinary(64) NOT NULL,
  `sha1` varbinary(40) NOT NULL,
  `backup_time` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`location`,`wiki`,`sha256`),
  KEY `sha1` (`sha1`),
  KEY `backup_time` (`backup_time`),
  KEY `wiki_sha256` (`wiki`,`sha256`),
  KEY `wiki_sha1` (`wiki`,`sha1`),
  CONSTRAINT `backups_ibfk_1` FOREIGN KEY (`location`) REFERENCES `locations` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

--
-- Table structure for table `file_history`
--

CREATE TABLE `file_history` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `wiki` int(10) unsigned NOT NULL,
  `upload_name` varbinary(255) DEFAULT NULL,
  `storage_container` int(10) unsigned DEFAULT NULL,
  `storage_path` varbinary(270) DEFAULT NULL,
  `file_type` tinyint(3) unsigned DEFAULT NULL,
  `status` tinyint(3) unsigned DEFAULT NULL,
  `sha1` varbinary(40) DEFAULT NULL,
  `md5` varbinary(32) DEFAULT NULL,
  `size` bigint(20) unsigned DEFAULT NULL,
  `upload_timestamp` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `archived_timestamp` timestamp NULL DEFAULT NULL,
  `deleted_timestamp` timestamp NULL DEFAULT NULL,
  `file_update` timestamp NOT NULL DEFAULT current_timestamp(),
  `file_id` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `sha1` (`sha1`),
  KEY `file_type` (`file_type`),
  KEY `status` (`status`),
  KEY `upload_name` (`upload_name`,`status`),
  KEY `upload_timestamp` (`upload_timestamp`),
  KEY `md5` (`md5`),
  KEY `wiki_status_backup_status` (`wiki`,`status`, `backup_status`),
  KEY `wiki_status_upload_timestamp` (`wiki`,`status`, `upload_timestamp`),
  KEY `wiki_backup_status` (`wiki`,`backup_status`)
  KEY `location` (`storage_container`,`storage_path`),
  KEY `archived_timestamp` (`archived_timestamp`),
  KEY `deleted_timestamp` (`deleted_timestamp`),
  KEY `file_id` (`file_id`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

--
-- Table structure for table `file_status`
--

CREATE TABLE `file_status` (
  `id` tinyint(3) unsigned NOT NULL AUTO_INCREMENT,
  `status_name` varbinary(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

--
-- Table structure for table `file_types`
--

CREATE TABLE `file_types` (
  `id` tinyint(3) unsigned NOT NULL AUTO_INCREMENT,
  `Type_name` varbinary(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

--
-- Table structure for table `files`
--

CREATE TABLE `files` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `wiki` int(10) unsigned NOT NULL,
  `upload_name` varbinary(255) DEFAULT NULL,
  `storage_container` int(10) unsigned DEFAULT NULL,
  `storage_path` varbinary(270) DEFAULT NULL,
  `file_type` tinyint(3) unsigned DEFAULT NULL,
  `status` tinyint(3) unsigned DEFAULT NULL,
  `sha1` varbinary(40) DEFAULT NULL,
  `md5` varbinary(32) DEFAULT NULL,
  `size` bigint(20) unsigned DEFAULT NULL,
  `upload_timestamp` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `archived_timestamp` timestamp NULL DEFAULT NULL,
  `deleted_timestamp` timestamp NULL DEFAULT NULL,
  `backup_status` tinyint(3) unsigned DEFAULT 1,
  PRIMARY KEY (`id`),
  KEY `sha1` (`sha1`),
  KEY `file_type` (`file_type`),
  KEY `status` (`status`),
  KEY `backup_status` (`backup_status`),
  KEY `upload_name` (`upload_name`,`status`),
  KEY `upload_timestamp` (`upload_timestamp`),
  KEY `md5` (`md5`),
  KEY `wiki_status_backup_status` (`wiki`,`status`,`backup_status`),
  KEY `wiki_status_upload_timestamp` (`wiki`,`status`, `upload_timestamp`),
  KEY `wiki_backup_status` (`wiki`,`backup_status`),
  KEY `location` (`storage_container`,`storage_path`),
  KEY `archived_timestamp` (`archived_timestamp`),
  KEY `deleted_timestamp` (`deleted_timestamp`),
  CONSTRAINT `files_ibfk_1` FOREIGN KEY (`file_type`) REFERENCES `file_types` (`id`),
  CONSTRAINT `files_ibfk_2` FOREIGN KEY (`status`) REFERENCES `file_status` (`id`),
  CONSTRAINT `files_ibfk_3` FOREIGN KEY (`wiki`) REFERENCES `wikis` (`id`),
  CONSTRAINT `files_ibfk_4` FOREIGN KEY (`backup_status`) REFERENCES `backup_status` (`id`),
  CONSTRAINT `files_ibfk_6` FOREIGN KEY (`storage_container`) REFERENCES `storage_containers` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

--
-- Table structure for table `locations`
--

CREATE TABLE `locations` (
  `id` tinyint(3) unsigned NOT NULL AUTO_INCREMENT,
  `location_name` varbinary(100) NOT NULL,
  `endpoint_url` varbinary(500) DEFAULT NULL,
  `writes_enabled` tinyint(1) DEFAULT 1,
  PRIMARY KEY (`id`),
  UNIQUE KEY `location_name` (`location_name`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

--
-- Table structure for table `storage_container_types`
--

CREATE TABLE `storage_container_types` (
  `id` tinyint(3) unsigned NOT NULL AUTO_INCREMENT,
  `storage_container_type_name` varbinary(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

--
-- Table structure for table `storage_containers`
--

CREATE TABLE `storage_containers` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `storage_container_name` varbinary(270) NOT NULL,
  `wiki` int(10) unsigned DEFAULT NULL,
  `type` tinyint(3) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `swift_container_name` (`storage_container_name`),
  KEY `wiki` (`wiki`),
  KEY `type` (`type`),
  CONSTRAINT `storage_containers_ibfk_1` FOREIGN KEY (`wiki`) REFERENCES `wikis` (`id`),
  CONSTRAINT `storage_containers_ibfk_2` FOREIGN KEY (`type`) REFERENCES `storage_container_types` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

--
-- Table structure for table `wiki_types`
--

CREATE TABLE `wiki_types` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `type_name` varbinary(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

--
-- Table structure for table `wikis`
--

CREATE TABLE `wikis` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `wiki_name` varbinary(255) DEFAULT NULL,
  `type` tinyint(3) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `wiki_name` (`wiki_name`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;
