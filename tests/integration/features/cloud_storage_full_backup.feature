Feature: Full backup of cloud storage data

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  @object_storage_copy
  @require_version_24.1
  Scenario: Restore from a copy of cloud storage data after the source bucket is lost
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32,
        Payload   String
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 3
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_s3 SELECT 0, number, repeat('a', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 1, number, repeat('b', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 2, number, repeat('c', 256) FROM system.numbers LIMIT 1000;
    """
    When we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup
    copy_cloud_storage_data: true
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 3          | 0          |
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf show --pretty test_backup
    """
    Then we get response contains
    """
    "data_copied": true
    """
    And s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup/cloud_storage/s3/"
    # Without this step the restore would silently read the original objects
    # and the scenario would pass even if nothing had been copied.
    When we delete all objects in s3 bucket cloud-storage-01
    Then s3 bucket cloud-storage-01 contains 0 objects
    When we restore clickhouse backup #0 to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02

  @object_storage_copy
  @require_version_22.8
  Scenario: Restore without copied data still requires the source bucket
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_s3 SELECT 0, number FROM system.numbers LIMIT 10;
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test_backup
    """
    And we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore test_backup
    """
    Then we get response contains
    """
    Cloud storage source bucket must be set
    """
