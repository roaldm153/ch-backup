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
  @require_version_24.1
  Scenario: Deleting a backup deletes copied cloud storage data
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

    INSERT INTO test_db.table_s3 SELECT 0, number FROM system.numbers LIMIT 100;
    """
    # ClickHouse writes cloud storage data under a name with '-' replaced by '_',
    # so a dashed name is the case where deletion can miss the data.
    When we create clickhouse01 clickhouse backup
    """
    name: test-backup
    copy_cloud_storage_data: true
    """
    Then s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup/cloud_storage/s3/"
    When we delete clickhouse01 clickhouse backup #0
    Then s3 bucket ch-backup contains no objects with prefix "ch_backup/test_backup/"
    And s3 bucket ch-backup contains no objects with prefix "ch_backup/test-backup/"

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

  @object_storage_copy
  @require_version_24.1
  Scenario Outline: Restore from a copy of cloud storage data of <part_format> parts
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32,
        Payload   String
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 8
    ORDER BY UserID
    SETTINGS storage_policy = 's3', min_bytes_for_wide_part = <min_bytes_for_wide_part>;

    SYSTEM STOP MERGES test_db.table_s3;

    INSERT INTO test_db.table_s3 SELECT number, number, repeat('a', 128) FROM system.numbers LIMIT 2000;
    INSERT INTO test_db.table_s3 SELECT number, number, repeat('b', 128) FROM system.numbers LIMIT 2000;
    INSERT INTO test_db.table_s3 SELECT number, number, repeat('c', 128) FROM system.numbers LIMIT 2000;
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.parts
    WHERE database = 'test_db' AND table = 'table_s3' AND active AND part_type != '<part_format>'
    """
    Then we get response
    """
    0
    """
    When we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup
    copy_cloud_storage_data: true
    """
    Then s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup/cloud_storage/s3/"
    When we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup #0 to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02

    Examples:
      | part_format | min_bytes_for_wide_part |
      | Compact     | 10000000                |
      | Wide        | 0                       |

  @object_storage_copy
  @require_version_24.1
  Scenario: Restore from a copy of cloud storage data spread over two disks
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32,
        Payload   String
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID
    ORDER BY UserID
    SETTINGS storage_policy = 'multiple_s3';

    INSERT INTO test_db.table_s3 SELECT 0, number, repeat('a', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 1, number, repeat('b', 256) FROM system.numbers LIMIT 1000;

    ALTER TABLE test_db.table_s3 MOVE PARTITION 1 TO DISK 's3_second';
    """
    # Without this check the scenario silently degrades to the single disk case.
    When we execute query on clickhouse01
    """
    SELECT countDistinct(disk_name) FROM system.parts
    WHERE database = 'test_db' AND table = 'table_s3' AND active
    """
    Then we get response
    """
    2
    """
    When we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup
    copy_cloud_storage_data: true
    """
    Then s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup/cloud_storage/s3/"
    And s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup/cloud_storage/s3_second/"
    When we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup #0 to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02

  @object_storage_copy
  @require_version_24.1
  Scenario: Restore a table stored on both a local and a cloud storage disk
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32,
        Payload   String
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID
    ORDER BY UserID
    SETTINGS storage_policy = 's3_cold';

    INSERT INTO test_db.table_s3 SELECT 0, number, repeat('a', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 1, number, repeat('b', 256) FROM system.numbers LIMIT 1000;

    ALTER TABLE test_db.table_s3 MOVE PARTITION 1 TO VOLUME 'external';
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.parts
    WHERE database = 'test_db' AND table = 'table_s3' AND active AND disk_name = 'default'
    """
    Then we get response
    """
    1
    """
    When we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup
    copy_cloud_storage_data: true
    """
    Then s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup/cloud_storage/s3/"
    When we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup #0 to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02

  @object_storage_copy
  @require_version_24.1
  Scenario: Backup of a table without data on a cloud storage disk needs no source bucket
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
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test_backup
    copy_cloud_storage_data: true
    """
    Then s3 bucket ch-backup contains no objects with prefix "ch_backup/test_backup/cloud_storage/"
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And on clickhouse02 tables are empty

  @object_storage_copy
  @require_version_24.1
  Scenario: Repeated backup of an unchanged table copies no data
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32,
        Payload   String
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_s3 SELECT 0, number, repeat('a', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 1, number, repeat('b', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 2, number, repeat('c', 256) FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test_backup1
    copy_cloud_storage_data: true
    """
    And we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup2
    copy_cloud_storage_data: true
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 3          |
      | 1   | created | 3          | 0          |
    And s3 bucket ch-backup contains no objects with prefix "ch_backup/test_backup2/cloud_storage/"
    When we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup #0 to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02

  @object_storage_copy
  @require_version_24.1
  Scenario: Deleting a backup keeps data reused by a later one
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32,
        Payload   String
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_s3 SELECT 0, number, repeat('a', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 1, number, repeat('b', 256) FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test_backup1
    copy_cloud_storage_data: true
    """
    And we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup2
    copy_cloud_storage_data: true
    """
    And we delete clickhouse01 clickhouse backup #1
    Then we got the following backups on clickhouse01
      | num | state             | data_count | link_count |
      | 0   | created           | 0          | 2          |
      | 1   | partially_deleted | 2          | 0          |
    And s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup1/cloud_storage/s3/"
    When we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup #0 to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02

  @object_storage_copy
  @require_version_24.1
  Scenario: Restore of a backup made after a mutation renaming parts
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32,
        Payload   String
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_s3 SELECT 0, number, repeat('a', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 1, number, repeat('b', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 2, number, repeat('c', 256) FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test_backup1
    copy_cloud_storage_data: true
    """
    # Parts of untouched partitions keep their data and get a mutation suffix,
    # which is the case deduplication has to see through.
    And we execute queries on clickhouse01
    """
    ALTER TABLE test_db.table_s3
    UPDATE Payload = repeat('z', 256) WHERE CounterID = 0
    SETTINGS mutations_sync = 2;
    """
    And we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup2
    copy_cloud_storage_data: true
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 2          |
      | 1   | created | 3          | 0          |
    When we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup #0 to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02
