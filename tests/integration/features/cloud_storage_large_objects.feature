Feature: Copy of cloud storage objects that do not fit into a single CopyObject

  # ClickHouse copies an object in several parts once it grows above
  # max_single_operation_copy_size, and S3 refuses a single CopyObject above
  # 5 GiB in any case. An object of that size does not fit into a test
  # environment, so the disk of the 's3_multipart' policy lowers the threshold
  # to 5 MiB, the smallest part S3 accepts, and a few dozen megabytes take the
  # same path.
  #
  # This is not a part of the usual suite: it is slower than the rest and its
  # value is in being run against a real installation.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  @object_storage_large_copy
  @require_version_24.1
  Scenario: Restore from a copy of an object copied in several parts
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        UserID  UInt32,
        Payload String
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3_multipart', min_bytes_for_wide_part = 1000000000;

    INSERT INTO test_db.table_s3
    SELECT number, randomPrintableASCII(1024) FROM system.numbers LIMIT 20000;
    """
    # Random data does not compress, so the part is a single object of about
    # 20 MiB. Without this check the scenario would pass on an object copied in
    # one operation, which the rest of the suite covers anyway.
    Then s3 bucket cloud-storage-01 contains an object larger than 5242880 bytes with prefix "data_multipart/"
    # The write itself stayed in one part, so a multipart ETag further on can
    # only have been left by a copy.
    And s3 bucket cloud-storage-01 contains no objects of several parts with prefix "data_multipart/"
    When we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup
    copy_cloud_storage_data: true
    """
    Then s3 bucket ch-backup contains an object of several parts with prefix "ch_backup/test_backup/cloud_storage/s3_multipart/"
    When we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup #0 to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02
    # Restore copies the data back the same way, so it takes the multipart path
    # as well. Every instance has a bucket of its own, and this is the one of
    # the instance restored to.
    And s3 bucket cloud-storage-02 contains an object of several parts with prefix "data_multipart/"
