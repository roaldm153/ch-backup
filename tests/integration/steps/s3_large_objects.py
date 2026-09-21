"""
Steps for checking how large objects are stored in S3.

S3 ends the ETag of an object written in several parts with the number of those
parts, which is how a multipart copy is told from a single CopyObject.
"""

import re

from behave import then, when
from hamcrest import assert_that, equal_to, greater_than

from tests.integration.modules import s3
from tests.integration.modules.docker import get_container
from tests.integration.modules.typing import ContextT

MULTIPART_ETAG = re.compile(r'"?\w+-\d+"?')
ROW_SIZE = 1024


def _multipart_objects(context: ContextT, bucket: str, prefix: str) -> list[str]:
    """
    Return objects with given prefix that were written in several parts.
    """
    s3_client = s3.S3Client(context, bucket)
    return [
        obj["Key"]
        for obj in s3_client.list_objects_metadata(prefix)
        if MULTIPART_ETAG.fullmatch(obj["ETag"])
    ]


@when("we insert {size:d} GiB of incompressible data into {table} on {node:w}")
def step_insert_incompressible_data(context, size, table, node):
    """
    Fill a table with random data, which S3 stores as given.

    The size of a single table is overridden with -D load_table_gb=<size>.
    The query is run inside the container: it takes longer than the timeout of
    the HTTP client used by the rest of the steps.
    """
    size = int(context.config.userdata.get("load_table_gb", size))
    rows = size * 1024**3 // ROW_SIZE
    query = (
        f"INSERT INTO {table} "
        f"SELECT number, randomPrintableASCII({ROW_SIZE}) "
        f"FROM system.numbers_mt LIMIT {rows} SETTINGS max_insert_threads = 4"
    )

    result = get_container(context, node).exec_run(["clickhouse-client", "-q", query])
    assert result.exit_code == 0, result.output.decode()


@then(
    's3 bucket {bucket} contains an object larger than {size:d} bytes with prefix "{prefix}"'
)
def step_bucket_contains_object_larger_than(context, bucket, size, prefix):
    s3_client = s3.S3Client(context, bucket)
    sizes = [obj["Size"] for obj in s3_client.list_objects_metadata(prefix)]
    assert_that(
        max(sizes, default=0),
        greater_than(size),
        f"No object larger than {size} bytes with prefix {prefix} in bucket {bucket}",
    )


@then('s3 bucket {bucket} contains an object of several parts with prefix "{prefix}"')
def step_bucket_contains_multipart_object(context, bucket, prefix):
    objects = _multipart_objects(context, bucket, prefix)
    assert_that(
        len(objects) > 0,
        equal_to(True),
        f"No object of several parts with prefix {prefix} in bucket {bucket}",
    )


@then('s3 bucket {bucket} contains no objects of several parts with prefix "{prefix}"')
def step_bucket_contains_no_multipart_objects(context, bucket, prefix):
    objects = _multipart_objects(context, bucket, prefix)
    assert_that(
        objects,
        equal_to([]),
        f"Unexpected objects of several parts with prefix {prefix} in bucket {bucket}: {objects}",
    )
