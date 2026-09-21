"""
Steps for checking how large objects are stored in S3.

S3 ends the ETag of an object written in several parts with the number of those
parts, which is how a multipart copy is told from a single CopyObject.
"""

import re

from behave import then
from hamcrest import assert_that, equal_to, greater_than

from tests.integration.modules import s3
from tests.integration.modules.typing import ContextT

MULTIPART_ETAG = re.compile(r'"?\w+-\d+"?')


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
