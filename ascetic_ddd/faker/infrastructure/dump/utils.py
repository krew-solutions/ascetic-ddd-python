import datetime

from ascetic_ddd.faker.infrastructure.dump.pg_dump import FileDump, GzipDump, PgDump
from ascetic_ddd.faker.infrastructure.dump.s3_dump import AwsCredentials, S3Dump
from ascetic_ddd.faker.infrastructure.dump.single_pg_dump import SinglePgDump

__all__ = ("make_dumper", "make_single_dumper",)


def make_single_dumper(
        base_path: str,
        ttl: datetime.timedelta,
        conninfo: str,
        aws_credentials: AwsCredentials | None = None
):
    dumper = SinglePgDump(
        base_path=base_path,
        ttl=ttl,
        conninfo=conninfo
    )
    if aws_credentials is not None:
        dumper = S3Dump(delegate=dumper, aws_credentials=aws_credentials)
    return dumper


def make_dumper(
        base_path: str,
        ttl: datetime.timedelta,
        conninfo: str,
        aws_credentials: AwsCredentials | None = None
):
    dumper = FileDump(
        delegate=GzipDump(
            delegate=PgDump(
                conninfo=conninfo
            )
        ),
        base_path=base_path,
        ttl=ttl
    )
    if aws_credentials is not None:
        dumper = S3Dump(delegate=dumper, aws_credentials=aws_credentials)
    return dumper
