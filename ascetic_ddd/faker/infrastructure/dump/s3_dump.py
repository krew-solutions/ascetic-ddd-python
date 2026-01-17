import datetime
import functools
import dataclasses
import logging
import shutil
import tarfile
import os.path

try:
    import boto3
except ImportError:
    boto3 = object()

try:
    import botocore
except ImportError:
    botocore = object()


from dateutil import tz

from ascetic_ddd.faker.infrastructure.dump.interfaces import IFileDump

__all__ = ("S3Dump", "AwsCredentials",)


@dataclasses.dataclass(kw_only=True, frozen=True)
class AwsCredentials:
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_account_id: str
    aws_bucket: str = 'springdel-fitness-functions'


class S3Dump(IFileDump):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
    """
    _delegate: IFileDump
    _aws_credentials: AwsCredentials

    def __init__(
            self,
            delegate: IFileDump,
            aws_credentials: AwsCredentials,
    ):
        self._aws_credentials = aws_credentials
        self._delegate = delegate
        self._logger = logging.getLogger(__name__)

    @functools.cached_property
    def client(self):
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#configuring-account-id
        """
        client = boto3.client(
            's3',
            aws_access_key_id=self._aws_credentials.aws_access_key_id,
            aws_secret_access_key=self._aws_credentials.aws_secret_access_key,
            aws_account_id=self._aws_credentials.aws_account_id
        )
        """
        session = boto3.Session(
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_access_key_id,
            aws_account_id=self._aws_account_id
        )
        """
        return client

    @property
    def ttl(self) -> datetime.timedelta:
        return self._delegate.ttl

    async def exists(self, name: str) -> bool:
        if await self._delegate.exists(name):
            return True

        remote_archive_filename = self._make_remote_filepath(name)
        try:
            remote_head = self.client.head_object(
                Bucket=self._aws_credentials.aws_bucket,
                Key=remote_archive_filename,
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                self._logger.info("Remote %s is not found", remote_archive_filename)
                return False
            else:
                raise
        else:
            if remote_head:
                delta = datetime.datetime.now(tz.tzutc()) - remote_head['LastModified']
                if delta < self.ttl:
                    self._logger.info("Remote dump %s exists", remote_archive_filename)
                    return True
            self._logger.info("Remote dump %s does not exist", remote_archive_filename)
        return False

    async def _download(self, name: str) -> bool:
        local_filename = self._delegate.make_filepath(name)
        local_archive_filename = self._make_local_archive_filepath(name)
        remote_archive_filename = self._make_remote_filepath(name)

        if os.path.exists(local_filename):
            shutil.rmtree(local_filename)
            self._logger.info("Remove local %s", local_filename)
            """
            self.client.delete_object(
                self._aws_credentials.aws_bucket,
                remote_archive_filename,
            )
            self._logger.info("Remove remote %s", remote_archive_filename)
            """
            return False

        try:
            self.client.download_file(
                self._aws_credentials.aws_bucket,
                remote_archive_filename,
                local_archive_filename
            )
            self._logger.info("Download remote %s to %s", remote_archive_filename, local_archive_filename)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                self._logger.info("Remote %s is not found", remote_archive_filename)
                return False
            else:
                raise
        else:
            untar_file(local_archive_filename, os.path.dirname(local_filename))
            self._logger.info("Untar archive %s to %s", local_archive_filename, os.path.dirname(local_filename))
            return await self.exists(name)

    async def dump(self, name: str):
        await self._delegate.dump(name)
        local_filename = self._delegate.make_filepath(name)
        local_archive_filename = self._make_local_archive_filepath(name)
        remote_archive_filename = self._make_remote_filepath(name)
        make_tarfile(local_archive_filename, local_filename)
        self.client.upload_file(local_archive_filename, self._aws_credentials.aws_bucket, remote_archive_filename)
        self._logger.info("Upload archive %s to %s %s",
                          local_archive_filename, self._aws_credentials.aws_bucket, remote_archive_filename)

    async def load(self, name: str):
        if not await self._delegate.exists(name):
            if await self.exists(name):
                await self._download(name)
        return await self._delegate.load(name)

    def make_filepath(self, name: str) -> str:
        return self._make_local_archive_filepath(name)

    def _make_local_archive_filepath(self, name: str) -> str:
        path = self._delegate.make_filepath(name)
        base = os.path.dirname(os.path.abspath(path))
        return os.path.join(base, "%s.remote.tar.gz" % name)

    def _make_remote_filepath(self, name: str) -> str:
        return os.path.basename(self.make_filepath(name))


def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


def untar_file(source_filename, destination_dir):
    with tarfile.open(source_filename, "r:gz") as f:
        f.extractall(path=destination_dir)
