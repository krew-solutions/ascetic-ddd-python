import os
import time
import logging
import typing
import datetime
import subprocess

from psycopg.conninfo import conninfo_to_dict
from ascetic_ddd.faker.infrastructure.dump.interfaces import IDump, IFileDump

__all__ = ("PgDump", "GzipDump", "FileDump",)


class FileDump(IFileDump):
    _base_path: str
    _ttl: datetime.timedelta
    _delegate: IDump

    def __init__(self, delegate: IDump, base_path: str, ttl: datetime.timedelta):
        self._base_path = base_path
        self._ttl = ttl
        self._delegate = delegate
        self._logger = logging.getLogger(__name__)

    @property
    def ttl(self) -> datetime.timedelta:
        return self._ttl

    async def exists(self, name: str) -> bool:
        filename = self.make_filepath(name)
        if os.path.exists(filename):
            mtime = os.path.getmtime(filename)
            delta = time.time() - mtime
            if delta < self._ttl.total_seconds():
                self._logger.info("Dump %s exists", filename)
                return True
        self._logger.info("Dump %s does not exist", filename)
        return False

    async def dump(self, name: str):
        if not os.path.exists(self._base_path):
            os.mkdir(self._base_path)
        filename = self.make_filepath(name)
        with open(filename, "wb") as file:
            self._logger.info("Make dump %s", filename)
            return await self._delegate.dump(file)

    async def load(self, name: str):
        filename = self.make_filepath(name)
        with open(filename, "rb") as file:
            self._logger.info("Load dump %s", filename)
            return await self._delegate.load(file)

    def make_filepath(self, name: str) -> str:
        return os.path.join(self._base_path, "%s.sql.gz" % name)


class GzipDump(IDump):
    _delegate: IDump

    def __init__(self, delegate: IDump):
        self._delegate = delegate

    async def dump(self, out: typing.IO[bytes]):
        proc = subprocess.Popen(
            ["gzip"], stdin=subprocess.PIPE, stdout=out, stderr=subprocess.PIPE
        )
        await self._delegate.dump(proc.stdin)

        sub_out, sub_err = proc.communicate()
        code = proc.returncode
        if code != 0:
            raise Exception("gzip failed for Postgres: %s, %s", code, sub_err)

    async def load(self, in_: typing.IO[bytes]):
        proc = subprocess.Popen(
            ["gunzip"], stdin=in_, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        await self._delegate.load(proc.stdout)

        sub_out, sub_err = proc.communicate()
        code = proc.returncode
        if code != 0:
            raise Exception("gunzip failed for Postgres: %s, %s", code, sub_err)


class PgDump(IDump):
    """
    https://github.com/IBM/backwork-backup-postgresql/blob/master/postgresql/postgresql.py
    """
    _credentials: dict

    def __init__(self, conninfo: str):
        self._credentials = conninfo_to_dict(conninfo)

    async def dump(self, out: typing.IO[bytes]):
        os.environ["PGPASSWORD"] = self._credentials['password']
        cmd = [
            "pg_dump",
            "-c",
            "-h", self._credentials['host'],
            "-p", self._credentials['port'],
            "-U", self._credentials['user'],
            "-d", self._credentials['dbname'],
            "--no-password",
            # "--inserts",
            "--no-owner",
            "--no-privileges",
        ]
        proc = subprocess.Popen(cmd, env={'PGPASSWORD': self._credentials['password']},
                                stdout=out, stderr=subprocess.PIPE)
        sub_out, sub_err = proc.communicate()
        code = proc.returncode
        if code != 0:
            raise Exception("pg_dump failed for Postgres: %s, %s", proc.returncode, sub_err)

    async def load(self, in_: typing.IO[bytes]):
        os.environ["PGPASSWORD"] = self._credentials['password']
        cmd = [
            "psql",
            "-h", self._credentials['host'],
            "-p", self._credentials['port'],
            "-U", self._credentials['user'],
            "-d", self._credentials['dbname'],
            "--no-password",
        ]
        proc = subprocess.Popen(cmd, env={'PGPASSWORD': self._credentials['password']},
                                stdin=in_, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sub_out, sub_err = proc.communicate()
        code = proc.returncode
        if code != 0:
            raise Exception("psql failed for Postgres: %s, %s", proc.returncode, sub_err)
