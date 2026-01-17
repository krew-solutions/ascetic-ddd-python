import functools
import os
import time
import shlex
import shutil
import logging
import datetime
import subprocess

from psycopg.conninfo import conninfo_to_dict
from ascetic_ddd.faker.infrastructure.dump.interfaces import IDump, IFileDump

__all__ = ("SinglePgDump",)


class SinglePgDump(IFileDump):
    """
    https://github.com/IBM/backwork-backup-postgresql/blob/master/postgresql/postgresql.py
    """
    _credentials: dict
    _base_path: str
    _ttl: datetime.timedelta

    def __init__(self, base_path: str, ttl: datetime.timedelta, conninfo: str):
        self._base_path = base_path
        self._ttl = ttl
        self._logger = logging.getLogger(__name__)
        self._credentials = conninfo_to_dict(conninfo)

    @functools.cached_property
    def _cpu_count(self) -> int:
        cpu_count = len(os.sched_getaffinity(0))  # os.process_cpu_count() from 3.13v
        cpu_count = min(max(2, cpu_count), 99)  # max_connections
        return cpu_count

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
        if os.path.exists(filename):
            shutil.rmtree(filename)
        os.environ["PGPASSWORD"] = self._credentials['password']
        cmd = " ".join([
            shlex.join([
                "pg_dump",
                "-c",
                "-Fd",
                "-j", str(self._cpu_count),
                "-h", self._credentials['host'],
                "-p", self._credentials['port'],
                "-U", self._credentials['user'],
                "-d", self._credentials['dbname'],
                "--no-password",
                # "--inserts",
                "--no-owner",
                "--no-privileges",
                "-Z", "9",
                "-f", shlex.quote(filename),
            ])
        ])
        proc = subprocess.Popen(cmd, shell=True, env={'PGPASSWORD': self._credentials['password']},
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sub_out, sub_err = proc.communicate()
        code = proc.returncode
        if code != 0:
            raise Exception("pg_dump failed: %s, %s", proc.returncode, sub_err)

    async def load(self, name: str):
        os.environ["PGPASSWORD"] = self._credentials['password']
        """
        cmd = " ".join([
            shlex.join(["gunzip", "-c", shlex.quote(self._make_filename(name)),]),
            "|",
            shlex.join([
                "psql",
                "-h", self._credentials['host'],
                "-p", self._credentials['port'],
                "-U", self._credentials['user'],
                "-d", self._credentials['dbname'],
                "--no-password",
            ])
        ])
        """
        cmd = shlex.join([
            "pg_restore",
            "-c",
            "-j", str(self._cpu_count),
            "-h", self._credentials['host'],
            "-p", self._credentials['port'],
            "-U", self._credentials['user'],
            "-d", self._credentials['dbname'],
            "--no-password",
            "--no-owner",
            "--no-privileges",
            shlex.quote(self.make_filepath(name))
        ])
        proc = subprocess.Popen(cmd, shell=True, env={'PGPASSWORD': self._credentials['password']},
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sub_out, sub_err = proc.communicate()
        code = proc.returncode
        if code != 0:
            raise Exception("pg_restore failed: %s, %s", proc.returncode, sub_err)

    def make_filepath(self, name: str) -> str:
        return os.path.join(self._base_path, "%s.sql.gz" % name)
