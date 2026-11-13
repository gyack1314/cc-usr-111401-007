"""本地目录引用的比较更新及持久写入。"""
from pathlib import Path
import os, json, tempfile, fcntl
from contextlib import contextmanager
from .objects import encode

class Catalog:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.file = self.root/'refs.json'

    def read(self):
        return json.loads(self.file.read_bytes()) if self.file.exists() else {}

    @contextmanager
    def locked(self):
        # 阻塞式 flock：进程内多线程与跨进程都互斥；进程退出由内核自动释放，
        # 不会像 O_EXCL 锁文件那样残留导致后续操作永久失败。
        lock = self.root/'.refs.lock'
        with open(lock, 'w') as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    def compare_update(self, name, expected, value):
        with self.locked():
            refs = self.read()
            if refs.get(name) != expected:
                raise ValueError('目录引用已改变')
            refs[name] = value
            fd, path = tempfile.mkstemp(dir=self.root, prefix='.refs-')
            try:
                with os.fdopen(fd, 'wb') as stream:
                    stream.write(encode(refs)); stream.flush(); os.fsync(stream.fileno())
                os.replace(path, self.file)
            finally:
                if os.path.exists(path): os.unlink(path)
        return value
