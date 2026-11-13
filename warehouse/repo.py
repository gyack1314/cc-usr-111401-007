"""资料仓持久化：提交日志内容寻址，目录引用比较更新，重启可重建。

写入路径与索引构建完全解耦：每次提交先经 Table 校验旧新像，再把提交对象
写入不可变对象仓，最后以 CAS 推进 table/head 引用。任何时刻写入都不会被
索引构建暂停。
"""
import threading
from pathlib import Path
from .objects import ObjectStore
from .catalog import Catalog
from .table import Table

LOG_REF = 'table/head'


class Repository:
    def __init__(self, root):
        self.root = Path(root)
        self.store = ObjectStore(self.root / 'objects')
        self.catalog = Catalog(self.root / 'catalog')
        self._lock = threading.RLock()
        manifest = self.catalog.read().get(LOG_REF)
        if manifest is None:
            raise ValueError('资料仓未初始化')
        self._load(manifest)

    # ---- 生命周期 -------------------------------------------------------
    @classmethod
    def create(cls, root, initial):
        repo = object.__new__(cls)
        repo.root = Path(root)
        repo.store = ObjectStore(Path(root) / 'objects')
        repo.catalog = Catalog(Path(root) / 'catalog')
        repo._lock = threading.RLock()
        initial_digest = repo.store.put_json(list(initial))
        head = repo.store.put_json({'format': 1, 'initial': initial_digest, 'commits': []})
        # 已存在 table/head 时拒绝覆盖，避免静默丢库
        repo.catalog.compare_update(LOG_REF, None, head)
        repo._load(head)
        return repo

    @classmethod
    def open(cls, root):
        return cls(root)

    def _load(self, head):
        self.head = head
        manifest = self.store.json(head)
        if manifest['format'] != 1:
            raise ValueError('提交日志格式不支持')
        initial = self.store.json(manifest['initial'])
        self.table = Table(initial)
        for commit_digest in manifest['commits']:
            self.table.append(self.store.json(commit_digest))

    def reload(self):
        """重启/外部进程推进后，从目录引用重建内存表。"""
        with self._lock:
            head = self.catalog.read().get(LOG_REF)
            if head is None:
                raise ValueError('资料仓引用丢失')
            self._load(head)
        return self

    # ---- 写入 -----------------------------------------------------------
    def commit(self, changes):
        """提交一组带 before/after 的变更，返回落盘后的提交序号。"""
        commit = {'sequence': self.table.commits.__len__() + 1, 'changes': list(changes)}
        with self._lock:
            expected = self.head
            manifest = self.store.json(expected)
            # Table.append 负责旧像、身份、序号的全部校验
            self.table.append(commit)
            commit_digest = self.store.put_json(commit)
            new_manifest = self.store.put_json({
                'format': 1,
                'initial': manifest['initial'],
                'commits': manifest['commits'] + [commit_digest],
            })
            try:
                self.catalog.compare_update(LOG_REF, expected, new_manifest)
            except ValueError:
                # 目录被外部推进：内存表与目录脱节，强制重建后由调用方重试
                self.reload()
                raise
            self.head = new_manifest
            return commit['sequence']

    def change(self, row_id, after):
        """按当前状态构造一条变更（after=None 表示删除），自动补旧像。"""
        with self._lock:
            current = {r['row_id']: r for r in self.table.snapshot(self.head_sequence())}
            before = current.get(row_id)
            if after is not None and after.get('row_id') != row_id:
                raise ValueError('新像身份不符')
            if after is None and before is None:
                raise ValueError('删除目标不存在')
            return {'row_id': row_id, 'before': before, 'after': after}

    def upsert(self, row):
        return self.commit([self.change(row['row_id'], row)])

    def delete(self, row_id):
        return self.commit([self.change(row_id, None)])

    # ---- 读取 -----------------------------------------------------------
    def head_sequence(self):
        with self._lock:
            return len(self.table.commits)

    def snapshot(self, sequence=None):
        with self._lock:
            if sequence is None:
                sequence = self.head_sequence()
            return self.table.snapshot(sequence)

    def commit_object(self, sequence):
        manifest = self.store.json(self.head)
        return self.store.json(manifest['commits'][sequence - 1])
