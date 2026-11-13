"""持久化资料仓：提交日志、重启重建与写入冲突检查。"""
import tempfile, unittest
from pathlib import Path
from warehouse.repo import Repository, LOG_REF


class RepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'wh'
        self.repo = Repository.create(self.root, [
            {'row_id': 'a', 'tool': 'chat', 'occurred': 1},
            {'row_id': 'b', 'tool': 'code', 'occurred': 2},
        ])

    def test_commit_persists_and_rebuilds_after_reopen(self):
        self.repo.upsert({'row_id': 'c', 'tool': 'mail', 'occurred': 3})
        self.repo.delete('a')
        reopened = Repository.open(self.root)
        self.assertEqual(reopened.head_sequence(), 2)
        self.assertEqual({r['row_id'] for r in reopened.snapshot()}, {'b', 'c'})
        # 历史快照同样可重建
        self.assertEqual({r['row_id'] for r in reopened.snapshot(1)}, {'a', 'b', 'c'})

    def test_stale_before_image_rejected_and_writer_can_retry(self):
        stale = {'row_id': 'a', 'before': {'row_id': 'a', 'tool': 'chat', 'occurred': 99},
                 'after': {'row_id': 'a', 'tool': 'chat', 'occurred': 5}}
        with self.assertRaises(ValueError):
            self.repo.commit([stale])
        # 失败未推进日志
        self.assertEqual(self.repo.head_sequence(), 0)
        # 用 change() 自动补当前旧像即可成功
        self.repo.commit([self.repo.change('a', {'row_id': 'a', 'tool': 'chat', 'occurred': 5})])
        self.assertEqual(self.repo.head_sequence(), 1)

    def test_catalog_conflict_forces_reload(self):
        # 模拟外部进程推进目录引用：内存 CAS 失败并报错，reload 后对齐
        other = Repository.open(self.root)
        other.upsert({'row_id': 'z', 'tool': 'chat', 'occurred': 9})
        with self.assertRaises(ValueError):
            self.repo.upsert({'row_id': 'y', 'tool': 'chat', 'occurred': 8})
        self.repo.reload()
        self.assertEqual(self.repo.head_sequence(), 1)
        self.repo.upsert({'row_id': 'y', 'tool': 'chat', 'occurred': 8})

    def test_create_is_idempotent_guard(self):
        with self.assertRaises(ValueError):
            Repository.create(self.root, [])

    def test_delete_missing_rejected(self):
        with self.assertRaises(ValueError):
            self.repo.delete('ghost')

    def test_head_ref_present_in_catalog(self):
        self.assertEqual(self.repo.catalog.read()[LOG_REF], self.repo.head)


if __name__ == '__main__':
    unittest.main()
