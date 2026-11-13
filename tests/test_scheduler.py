"""后台构建调度的在线性与关停语义检查。"""
import tempfile, time, unittest
from pathlib import Path
from warehouse.repo import Repository
from warehouse.online import OnlineBuilder
from warehouse.scheduler import BuildScheduler
from warehouse.planner import QueryPlan


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'wh'
        self.repo = Repository.create(self.root, [
            {'row_id': 'r%03d' % i, 'tool': 'chat', 'occurred': i}
            for i in range(40)])
        self.builder = OnlineBuilder(self.repo, page_size=8, scan_workers=2)
        self.plan = QueryPlan(self.repo, page_size=8)

    def test_background_build_converges_while_writes_continue(self):
        scheduler = BuildScheduler(self.builder, poll_seconds=0.01).start()
        self.addCleanup(scheduler.stop)
        # 构建期间持续写入，任何时刻查询都能得到与扫表一致的结果
        for i in range(12):
            self.repo.upsert({'row_id': 'new%02d' % i, 'tool': 'code', 'occurred': 200 + i})
            hits = self.plan.range('code', 200, 300)
            expect = sorted(
                (r for r in self.repo.snapshot()
                 if r['tool'] == 'code' and 200 <= r['occurred'] < 300),
                key=lambda r: r['row_id'])
            self.assertEqual(hits, expect)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            # 收敛判据是活动索引覆盖最新表头；构建线程可能正在跑下一轮
            if self.plan.open_reader().kind == 'index' and \
                    self.plan.coverage_report()['sequence'] == self.repo.head_sequence():
                break
            time.sleep(0.02)
        self.assertEqual(self.plan.open_reader().kind, 'index')
        self.assertEqual(self.plan.coverage_report()['sequence'],
                         self.repo.head_sequence())
        scheduler.stop()
        self.assertFalse(scheduler.is_running())

    def test_new_writes_trigger_rebuild_through_scheduler(self):
        scheduler = BuildScheduler(self.builder, poll_seconds=0.01).start()
        self.addCleanup(scheduler.stop)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and self.plan.open_reader().kind != 'index':
            time.sleep(0.02)
        first_seq = self.builder.status()['watermark']
        self.repo.upsert({'row_id': 'tail', 'tool': 'chat', 'occurred': 7})
        # 调度器自动重建：以活动索引重新覆盖最新表头为收敛判据
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            report = self.plan.coverage_report()
            if report.get('sequence', -1) > first_seq and \
                    report['sequence'] == self.repo.head_sequence():
                break
            time.sleep(0.02)
        self.assertEqual(self.plan.open_reader().kind, 'index')
        self.assertIn('tail', [r['row_id'] for r in self.plan.range('chat', 0, 10)])


if __name__ == '__main__':
    unittest.main()
