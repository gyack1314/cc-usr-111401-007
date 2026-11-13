"""在线二级索引构建的正确性、在线性与持久化检查。"""
import json, tempfile, unittest, random
from pathlib import Path
from warehouse.repo import Repository
from warehouse.online import (
    OnlineBuilder, BuildError,
    SCANNING, CATCHING_UP, READY, ACTIVE, FAILED, ABANDONED,
    STATE_REF, ACTIVE_REF,
)
from warehouse.planner import QueryPlan
from warehouse.index import sort_key

TOOLS = ['chat', 'code', 'mail', None]


def make_rows(n, seed=0):
    rng = random.Random(seed)
    return [{'row_id': 'r%04d' % i,
             'tool': rng.choice(TOOLS),
             'occurred': rng.randrange(0, 500)} for i in range(n)]


def scan_range(repo, tool, start, end, sequence=None):
    return sorted(
        (r for r in repo.snapshot(sequence)
         if r['tool'] == tool and start <= r['occurred'] < end),
        key=sort_key)


class OnlineBuildTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.repo = Repository.create(self.root / 'wh', make_rows(60, seed=1))
        self.builder = OnlineBuilder(self.repo, page_size=8, scan_workers=3)
        self.plan = QueryPlan(self.repo, page_size=8)

    def assert_bag_equals_scan(self, sequence=None):
        ranges = [(t, s, e) for t in TOOLS for s in (0, 100, 250) for e in (50, 300, 600)
                  if s < e]
        for tool, start, end in ranges:
            via_index = self.plan.range(tool, start, end, sequence=sequence)
            via_scan = scan_range(self.repo, tool, start, end, sequence)
            self.assertEqual(via_index, via_scan,
                             (tool, start, end, via_index, via_scan))

    # ---- 基本在线流程 ---------------------------------------------------
    def test_queries_scan_before_index_and_build_keeps_writes_running(self):
        self.assertEqual(self.plan.open_reader().kind, 'scan')
        state = self.builder.start()
        self.assertEqual(state['phase'], SCANNING)
        # 构建进行中继续写入，查询仍走旧路径且结果正确
        for i in range(5):
            self.repo.upsert({'row_id': 'new%02d' % i, 'tool': 'chat', 'occurred': 20 + i})
        self.assertEqual(self.plan.open_reader().kind, 'scan')
        state = self.builder.run_forever()
        self.assertEqual(state['phase'], ACTIVE)
        self.assertEqual(self.plan.open_reader().kind, 'index')
        self.assert_bag_equals_scan()

    def test_progress_watermark_and_proof_are_persisted(self):
        self.builder.start()
        state = self.builder.run_forever(max_rounds=1)
        on_disk = self.builder.status()
        self.assertEqual(on_disk['scan_cursor'], state['scan_cursor'])
        self.assertGreaterEqual(on_disk['baseline_sequence'], 0)
        final = self.builder.run_forever()
        self.assertEqual(final['phase'], ACTIVE)
        proof = final['coverage']
        self.assertEqual(proof['sequence'], self.repo.head_sequence())
        self.assertEqual(proof['index_head'], self.builder.active_index())
        # 活动引用与构建状态都在目录里
        refs = self.repo.catalog.read()
        self.assertIn(ACTIVE_REF, refs)
        self.assertIn(STATE_REF, refs)

    # ---- 边界：键与分区同时变更、扫描/追平交界删除 ----------------------
    def test_key_and_partition_change_plus_boundary_delete(self):
        state = self.builder.start()
        target_ids = [r['row_id'] for r in self.repo.snapshot(0)[:6]]
        # 扫描尚未完成时，对同一行先改 tool+occurred 再删除，最后再复活
        self.repo.upsert({'row_id': target_ids[0], 'tool': 'mail', 'occurred': 401})
        self.builder.scan_once(state)
        self.repo.delete(target_ids[0])
        self.repo.upsert({'row_id': target_ids[1], 'tool': None, 'occurred': 2})
        self.repo.upsert({'row_id': target_ids[0], 'tool': 'chat', 'occurred': 77})
        # 对已扫描行反复变更键与分区，验证无旧键残留、无重复
        for _ in range(4):
            self.repo.upsert({'row_id': target_ids[2],
                              'tool': random.choice(TOOLS), 'occurred': random.randrange(0, 500)})
        self.repo.delete(target_ids[3])
        self.builder.run_forever()
        self.assertEqual(self.plan.open_reader().kind, 'index')
        self.assert_bag_equals_scan()
        live = {r['row_id']: r for r in self.repo.snapshot()}
        indexed = {}
        head = self.builder.active_index()
        manifest = self.repo.store.json(head)
        for page in manifest['pages']:
            for row in self.repo.store.json(page):
                self.assertNotIn(row['row_id'], indexed, '同一行被重复索引')
                indexed[row['row_id']] = row
        self.assertEqual(set(indexed), set(live))
        self.assertNotIn(target_ids[3], indexed)
        self.assertEqual(indexed[target_ids[0]]['occurred'], 77)

    def test_delete_between_scan_and_catchup_is_not_missed(self):
        state = self.builder.start()
        victim = self.repo.snapshot(0)[0]['row_id']
        self.builder.scan_once(state)  # 基线已包含 victim
        self.repo.delete(victim)
        self.builder.run_forever()
        all_hits = self.plan.range(None, -10**9, 10**9) + self.plan.range(
            'chat', -10**9, 10**9) + self.plan.range('code', -10**9, 10**9) \
            + self.plan.range('mail', -10**9, 10**9)
        self.assertNotIn(victim, [r['row_id'] for r in all_hits])
        self.assert_bag_equals_scan()

    # ---- 重启续建与废弃 -------------------------------------------------
    def test_resume_mid_scan_after_restart(self):
        self.builder.start()
        self.builder.run_forever(max_rounds=1)
        # 重启：仅靠目录与对象仓重建仓库与构建器
        repo2 = Repository.open(self.root / 'wh')
        builder2 = OnlineBuilder(repo2, page_size=8, scan_workers=3)
        status = builder2.status()
        self.assertIn(status['phase'], (SCANNING, CATCHING_UP))
        repo2.upsert({'row_id': 'after_crash', 'tool': 'chat', 'occurred': 123})
        builder2.resume()
        plan2 = QueryPlan(repo2, page_size=8)
        self.assertEqual(plan2.open_reader().kind, 'index')
        ranges = [(t, s, e) for t in TOOLS for s, e in ((0, 500), (0, 50), (100, 200))]
        for tool, start, end in ranges:
            self.assertEqual(plan2.range(tool, start, end),
                             scan_range(repo2, tool, start, end))

    def test_failed_candidate_keeps_scan_working_and_can_resume_or_abandon(self):
        state = self.builder.start()
        self.builder.scan_once(state)
        self.repo.upsert({'row_id': 'moving', 'tool': 'chat', 'occurred': 88})
        # 注入真实故障：破坏已持久化的追平提交对象内容寻址关系不可行，
        # 这里改为破坏状态中的追平行袋，使 promote 的覆盖证明必然失败。
        state = self.builder._load_state()
        state['delta_rows'] = {'moving': {'row_id': 'moving', 'tool': 'chat',
                                          'occurred': 999999}}
        state['watermark'] = self.repo.head_sequence()
        state['tombstones'] = []
        self.builder._save(state)
        with self.assertRaises(BuildError):
            self.builder.resume()
        self.assertEqual(self.builder.status()['phase'], FAILED)
        # 构建失败不影响查询：继续扫表，结果仍与实时表一致
        self.assertEqual(self.plan.open_reader().kind, 'scan')
        self.assertEqual(self.plan.range('chat', 0, 500),
                         scan_range(self.repo, 'chat', 0, 500))
        # 续建：清空错误的追平产物，从基线水位重放，进度（基线分段）保留
        fixed = self.builder._load_state()
        fixed['watermark'] = fixed['baseline_sequence']
        fixed['applied_digests'] = []
        fixed['delta_rows'] = {}
        fixed['tombstones'] = []
        self.builder._save(fixed)
        self.builder.resume()
        self.assertEqual(self.builder.status()['phase'], ACTIVE)
        self.assertEqual(self.plan.open_reader().kind, 'index')
        self.assert_bag_equals_scan()

    def test_explicit_abandon_clears_candidate_but_keeps_active_index(self):
        self.builder.build_online()
        active = self.builder.active_index()
        self.builder.rebuild()
        self.builder.abandon('测试废弃')
        self.assertIsNone(self.builder.catalog.read().get(STATE_REF))
        # 已启用的索引仍在，查询不受候选废弃影响
        self.assertEqual(self.plan.open_reader().kind, 'index')
        self.assertEqual(self.builder.active_index(), active)

    # ---- 覆盖证明把关 ---------------------------------------------------
    def test_old_reader_never_uses_newer_index(self):
        self.builder.build_online()
        self.repo.upsert({'row_id': 'late', 'tool': 'chat', 'occurred': 5})
        self.builder.rebuild()
        # 索引只代表最新快照：旧序号读句柄必须扫表
        old = self.plan.open_reader(sequence=0)
        self.assertEqual(old.kind, 'scan')
        self.assertEqual(self.plan.range('chat', 0, 500, sequence=0),
                         scan_range(self.repo, 'chat', 0, 500, 0))
        # 落后的未来序号不构成读句柄：与 Table.snapshot 同样拒绝
        future = self.repo.head_sequence() + 1
        with self.assertRaises(ValueError):
            self.plan.open_reader(sequence=future)

    def test_index_file_existence_is_not_coverage(self):
        self.builder.build_online()
        # 活动引用被指向一个水位不符的索引对象：存在但不覆盖，必须回退
        bogus = self.repo.store.put_json({
            'format': 1, 'sequence': 9999, 'key': ['tool', 'occurred', 'row_id'],
            'page_size': 8, 'pages': [], 'anchors': [], 'rows': 0})
        refs = self.repo.catalog.read()
        self.repo.catalog.compare_update(ACTIVE_REF, refs[ACTIVE_REF], bogus)
        self.assertEqual(self.plan.open_reader().kind, 'scan')
        self.assertEqual(self.plan.range('chat', 0, 500),
                         scan_range(self.repo, 'chat', 0, 500))
        # 空追平队列也不能代替证明：损坏索引内容后同样回退
        self.repo.catalog.compare_update(ACTIVE_REF, bogus, refs[ACTIVE_REF])
        head = refs[ACTIVE_REF]
        manifest = self.repo.store.json(head)
        tampered = dict(manifest, rows=manifest['rows'] + 1)
        bad_head = self.repo.store.put_json(tampered)
        self.repo.catalog.compare_update(ACTIVE_REF, head, bad_head)
        self.assertEqual(self.plan.open_reader().kind, 'scan')

    # ---- 选择性访问 -----------------------------------------------------
    def test_index_path_really_reduces_access(self):
        self.builder.build_online()
        reader = self.plan.open_reader()
        reader.range('chat', 10, 20)
        manifest = self.repo.store.json(self.builder.active_index())
        self.assertLess(reader.pages_checked, len(manifest['pages']))
        # 新提交使活动索引水位落后：同一句柄位置回退扫表，访问量为全表行数
        self.repo.upsert({'row_id': 'fresh', 'tool': 'chat', 'occurred': 15})
        fallback = self.plan.open_reader()
        self.assertEqual(fallback.kind, 'scan')
        fallback.range('chat', 10, 20)
        self.assertEqual(fallback.rows_checked, len(self.repo.snapshot()))

    # ---- 随机压力：写入与构建全程交错 -----------------------------------
    def test_random_interleaved_writer_matches_scan(self):
        rng = random.Random(42)
        state = self.builder.start()
        live_ids = [r['row_id'] for r in self.repo.snapshot(0)]
        rounds = 0
        while state['phase'] != ACTIVE:
            for _ in range(rng.randrange(1, 4)):
                rid = rng.choice(live_ids + [('new%d' % rounds)])
                if rng.random() < 0.25:
                    if rid in live_ids:
                        self.repo.delete(rid)
                        live_ids.remove(rid)
                else:
                    row = {'row_id': rid, 'tool': rng.choice(TOOLS),
                           'occurred': rng.randrange(0, 500)}
                    self.repo.upsert(row)
                    if rid not in live_ids:
                        live_ids.append(rid)
            state = self.builder.run_forever(max_rounds=1)
            rounds += 1
            self.assertLess(rounds, 100)
        self.assertEqual(self.plan.open_reader().kind, 'index')
        self.assert_bag_equals_scan()

    def test_two_builders_older_candidate_cannot_replace_newer_index(self):
        # B 基于当前表头完成构建并启用
        for i in range(8):
            self.repo.upsert({'row_id': 'late%02d' % i, 'tool': 'chat', 'occurred': 100 + i})
        builder_b = OnlineBuilder(self.repo, page_size=8, scan_workers=2)
        builder_b.build_online()
        active_seq_b = self.repo.store.json(builder_b.active_index())['sequence']
        self.assertEqual(active_seq_b, self.repo.head_sequence())

        # 人为构造一个水位落后的“已证明”候选，模拟更早冻结基线的构建者
        stale_seq = 0
        stale_rows = self.repo.snapshot(stale_seq)
        from warehouse.paged import PagedIndex
        stale_head = PagedIndex(self.repo.store, page_size=8).build(stale_rows, stale_seq)
        # 直接写入候选状态（生产路径中这相当于另一个构建者冻结的早期基线）
        snapshot_digest = self.repo.store.put_json(stale_rows)
        ordered = sorted(stale_rows, key=lambda r: r['row_id'])
        from warehouse.paged import _sorted_rows
        segments = []
        for i in range(0, len(ordered), 8):
            chunk = ordered[i:i + 8]
            seg_digest = self.repo.store.put_json(_sorted_rows(chunk))
            segments.append({'digest': seg_digest, 'first': chunk[0]['row_id'],
                             'last': chunk[-1]['row_id'], 'rows': len(chunk)})
        stale_state = {
            'build_id': 'stale-candidate',
            'phase': CATCHING_UP,
            'baseline_sequence': stale_seq,
            'baseline_snapshot': snapshot_digest,
            'baseline_rows': len(stale_rows),
            'scan_cursor': ordered[-1]['row_id'] if ordered else '',
            'segments': segments,
            'watermark': stale_seq,
            'applied_digests': [],
            'delta_rows': {},
            'tombstones': [],
            'index_head': stale_head,
            'coverage': {'sequence': stale_seq, 'rows': len(stale_rows),
                         'index_head': stale_head, 'identity_digest': 'stale'},
            'error': None,
        }
        state_digest = self.repo.store.put_json(stale_state)
        current_ref = self.repo.catalog.read()[STATE_REF]
        self.repo.catalog.compare_update(STATE_REF, current_ref, state_digest)
        builder = OnlineBuilder(self.repo, page_size=8, scan_workers=2)
        state = builder._load_state()
        builder.promote(state)
        # 旧水位候选不得替换活动索引，自动退回追平阶段等待重放新提交
        active_head = self.plan.catalog.read()[ACTIVE_REF]
        self.assertEqual(self.repo.store.json(active_head)['sequence'], active_seq_b)
        self.assertEqual(state['phase'], CATCHING_UP)
        # 续建后可收敛到最新水位并正确启用，查询结果仍与扫表一致
        builder.resume()
        self.assertEqual(builder.status()['phase'], ACTIVE)
        self.assert_bag_equals_scan()

    def test_empty_baseline_build(self):
        repo = Repository.create(self.root / 'empty', [])
        builder = OnlineBuilder(repo, page_size=4)
        plan = QueryPlan(repo, page_size=4)
        builder.build_online()
        self.assertEqual(plan.open_reader().kind, 'index')
        repo.upsert({'row_id': 'x', 'tool': 'chat', 'occurred': 1})
        self.assertEqual(plan.open_reader().kind, 'scan')
        builder.rebuild()
        self.assertEqual([r['row_id'] for r in plan.range('chat', 0, 10)], ['x'])


if __name__ == '__main__':
    unittest.main()
