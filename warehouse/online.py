"""在线二级索引构建：基线扫描、追平、覆盖证明与原子启用。

构建期间写入与查询都不暂停，查询始终保留扫表旧路径。整个生命周期是一个
显式持久化的状态机（build/state 引用，内容寻址），重启后按状态续建或
废弃候选：

    scanning    基线快照分页扫描（多 worker，互斥不相交的行身份区间）
    catching_up 按提交序号单车道追平，水位 watermark 单调推进；覆盖证明与
                原子启用也从这里发起，READY 仅是 promote 内的内存瞬态、不落盘
    active      已 CAS 切换为 build/active，构建结束
    failed      构建失败，候选保留但永不参与查询，可续建或显式废弃
    abandoned   显式废弃，引用由新一轮构建原子替换

正确性要点：
- 基线快照在构建开始时冻结（内容寻址），扫描期间的新提交全部进入追平阶段；
- 追平只处理 baseline_sequence 之后的提交，每个提交恰好应用一次（水位+序号
  双重判定），因此扫描/追平交界处的更新与删除不会漏、不会重、不会留旧键；
- 索引键与分区（tool）同时改变时，用整行覆盖、删除按 row_id 清理，不依赖
  旧键反查；
- “索引文件存在”和“追平队列暂时为空”都不构成覆盖，promote 必须重放证明。
"""
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from .paged import PagedIndex, _sorted_rows

STATE_REF = 'build/state'
ACTIVE_REF = 'build/active'

SCANNING = 'scanning'
CATCHING_UP = 'catching_up'
READY = 'ready'
ACTIVE = 'active'
FAILED = 'failed'
ABANDONED = 'abandoned'


class BuildError(RuntimeError):
    pass


class OnlineBuilder:
    def __init__(self, repo, page_size=64, scan_workers=2):
        self.repo = repo
        self.store = repo.store
        self.catalog = repo.catalog
        self.page_size = page_size
        self.scan_workers = max(1, scan_workers)
        self._lock = threading.RLock()
        self._snapshot_cache = {}

    # ---- 状态持久化 -----------------------------------------------------
    def _read_state_ref(self):
        return self.catalog.read().get(STATE_REF)

    def _load_state(self):
        ref = self._read_state_ref()
        if ref is None:
            return None
        state = self.store.json(ref)
        # _prev_ref 是本地 CAS 游标，不参与状态对象的内容寻址
        state['_prev_ref'] = ref
        return state

    def _save(self, state):
        expected = state.pop('_prev_ref', None)
        digest = self.store.put_json(state)
        self.catalog.compare_update(STATE_REF, expected, digest)
        state['_prev_ref'] = digest
        return state

    def status(self):
        state = self._load_state()
        if state is None:
            return None
        return {k: v for k, v in state.items() if not k.startswith('_')}

    # ---- 启动 -----------------------------------------------------------
    def start(self, replace=None):
        """开始一次新构建；已有候选时必须先续建或废弃。

        replace 给出被替换候选的当前引用时，原子换入新状态，不留引用空窗，
        供 rebuild 在后台调度中安全使用。
        """
        with self._lock:
            current = self._read_state_ref()
            if replace is None:
                if current is not None:
                    raise BuildError('已有构建候选，先 resume 或 abandon')
            elif current != replace:
                raise BuildError('被替换候选引用已改变')
            baseline = self.repo.head_sequence()
            # 冻结基线：完整快照内容寻址，扫描读到的永远是这个版本。
            # 冻结后即使表头继续推进也无妨：新提交全部落在水位之后，由追平
            # 阶段处理，绝不把落后快照当成最终索引。
            snapshot_digest = self.store.put_json(self.repo.snapshot(baseline))
            rows = self.store.json(snapshot_digest)
            state = {
                'build_id': uuid.uuid4().hex,
                'phase': SCANNING,
                'baseline_sequence': baseline,
                'baseline_snapshot': snapshot_digest,
                'baseline_rows': len(rows),
                'scan_cursor': '',          # 已扫描的最大 row_id（字典序）
                'segments': [],             # 基线分页段：{digest, first, last, rows}
                'watermark': baseline,      # 已追平到的提交序号
                'applied_digests': [],      # 已应用提交的内容寻址清单（重启可重放）
                'delta_rows': {},           # 追平后的行覆盖：row_id -> 行对象
                'tombstones': [],           # 追平删除的 row_id
                'index_head': None,         # ready/active 后的分页索引清单
                'coverage': None,           # 覆盖证明
                'error': None,
            }
            if replace is None:
                return self._save(state)
            digest = self.store.put_json(state)
            self.catalog.compare_update(STATE_REF, replace, digest)
            state['_prev_ref'] = digest
            return state

    def resume(self):
        """重启后续建：按持久化阶段继续；候选已终结则原样返回。

        FAILED 候选的进度仍然有效（所有中间产物内容寻址、幂等），清错后
        按已完成的扫描进度退回对应阶段重试；active/abandoned 不可续建。
        """
        with self._lock:
            state = self._load_state()
            if state is None:
                raise BuildError('没有可续建的候选')
            if state['phase'] in (ACTIVE, ABANDONED):
                return state
            if state['phase'] == FAILED:
                scanned = sum(s['rows'] for s in state['segments'])
                state['phase'] = CATCHING_UP if scanned >= state['baseline_rows'] else SCANNING
                state['error'] = None
                self._save(state)
            return self.run_forever(state)

    def abandon(self, reason='显式废弃'):
        """明确废弃候选：落盘终态后清除构建引用，活动索引不受影响。"""
        with self._lock:
            state = self._load_state()
            if state is None:
                return
            state['phase'] = ABANDONED
            state['error'] = reason
            self._save(state)
            self.catalog.compare_update(STATE_REF, state['_prev_ref'], None)

    # ---- 基线扫描 -------------------------------------------------------
    def _scan_slice(self, snapshot_digest, begin, end):
        """扫描基线快照中第 [begin, end) 个按 row_id 排序的分片。

        区间由调度方一次性切分，worker 之间互斥不相交；分片对象内容寻址，
        同一分片即使被重复执行也得到同一 digest，天然幂等。
        """
        ordered = self._snapshot_cache.get(snapshot_digest)
        if ordered is None:
            ordered = sorted(self.store.json(snapshot_digest), key=lambda r: r['row_id'])
            self._snapshot_cache[snapshot_digest] = ordered
        chunk = ordered[begin:end]
        if not chunk:
            return None
        digest = self.store.put_json(_sorted_rows(chunk))
        return {
            'digest': digest,
            'first': chunk[0]['row_id'],
            'last': chunk[-1]['row_id'],
            'rows': len(chunk),
        }

    def scan_once(self, state, chunk_limit=None):
        """推进一轮受控并发的基线扫描，返回是否扫描完成。"""
        chunk_limit = chunk_limit or self.page_size
        if state['phase'] != SCANNING:
            return state['phase'] != SCANNING
        total = state['baseline_rows']
        scanned = sum(s['rows'] for s in state['segments'])
        # 先按不相交的下标区间规划本轮分片（游标快照，避免 worker 抢区间）
        jobs = []
        offset = scanned
        for _ in range(self.scan_workers):
            if offset >= total:
                break
            jobs.append((offset, min(total, offset + chunk_limit)))
            offset = jobs[-1][1]
        if not jobs:
            state['phase'] = CATCHING_UP
            self._save(state)
            return True
        if len(jobs) == 1:
            produced = [self._scan_slice(state['baseline_snapshot'], *jobs[0])]
        else:
            with ThreadPoolExecutor(max_workers=self.scan_workers) as pool:
                produced = list(pool.map(
                    lambda job: self._scan_slice(state['baseline_snapshot'], *job), jobs))
        for segment in produced:
            if segment is not None:
                state['segments'].append(segment)
        state['scan_cursor'] = state['segments'][-1]['last']
        scanned = sum(s['rows'] for s in state['segments'])
        done = scanned >= total
        if done:
            state['phase'] = CATCHING_UP
        self._save(state)
        return done

    # ---- 追平 -----------------------------------------------------------
    def _apply_commit(self, state, sequence, commit):
        if commit['sequence'] != sequence:
            raise BuildError('提交序号错位，追平中止')
        delta = state['delta_rows']
        tombstones = set(state['tombstones'])
        for change in commit['changes']:
            row_id = change['row_id']
            after = change['after']
            # 整行覆盖即可同时处理“键不变改值”和“键/分区同时改变”，
            # 不依赖旧键反查，杜绝旧键残留；删除按 row_id 清理。
            delta.pop(row_id, None)
            tombstones.discard(row_id)
            if after is None:
                tombstones.add(row_id)
            else:
                delta[row_id] = after
        state['tombstones'] = sorted(tombstones)
        state['applied_digests'].append(self.store.put_json(commit))
        state['watermark'] = sequence

    def catchup_once(self, state):
        """单车道追平一轮：严格按序号应用基线之后的所有已提交变更。"""
        if state['phase'] not in (CATCHING_UP, READY):
            return False
        state['phase'] = CATCHING_UP
        head = self.repo.head_sequence()
        for sequence in range(state['watermark'] + 1, head + 1):
            commit = self.repo.commit_object(sequence)
            self._apply_commit(state, sequence, commit)
        self._save(state)
        return state['watermark'] >= head

    # ---- 合并与覆盖证明 -------------------------------------------------
    def _merged_rows(self, state):
        # 基线各段按 row_id 有序归并
        segments = sorted(state['segments'], key=lambda s: s['first'])
        baseline = {}
        for segment in segments:
            for row in self.store.json(segment['digest']):
                baseline[row['row_id']] = row
        # 追平覆盖与删除最后生效（删除可能发生在扫描与追平交界）
        baseline.update(state['delta_rows'])
        for row_id in state['tombstones']:
            baseline.pop(row_id, None)
        return list(baseline.values())

    def _coverage_proof(self, state, rows, index_head, target_sequence):
        """覆盖证明：索引必须与 target_sequence 的真实快照逐行一致。

        存在性（文件在）和队列为空都不是证明，这里对行袋、身份唯一性、
        索引水位逐项核对，证明随状态落盘，作为切换与查询计划的唯一依据。
        """
        expected = self.repo.snapshot(target_sequence)
        expected_bag = {r['row_id']: r for r in expected}
        actual_bag = {r['row_id']: r for r in rows}
        if actual_bag != expected_bag:
            raise BuildError('覆盖证明失败：索引行袋与快照不一致')
        manifest = self.store.json(index_head)
        if manifest['sequence'] != target_sequence:
            raise BuildError('覆盖证明失败：索引水位与快照序号不符')
        if manifest['rows'] != len(rows):
            raise BuildError('覆盖证明失败：索引行数不符')
        ordered = _sorted_rows(rows)
        if [r['row_id'] for r in ordered] != [r['row_id'] for r in
                                              self._read_index_rows(manifest)]:
            raise BuildError('覆盖证明失败：索引排序内容不符')
        return {
            'sequence': target_sequence,
            'rows': len(rows),
            'index_head': index_head,
            'identity_digest': self.store.put_json(
                [expected_bag[k] for k in sorted(expected_bag)]),
        }

    def _read_index_rows(self, manifest):
        rows = []
        for page_digest in manifest['pages']:
            rows.extend(self.store.json(page_digest))
        return rows

    def promote(self, state):
        """按候选已追平水位构建分页索引、重放覆盖证明并尝试原子启用。

        证明只针对候选自己的目标水位；能否换入活动引用再与当前活动索引
        比较水位，旧水位候选不会抢占新索引。READY 只是本方法内的瞬态，
        不持久化：外部观察到的状态只有 catching_up 与 active，不会读到
        半切换状态。
        """
        if state['phase'] not in (CATCHING_UP, READY):
            return state
        target = state['watermark']
        rows = self._merged_rows(state)
        paged = PagedIndex(self.store, page_size=self.page_size)
        index_head = paged.build(rows, target)
        proof = self._coverage_proof(state, rows, index_head, target)
        # 原子切换：CAS 活动引用。切换与追平之间若又有新提交，证明水位会
        # 落后于表头，下一轮 run 继续追平并重新证明，绝不暂停写入。
        refs = self.catalog.read()
        active = refs.get(ACTIVE_REF)
        # 只允许更高（或相等的同水位）换入，杜绝旧水位索引覆盖新索引
        active_seq = self.store.json(active)['sequence'] if active else -1
        if target < active_seq:
            # 本候选证明有效但水位落后：留在追平阶段重放新提交后重新证明
            state['phase'] = CATCHING_UP
            state['error'] = '水位 %d 落后于活动索引 %d，继续追平' % (target, active_seq)
            self._save(state)
            return state
        try:
            self.catalog.compare_update(ACTIVE_REF, active, index_head)
        except ValueError:
            # 活动引用被并发构建推进：回到追平阶段，按最新表头重新证明
            state['phase'] = CATCHING_UP
            self._save(state)
            return state
        # CAS 成功后才一次性持久化 active 状态与覆盖证明
        state['index_head'] = index_head
        state['coverage'] = proof
        state['error'] = None
        state['phase'] = ACTIVE
        self._save(state)
        return state

    # ---- 驱动 -----------------------------------------------------------
    def run_forever(self, state=None, max_rounds=None):
        """按阶段驱动构建直到 active；返回终态。max_rounds 供测试限速。"""
        with self._lock:
            if state is None:
                state = self._load_state()
            rounds = 0
            while state['phase'] != ACTIVE:
                if max_rounds is not None and rounds >= max_rounds:
                    break
                rounds += 1
                try:
                    if state['phase'] == SCANNING:
                        # 单轮只调度一批受控并发分片，未完成则继续下一轮
                        # （max_rounds 用尽时由循环条件自然停下，不会空转）
                        self.scan_once(state)
                    elif state['phase'] == CATCHING_UP:
                        self.catchup_once(state)
                        if state['watermark'] == self.repo.head_sequence():
                            # promote 成功则进入 ACTIVE；水位落后或 CAS 冲突
                            # 会自行退回 CATCHING_UP，下一轮继续追平重证
                            self.promote(state)
                        else:
                            self._save(state)
                    elif state['phase'] == READY:
                        # 防御：READY 不持久化，仅在内存态出现时退回追平
                        state['phase'] = CATCHING_UP
                        self._save(state)
                    else:
                        raise BuildError('候选处于终结状态 %s，无法继续驱动' % state['phase'])
                except Exception as exc:  # 构建失败：候选保留，查询继续扫表
                    state['phase'] = FAILED
                    state['error'] = f'{type(exc).__name__}: {exc}'
                    self._save(state)
                    raise
            return state

    def build_online(self, max_rounds=None):
        state = self._load_state()
        if state is None:
            state = self.start()
        return self.run_forever(state, max_rounds=max_rounds)

    def rebuild(self, max_rounds=None):
        """表头推进导致活动索引落后时开启新一轮构建。

        旧候选（含已 active 的历史构建）转为 abandoned 并让出构建引用，
        活动索引保持可用，直到新候选通过证明后原子替换；新基线直接取最新
        快照，不再重放历史。
        """
        with self._lock:
            state = self._load_state()
            if state is not None and state['phase'] not in (ACTIVE, READY, ABANDONED):
                raise BuildError('当前候选尚未终结，不能重建')
            replaced = self._read_state_ref()
            if state is not None:
                # 旧候选先落盘为 abandoned，再由 start 原子换入新状态，
                # 引用始终非空，后台调度不会读到空窗
                state['phase'] = ABANDONED
                state['error'] = '被新一轮构建替换'
                self._save(state)
                replaced = state['_prev_ref']
            new_state = self.start(replace=replaced)
            return self.run_forever(new_state, max_rounds=max_rounds)

    # ---- 活动索引 -------------------------------------------------------
    def active_index(self):
        return self.catalog.read().get(ACTIVE_REF)
