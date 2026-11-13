"""查询计划：只有证明覆盖所读快照的索引才能被使用。

计划器不相信“索引文件存在”或“构建队列已空”，每次为读句柄选路前都重新
核对活动索引的水位与覆盖证明：
- 索引水位晚于读快照（旧读句柄）或早于读快照（索引落后）都回退扫表；
- 覆盖证明无法重放（索引对象损坏、行袋与实时表不符）回退扫表；
- 构建失败、无候选、无活动索引一律扫表，功能永不依赖索引。
"""
from .paged import PagedIndex
from .online import ACTIVE_REF
from .index import sort_key


class QueryPlan:
    def __init__(self, repo, page_size=64):
        self.repo = repo
        self.store = repo.store
        self.catalog = repo.catalog
        self.page_size = page_size
        # 重放证明的结果按 (索引, 水位) 缓存；表头推进后键自然失效
        self._proof_cache = {}
        self.scan_rows_checked = 0

    # ---- 覆盖证明重放 ---------------------------------------------------
    def _index_covers(self, index_head, sequence):
        if index_head is None:
            return False
        manifest = self.store.json(index_head)
        if manifest.get('format') != 1 or manifest.get('sequence') != sequence:
            # 水位不符：旧读句柄不能套用只代表最新状态的索引，反之亦然
            return False
        cache_key = (index_head, sequence)
        cached = self._proof_cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            indexed = {}
            for page_digest in manifest['pages']:
                for row in self.store.json(page_digest):
                    if row['row_id'] in indexed:
                        raise ValueError('索引行身份重复')
                    indexed[row['row_id']] = row
            if len(indexed) != manifest['rows']:
                raise ValueError('索引行数不符')
            expected = {r['row_id']: r for r in self.repo.snapshot(sequence)}
            if indexed != expected:
                raise ValueError('索引行袋与快照不一致')
        except (KeyError, ValueError):
            self._proof_cache[cache_key] = False
            return False
        self._proof_cache[cache_key] = True
        return True

    def coverage_report(self):
        """供运维/测试查看当前活动索引及其已证明水位。"""
        index_head = self.catalog.read().get(ACTIVE_REF)
        if index_head is None:
            return {'index': None, 'provable_sequences': []}
        sequence = self.store.json(index_head)['sequence']
        return {
            'index': index_head,
            'sequence': sequence,
            'provable_sequences': [sequence] if self._index_covers(index_head, sequence) else [],
        }

    # ---- 选路与查询 -----------------------------------------------------
    def open_reader(self, sequence=None):
        if sequence is None:
            sequence = self.repo.head_sequence()
        if not 0 <= sequence <= self.repo.head_sequence():
            raise ValueError('快照位置不存在')
        index_head = self.catalog.read().get(ACTIVE_REF)
        if self._index_covers(index_head, sequence):
            return _IndexReader(self, index_head, sequence)
        return _ScanReader(self, sequence)

    def range(self, tool, start, end, sequence=None):
        return self.open_reader(sequence).range(tool, start, end)


class _ScanReader:
    """旧路径：整表快照后过滤。访问量即全表行数。"""

    kind = 'scan'

    def __init__(self, plan, sequence):
        self.plan = plan
        self.sequence = sequence
        self.rows_checked = 0

    def range(self, tool, start, end):
        hits = []
        for row in self.plan.repo.snapshot(self.sequence):
            self.rows_checked += 1
            self.plan.scan_rows_checked += 1
            if row['tool'] == tool and start <= row['occurred'] < end:
                hits.append(row)
        return sorted(hits, key=sort_key)


class _IndexReader:
    """索引路径：只读取命中页。pages_checked 证明选择性访问减少。"""

    kind = 'index'

    def __init__(self, plan, index_head, sequence):
        self.plan = plan
        self.index_head = index_head
        self.sequence = sequence
        self.pages_checked = 0

    def range(self, tool, start, end):
        paged = PagedIndex(self.plan.store, page_size=self.plan.page_size)
        hits = paged.range(self.index_head, tool, start, end)
        self.pages_checked = paged.fetched_pages
        return hits
