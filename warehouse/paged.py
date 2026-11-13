"""分页不可变二级索引。

索引由清单与若干有序页组成，页全部内容寻址、可单独读取。范围查询先用
清单里的页首键二分定位入口页，再顺序读到超出上界为止——选择性范围只触达
命中页而非整份索引。fetched_pages 计数用于在测试中证明访问确实减少。
"""
from bisect import bisect_right
from .index import sort_key


def _sorted_rows(rows):
    return sorted(rows, key=sort_key)


class PagedIndex:
    def __init__(self, store, page_size=64):
        self.store = store
        self.page_size = max(1, page_size)
        self.fetched_pages = 0

    # ---- 构建 -----------------------------------------------------------
    def build(self, rows, sequence):
        rows = _sorted_rows(rows)
        identities = [r['row_id'] for r in rows]
        if len(set(identities)) != len(identities):
            raise ValueError('行身份重复')
        pages, anchors = [], []
        for start in range(0, len(rows), self.page_size):
            chunk = rows[start:start + self.page_size]
            pages.append(self.store.put_json(chunk))
            anchors.append(sort_key(chunk[0]))
        return self.store.put_json({
            'format': 1,
            'sequence': sequence,
            'key': ['tool', 'occurred', 'row_id'],
            'page_size': self.page_size,
            'pages': pages,
            'anchors': anchors,
            'rows': len(rows),
        })

    # ---- 读取 -----------------------------------------------------------
    def manifest(self, head):
        m = self.store.json(head)
        if m['format'] != 1:
            raise ValueError('索引格式不支持')
        return m

    def page(self, manifest, index):
        self.fetched_pages += 1
        return self.store.json(manifest['pages'][index])

    def range(self, head, tool, start, end):
        """返回 tool 相等且 start <= occurred < end 的行。"""
        m = self.manifest(head)
        # JSON 往返后锚点由 tuple 变为 list，比较前归一化
        anchors = [tuple(a) for a in m['anchors']]
        first_key = sort_key({'row_id': '', 'tool': tool, 'occurred': start})
        # 入口页 = 最后一个首锚不大于目标键的页（bisect_right 再退一页）
        entry = max(0, bisect_right(anchors, first_key) - 1)
        hits = []
        for index in range(entry, len(m['pages'])):
            page = self.page(m, index)
            for row in page:
                if row['tool'] != tool:
                    continue
                if row['occurred'] < start:
                    continue
                if row['occurred'] >= end:
                    # 同 tool 内按 occurred、row_id 有序，越过右端点即结束
                    return hits
                hits.append(row)
        return hits
