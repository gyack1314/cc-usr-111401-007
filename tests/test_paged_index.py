"""分页索引的边界、排序与访问选择性检查。"""
import tempfile, unittest
from pathlib import Path
from warehouse.objects import ObjectStore
from warehouse.paged import PagedIndex
from warehouse.index import sort_key


def rows(identities, tool='chat', occurred=0):
    return [{'row_id': i, 'tool': tool, 'occurred': occurred + n}
            for n, i in enumerate(identities)]


class PagedIndexTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = ObjectStore(Path(self.tmp.name) / 'objects')

    def test_range_endpoints_and_paging(self):
        data = rows(['r%02d' % i for i in range(20)])
        head = PagedIndex(self.store, page_size=4).build(data, 0)
        manifest = self.store.json(head)
        self.assertEqual(len(manifest['pages']), 5)
        got = PagedIndex(self.store, page_size=4).range(head, 'chat', 3, 7)
        expect = [r for r in data if 3 <= r['occurred'] < 7]
        self.assertEqual([r['row_id'] for r in got], [r['row_id'] for r in expect])

    def test_selective_range_touches_only_matching_pages(self):
        data = rows(['r%02d' % i for i in range(100)])
        index = PagedIndex(self.store, page_size=10)
        head = index.build(data, 0)
        index.range(head, 'chat', 0, 10)  # 只命中第一页
        self.assertLessEqual(index.fetched_pages, 2)
        wide = PagedIndex(self.store, page_size=10)
        wide.range(head, 'chat', 0, 100)
        self.assertEqual(wide.fetched_pages, 10)

    def test_null_tool_sorts_first_and_filters(self):
        data = [
            {'row_id': 'n1', 'tool': None, 'occurred': 5},
            {'row_id': 's1', 'tool': 'chat', 'occurred': 1},
            {'row_id': 's2', 'tool': 'chat', 'occurred': 9},
        ]
        head = PagedIndex(self.store, page_size=2).build(data, 0)
        none_hits = PagedIndex(self.store, page_size=2).range(head, None, 0, 10)
        self.assertEqual([r['row_id'] for r in none_hits], ['n1'])
        chat_hits = PagedIndex(self.store, page_size=2).range(head, 'chat', 0, 10)
        self.assertEqual([r['row_id'] for r in chat_hits], ['s1', 's2'])
        # 锚点排序约束：null 段先于字符串段
        self.assertLess(sort_key(data[0]), sort_key(data[1]))

    def test_empty_and_out_of_range(self):
        head = PagedIndex(self.store).build(rows(['a', 'b']), 0)
        self.assertEqual(PagedIndex(self.store).range(head, 'missing', 0, 10), [])
        self.assertEqual(PagedIndex(self.store).range(head, 'chat', 1000, 2000), [])

    def test_duplicate_identity_rejected(self):
        with self.assertRaises(ValueError):
            PagedIndex(self.store).build(rows(['a', 'a']), 0)


if __name__ == '__main__':
    unittest.main()
