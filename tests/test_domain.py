"""领域资料及现有接口核对。"""
import json,tempfile,unittest
from pathlib import Path
from warehouse.objects import ObjectStore
from warehouse.index import inspect

class DomainTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.store=ObjectStore(self.root/'objects')
        self.data=json.loads(Path('data/source.json').read_text(encoding='utf-8'))
    def test_existing_reader(self):
        result=inspect(self.data,self.store,self.root)
        self.assertTrue(result)

    def test_snapshot_and_old_image(self):
        from warehouse.index import Table,Index
        t=Table(self.data['initial']);i=Index(self.store);head=i.build(t.snapshot(0),0)
        for c in self.data['commits']:t.append(c)
        self.assertEqual([r['row_id'] for r in i.range(head,'chat',0,20)],['a'])
        self.assertEqual({r['row_id'] for r in t.snapshot(2)},{'b','c'})
        with self.assertRaises(ValueError):t.append({'sequence':3,'changes':[{'row_id':'a','before':{'row_id':'a'},'after':None}]})
