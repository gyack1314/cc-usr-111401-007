"""完整行集上的不可变二级索引基础接口。"""
from .objects import encode

def sort_key(row):
    tool=row['tool'];return (0 if tool is None else 1,'' if tool is None else tool,row['occurred'],row['row_id'])

class Index:
    def __init__(self,store):self.store=store
    def build(self,rows,sequence):
        if len({r['row_id'] for r in rows})!=len(rows):raise ValueError('行身份重复')
        return self.store.put_json({'sequence':sequence,'rows':sorted(rows,key=sort_key)})
    def range(self,head,tool,start,end):
        m=self.store.json(head)
        return [r for r in m['rows'] if r['tool']==tool and start<=r['occurred']<end]

from .table import Table

def inspect(data,store,root):
    table=Table(data['initial'])
    for batch in data['commits']:table.append(batch)
    index=Index(store);head=index.build(table.snapshot(0),0)
    return {'initial_index':head,'initial_matches':index.range(head,'chat',0,100),'head_rows':table.snapshot(len(table.commits)),'commits':len(table.commits)}
