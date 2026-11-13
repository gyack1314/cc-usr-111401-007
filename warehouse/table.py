"""完整行表的提交旧新像与版本读取。"""
class Table:
    def __init__(self,initial):self.initial={r['row_id']:r for r in initial};self.commits=[]
    def append(self,commit):
        if commit['sequence']!=len(self.commits)+1:raise ValueError('提交序列不连续')
        state={r['row_id']:r for r in self.snapshot(len(self.commits))};seen=set()
        for c in commit['changes']:
            if c['row_id'] in seen:raise ValueError('提交内身份重复')
            seen.add(c['row_id'])
            if state.get(c['row_id'])!=c['before']:raise ValueError('提交旧像不符')
            if c['after'] is not None and c['after']['row_id']!=c['row_id']:raise ValueError('新像身份不符')
        self.commits.append(commit)
    def snapshot(self,sequence):
        if not 0<=sequence<=len(self.commits):raise ValueError('快照位置不存在')
        state=dict(self.initial)
        for batch in self.commits[:sequence]:
            for c in batch['changes']:
                if c['after'] is None:state.pop(c['row_id'],None)
                else:state[c['row_id']]=c['after']
        return list(state.values())

