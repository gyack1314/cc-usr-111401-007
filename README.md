# 仓库在线二级索引资料工具

Python 3.11 项目，用于读取、保存及核对协作资料仓的本地资料。不可变内容通过 SHA256 寻址，目录引用单独保存。现有领域接口位于 warehouse/index.py，资料契约见 docs/contracts.md，合成输入位于 data/source.json。warehouse/objects.py 负责内容校验，catalog.py 负责本地引用更新，snapshot.py 提供完整行快照基础读取。

已有读取流程使用临时目录，不改变源资料。当前只提供源码中已实现的基础操作，输出相应对象身份、行或引用摘要。领域接口的测试说明其现有边界，不承诺额外的管理或查询功能。

在线二级索引由 `repo.py`（内容寻址提交日志）、`paged.py`（分页不可变索引）、`online.py`（基线扫描/追平/覆盖证明/原子启用状态机）、`planner.py`（覆盖证明驱动的查询选路）和 `scheduler.py`（后台守护调度）组成，设计约定见 docs/online-index.md。

运行与开发检查：

```sh
python -m unittest discover -s tests -v
python -m compileall -q warehouse tests
python -m warehouse
```

在线索引的最小用法：

```python
from warehouse.repo import Repository
from warehouse.online import OnlineBuilder
from warehouse.scheduler import BuildScheduler
from warehouse.planner import QueryPlan

repo = Repository.create(root, initial_rows)
builder = OnlineBuilder(repo, page_size=64, scan_workers=2)
BuildScheduler(builder).start()      # 后台构建，写入与查询不暂停
plan = QueryPlan(repo)
plan.range('chat', start, end)       # 证明覆盖则走索引，否则扫表
builder.rebuild()                    # 表头推进后开启下一轮构建
```
