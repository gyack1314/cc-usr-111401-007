# 资料与既有接口约定

table.py 的每次提交产生严格递增sequence及changes，每个change带row_id、before、after，null像表示该端不存在，单提交原子生效。snapshot(sequence)返回该提交结束后的行。复合索引键为(tool,occurred)，tool字符串可null，null先于字符串，occurred为整数；相同键按row_id区分，范围端点明确开闭。index.py 提供单版本不可变索引创建和范围读取，目前调用者必须显式给完整rows。

所有资料均为合成，字段、身份及状态按本文解释。源码中的 inspect 入口只读取并核对现有基础接口；不会建立高级查询或仓库管理流程。

在线二级索引在不暂停写入的前提下建立：提交日志与构建状态均内容寻址并由目录引用 CAS 管理，基线扫描、追平水位、覆盖证明与原子启用的完整约定见 docs/online-index.md。查询只有在索引被证明覆盖所读快照时才走索引，否则回退扫表；构建失败不影响读取。
