# 资料与既有接口约定

table.py 的每次提交产生严格递增sequence及changes，每个change带row_id、before、after，null像表示该端不存在，单提交原子生效。snapshot(sequence)返回该提交结束后的行。复合索引键为(tool,occurred)，tool字符串可null，null先于字符串，occurred为整数；相同键按row_id区分，范围端点明确开闭。index.py 提供单版本不可变索引创建和范围读取，目前调用者必须显式给完整rows。

所有资料均为合成，字段、身份及状态按本文解释。源码中的 inspect 入口只读取并核对现有基础接口；不会建立高级查询或仓库管理流程。
