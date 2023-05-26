概述

在第三个编程项目中，您将向数据库系统添加对查询执行的支持。您将实现执行器，负责获取查询计划节点并执行它们。您将创建执行以下操作的执行器：



访问方法：顺序扫描

修改：插入、更新、删除

其他：嵌套循环联接、哈希联接、聚合、限制、差异

因为DBMS还不支持SQL，所以您的实现将直接在手写的查询计划上操作。



我们将使用迭代器查询处理模型（即Volcano模型）。回想一下，在这个模型中，每个查询计划执行器都实现了Next函数。当DBMS调用执行器的Next函数时，执行器返回（1）单个元组或（2）不再有元组的指示符。通过这种方法，每个执行器都实现了一个循环，该循环继续对其子级调用Next，以检索元组并逐个处理它们。



在BusTub的迭代器模型实现中，每个执行器的Next函数除了返回一个元组之外，还返回一个记录标识符（RID）。记录标识符作为元组相对于其所属表的唯一标识符。

在关系数据库中，SQL语句将被转换为逻辑查询计划，并在进行查询优化后转化为物理查询计划，系统通过执行物理查询计划完成对应的语句功能。在本实验中，需要为`bustub`实现物理查询计划执行功能，包括顺序扫描、插入、删除、更改、连接、聚合以及`DISTINCT`和`LIMIT`。

在关系型数据库中，物理查询计划在系统内部被组织成树的形式，并通过特定的查询处理模型（迭代器模型、生产者模型）进行执行。在本实验中所要实现的模型为迭代器模型，如上图所示，该模型的每个查询计划节点通过`NEXT()`方法得到其所需的下一个元组，直至`NEXT()`方法返回假。在执行流中，根节点的`NEXT()`方法最先被调用，其控制流向下传播直至叶节点。

在`bustub`中，每个查询计划节点`AbstractPlanNode`都被包含在执行器类`AbstractExecutor`中，用户通过执行器类调用查询计划的`Next()`方法及初始化`Init()`方法，而查询计划节点中则保存该操作所需的特有信息，如顺序扫描需要在节点中保存其所要扫描的表标识符、连接需要在节点中保存其子节点及连接的谓词。同时。执行器类中也包含`ExecutorContext`上下文信息，其代表了查询计划的全局信息，如事务、事务管理器、锁管理器等。

# TASK #1 - EXECUTORS

`SeqScanExecutor`执行顺序扫描操作，其通过`Next()`方法顺序遍历其对应表中的所有元组，并将元组返回至调用者。

## seq_scan_plan.h

```c++
 private:

  const AbstractExpression *predicate_;

  table_oid_t table_oid_;
```

- predicate_为所有返回的元组必须满足的谓词
- table_oid_为应扫描其元组的表

在`bustub`中，所有与表有关的信息被包含在`TableInfo`中：

## TableInfo

```c++
struct TableInfo {
  /**
   * Construct a new TableInfo instance.
   * @param schema The table schema
   * @param name The table name
   * @param table An owning pointer to the table heap
   * @param oid The unique OID for the table
   */
  TableInfo(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
      : schema_{std::move(schema)}, name_{std::move(name)}, table_{std::move(table)}, oid_{oid} {}
  /** The table schema */
  Schema schema_;
  /** The table name */
  const std::string name_;
  /** An owning pointer to the table heap */
  std::unique_ptr<TableHeap> table_;
  /** The table OID */
  const table_oid_t oid_;
};
```

- schema_为表模式
- name_为表名
- table_为指向TableHeap的指针
- oid_为

表中的实际元组储存在`TableHeap`中，其包含用于插入、查找、更改、删除元组的所有函数接口，并可以通过`TableIterator`迭代器顺序遍历其中的元组。

## seq_scan_executor.h

```c++
  const SeqScanPlanNode *plan_;

  TableInfo *table_info_;

  TableIterator iter_;

  TableIterator end_;

  bool is_same_schema_;	//表模式与输出模式是否一致
```

在`SeqScanExecutor`中，为其增加`TableInfo`、及迭代器私有成员，用于访问表信息和遍历表。在`bustub`中，所有表都被保存在目录`Catalog`中，可以通过表标识符从中提取对应的`TableInfo`：

- plan_为要执行的顺序扫描计划节点
- table_info_为表的信息
- iter_为table_info的迭代器私有成员，用于访问表信息和遍历表

## seq_scan_executor.cpp

### SeqScanExecutor的构造函数

```c++
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr),  =//RID()  为给定的页面标识符和插槽号创建一个新的记录标识符
      end_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {

}
```

 * ExecutorContext存储运行执行器所需的所有上下文
 * SeqScanPlanNode表示一种顺序表扫描操作。它标识了一个要扫描的表和一个可选谓词。

### SchemaEequal()

SchemaEequal() 通过列名，偏移量判断模式是否相同

- 首先获取表输入输出列数组
- 比较数组大小，若不相等，则返回false
- 获得每列的偏移量与列名
- 若偏移量与列名不相等，则返回false

```c++
// 通过列名，偏移量判断模式是否相同
bool SeqScanExecutor::SchemaEqual(const Schema *table_schema, const Schema *output_schema) {
    auto table_colums = table_schema->GetColumns();     // 获得表输入列数组
    auto output_colums = output_schema->GetColumns();   // 获得表输出列数组
    if (table_colums.size() != output_colums.size()) {  // 如果两数组大小不一，则模式不相等
        return false;
    }

    int col_size = table_colums.size();             // 获得列大小
    uint32_t offset1;                               // 偏移量1
    uint32_t offset2;                               // 偏移量2
    std::string name1;                              // 姓名1
    std::string name2;                              // 姓名2
    for (int i = 0; i < col_size; i++) {            //遍历列
        offset1 = table_colums[i].GetOffset();
        offset2 = output_colums[i].GetOffset();
        name1 = table_colums[i].GetName();
        name2 = table_colums[i].GetName();
        if (name1 != name2 || offset1 != offset2) { //如果偏移量或列名有一处不相等
            return false;                           //返回false
        }
    }
    return true;
}
```

### Init()

在`Init()`中，执行计划节点所需的初始化操作

- 获取需要操作的表
- 获取迭代器
- 判断表输入/输出模式是否相同
- 如果隔离级别可重复读
- 遍历表，给每行元组上读锁

```c++
void SeqScanExecutor::Init() { 
    table_oid_t oid = plan_->GetTableOid();                             //从plan中获取oid
    table_info_ = exec_ctx_->GetCatalog()->GetTable(oid);               //通过索引表用oid获取对应表
    iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());    //获取起始迭代器
    end_ = table_info_->table_->End();                                  //获取结束迭代器

    auto output_schema = plan_->OutputSchema();                         //获取表输出模式
    auto table_schema = table_info_->schema_;                           //获取表输入模式
    is_same_schema_ = SchemaEqual(&table_schema, output_schema);        //判断是否相同

    // 可重复读：给所有元组加上读锁，事务提交后再解锁
    auto transaction = exec_ctx_->GetTransaction();                             //获得事务
    auto lockmanager = exec_ctx_->GetLockManager();                             //获得锁管理器
    if (transaction->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {  //如果隔离级别为可重复读
        auto iter = table_info_->table_->Begin(exec_ctx_->GetTransaction());    //遍历表，给每行元组都上读锁
        while (iter != table_info_->table_->End()) {
            lockmanager->LockShared(transaction, iter->GetRid());
            ++iter;
        }
    }
}
```

### TupleSchemaTranformUseEvaluate（）

TupleSchemaTranformUseEvaluate（）转变元组模式

```c++
void SeqScanExecutor::TupleSchemaTranformUseEvaluate(const Tuple *table_tuple, const Schema *table_schema,
                                                     Tuple *dest_tuple, const Schema *dest_schema) {
  auto colums = dest_schema->GetColumns();  //获取表的列数组
  std::vector<Value> dest_value;			//初始化目标值
  dest_value.reserve(colums.size());		

  for (const auto &col : colums) {			//传入值，根据列索引传入，筛选处不符合表模式的列
    dest_value.emplace_back(col.GetExpr()->Evaluate(table_tuple, table_schema));
  }
  *dest_tuple = Tuple(dest_value, dest_schema);	
}
```

### Next（）

在Next() 中，计划节点遍历表，并通过输入参数返回元组，当遍历结束时返回假

- 如果隔离级别为读已提交：读元组时加上读锁，读完后立即释放
- 遍历所有元组
- 经过谓词过滤检测元组是否仍存在，若不存在，迭代至下一个元组
- 如果存在，若表模式与输出模式不一致,改变元组数据结构
- 若一致则不进行处理

```c++
bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {

  //用于测试元组的谓词；只有当元组的计算结果为true时，才应返回元组
  const AbstractExpression *predicate = plan_->GetPredicate();  
  //返回此计划节点输出的架构
  const Schema *output_schema = plan_->OutputSchema();
  Schema table_schema = table_info_->schema_;
  auto exec_ctx = GetExecutorContext();
  Transaction *transaction = exec_ctx->GetTransaction();
  //TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();
  LockManager *lockmanager = exec_ctx->GetLockManager();
  bool res;
  
  while (iter_ != end_) {
      // 读已提交：读元组时加上读锁，读完后立即释放
    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lockmanager->LockShared(transaction, iter_->GetRid());
    }

    auto p_tuple = &(*iter_);  // 获取指向元组的指针；
    res = true;
    if (predicate != nullptr) {
      res = predicate->Evaluate(p_tuple, &table_schema).GetAs<bool>();  //如果有数据，返回true
    }

    if (res) {
      if (!is_same_schema_) {   //如果表模式与输出模式不一致,改变元组数据结构
        TupleSchemaTranformUseEvaluate(p_tuple, &table_schema, tuple, output_schema);
      } else {                  //如果一致，不进行处理
        *tuple = *p_tuple;
      }
      *rid = p_tuple->GetRid(); //返回行元组的ID
    }
    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lockmanager->Unlock(transaction, iter_->GetRid());
    }
    ++iter_;    // 指向下一位置后再返回
    if (res) {
      return true;
    }
  }
  return false;
}
```



在`InsertExecutor`中，其向特定的表中插入元组，元组的来源可能为其他计划节点或自定义的元组数组。其具体来源可通过`IsRawInsert()`提取。在构造函数中，提取其所要插入表的`TableInfo`，元组来源，以及与表中的所有索引：

## insert_executor.h

```c++
 private:
  /** The insert plan node to be executed*/
  const InsertPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> child_executor_;

  TableInfo *table_info_;

  bool is_raw_;

  uint32_t size_;

  std::vector<IndexInfo *> indexes_;
```

- is_raw_表示是否被加工过
- indexes_为索引信息数列

## insert_executor.cpp

### InsertExecutor()构造函数

```c++
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  table_oid_t oid = plan->TableOid();                   //获得插入元组的表的标识符
  table_info_ = exec_ctx->GetCatalog()->GetTable(oid);  //通过目录获取表信息
  is_raw_ = plan->IsRawInsert();                        //如果我们直接插入返回true, 如果我们有子计划返回false
  if (is_raw_) {
    size_ = plan->RawValues().size();                   //RawValues  要插入的原始值
  }
  indexes_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);   //获取由“table_name”标识的表的所有索引
}
```

### Init()

当元组来源为其他计划节点时，执行对应计划节点的Init(方法)

```c++
void InsertExecutor::Init() {
  if (!is_raw_) {               // 初始化子计划或者数组迭代器
    child_executor_->Init();    //子计划初始化
  }
}
```

### Next()

在Next() 中，根据不同的元组来源实施不同的插入策略

- 需要注意，Insert节点不应向外输出任何元组，所以其总是返回假，即所有的插入操作均应当在一次Next中被执行完成。
- 当来源为自定义的元组数组时，根据表模式构造对应的元组，并插入表中；
- 当来源为其他计划节点时，通过子节点获取所有元组并插入表。
- 在插入过程中，应当使用InsertEntry更新表中的所有索引，InsertEntry的参数应由KeyFromTuple方法构造。
- 更新索引日志

```c++
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto exec_ctx = GetExecutorContext();                             // 获得执行器
  Transaction *txn = exec_ctx_->GetTransaction();                   // 获得事务
  TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();  // 获得事务管理器
  LockManager *lock_mgr = exec_ctx->GetLockManager();               // 获得锁管理器

  Tuple tmp_tuple;  // 元组
  RID tmp_rid;      // 记录标识符，插入表后才被赋值

    if (is_raw_) {
    for (uint32_t idx = 0; idx < size_; idx++) {
        const std::vector<Value> &raw_value = plan_->RawValuesAt(idx);      // 要插入特定索引的原始值
        tmp_tuple = Tuple(raw_value, &table_info_->schema_);                // 自定义的元祖数组根据表模式构造对应的元组 
        if (table_info_->table_->InsertTuple(tmp_tuple, &tmp_rid, txn)) {   // 插入元组      此时tmp_rid才被赋值
            if (!lock_mgr->LockExclusive(txn, tmp_rid)) {                   // 如果没锁上写锁
            txn_mgr->Abort(txn);                                            // 否则事务夭折
            }                                       //更新索引列表
            for (auto indexinfo : indexes_) {       //遍历索引列表
                indexinfo->index_->InsertEntry(     //插入索引
                    tmp_tuple.KeyFromTuple(table_info_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()), //将自定义的元祖数组根据索引模式构造对应的数组  
                    tmp_rid, txn);
            IndexWriteRecord iwr(*rid, table_info_->oid_, WType::INSERT, *tuple, *tuple, indexinfo->index_oid_,  // 跟踪与写入相关的信息。
                                    exec_ctx->GetCatalog());
            txn->AppendIndexWriteRecord(iwr);  // 将索引写入记录添加到索引写入集中。
            }
        }
    }
    return false;
    }
    while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {  // 来源为其他计划节点
    if (table_info_->table_->InsertTuple(tmp_tuple, &tmp_rid, txn)) {
    if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {  // 如果事务隔离等级不为可重复读
        if (!lock_mgr->LockExclusive(txn, *rid)) {                      // 上写锁
        txn_mgr->Abort(txn);
        }
    } else {
        if (!lock_mgr->LockUpgrade(txn, *rid)) {                        // 写锁升级
            txn_mgr->Abort(txn);
        }
    }
    for (auto indexinfo : indexes_) {  // 更新索引
        indexinfo->index_->InsertEntry(
            tmp_tuple.KeyFromTuple(*child_executor_->GetOutputSchema(), indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()),
            tmp_rid, txn);
            txn->GetIndexWriteSet()->emplace_back(tmp_rid, table_info_->oid_, WType::INSERT, tmp_tuple, tmp_tuple,
                                                    indexinfo->index_oid_, exec_ctx->GetCatalog());
            }
        }
    }
    return false;
}
```

## UpdateExecutor.cpp

其实现方法与`InsertExecutor`相似，但其元组来源仅为其他计划节点

### updateExecutor()构造函数/Init()

```C++
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  table_oid_t oid = plan->TableOid();
  auto catalog = exec_ctx->GetCatalog();
  table_info_ = catalog->GetTable(oid);
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
}

void UpdateExecutor::Init() {
    child_executor_->Init(); 
}
```

### GenerateUpdatedTuple（）

UpdateExecutor::Next中，利用GenerateUpdatedTuple方法将源元组更新为新元组。

```c++
Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();            //获得更新属性,
  Schema schema = table_info_->schema_;                         //获得表模式
  uint32_t col_count = schema.GetColumnCount();                 //获得列数
  std::vector<Value> values;

  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {        //如果遍历结束，再元组的最后额外一列放入更新值备份
      values.emplace_back(src_tuple.GetValue(&schema, idx));    //获得src_tuple对应值添加到values中，即保持不变
    } else {
      const UpdateInfo info = update_attrs.at(idx);     //获取更新操作模式
      Value val = src_tuple.GetValue(&schema, idx);     //获取元组值
      switch (info.type_) {     //选择更新操作模式
        case UpdateType::Add:   //若更新操作为Add
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:   //若更新操作为Set
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}
```

### Next()

- 获得子节点的数据
- 调用GenerateUpdatedTuple（）生成更新元组
- 若事务隔离等级不为可重复读，上写锁或升级读锁
- 调用UpdateTuple(*tuple, *rid, txn)更新元组
- 遍历索引列表
- 对要更新的元组先删除再添加
- 记录索引日志

```c++
bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto exec_ctx = GetExecutorContext();                             //获得执行器
  Transaction *txn = exec_ctx_->GetTransaction();                   //获得事务
  TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();  //获得事务管理器
  LockManager *lock_mgr = exec_ctx->GetLockManager();               //获得锁管理器

  Tuple src_tuple;  
  while (child_executor_->Next(&src_tuple, rid)) {                      //来源为其他计划节点
    *tuple = this->GenerateUpdatedTuple(src_tuple);                     //生成更新元组
    if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {  //事务隔离等级不为可重复读
      if (!lock_mgr->LockExclusive(txn, *rid)) {                        //上写锁
        txn_mgr->Abort(txn);
      }
    } else {                        
      if (!lock_mgr->LockUpgrade(txn, *rid)) {                          //升级读锁
        txn_mgr->Abort(txn);
      }
    }
    if (table_info_->table_->UpdateTuple(*tuple, *rid, txn)) {          //更新元组，更新page
      for (auto indexinfo : indexes_) {     //更新索引，RID都为子执行器输出元组的RID
        indexinfo->index_->DeleteEntry(tuple->KeyFromTuple(*child_executor_->GetOutputSchema(), indexinfo->key_schema_,
                                                           indexinfo->index_->GetKeyAttrs()),
                                       *rid, txn);
        indexinfo->index_->InsertEntry(tuple->KeyFromTuple(*child_executor_->GetOutputSchema(), indexinfo->key_schema_,
                                                           indexinfo->index_->GetKeyAttrs()),
                                       *rid, txn);

        IndexWriteRecord iwr(*rid, table_info_->oid_, WType::UPDATE, *tuple, src_tuple, indexinfo->index_oid_,
                             exec_ctx->GetCatalog());
        txn->AppendIndexWriteRecord(iwr);
      }
    }
  }
  return false;
}
```

## delete_executor.cpp

其实现方法与`InsertExecutor`相似，但其元组来源仅为其他计划节点

### DeleteExecutor()构造函数/Init()

```c++
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  table_oid_t oid = plan->TableOid();
  auto catalog = exec_ctx->GetCatalog();
  table_info_ = catalog->GetTable(oid);
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
}

void DeleteExecutor::Init() { 
    child_executor_->Init(); 
}
```

### Next()

- 获得子节点的数据
- 若事务隔离等级不为可重复读，上写锁或升级读锁
- 调用MarkDelete(*rid, txn)删除元组
- 遍历索引列表
- 对要更新的元组进行删除
- 记录索引日志

```c++
bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto exec_ctx = GetExecutorContext();
  Transaction *txn = exec_ctx_->GetTransaction();
  TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();
  LockManager *lock_mgr = exec_ctx->GetLockManager();

  while (child_executor_->Next(tuple, rid)) {
    if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
      if (!lock_mgr->LockExclusive(txn, *rid)) {
        txn_mgr->Abort(txn);
      }
    } else {
      if (!lock_mgr->LockUpgrade(txn, *rid)) {
        txn_mgr->Abort(txn);
      }
    }
    if (table_info_->table_->MarkDelete(*rid, txn)) {   //删除元组
      for (auto indexinfo : indexes_) {                 //更新元组
        indexinfo->index_->DeleteEntry(tuple->KeyFromTuple(*child_executor_->GetOutputSchema(), indexinfo->key_schema_,
                                                           indexinfo->index_->GetKeyAttrs()),
                                       *rid, txn);
        IndexWriteRecord iwr(*rid, table_info_->oid_, WType::DELETE, *tuple, *tuple, indexinfo->index_oid_,
                             exec_ctx->GetCatalog());
        txn->AppendIndexWriteRecord(iwr);
      }
    }
  }
  return false;
}
```

## NestedLoopJoinExecutor.cpp

### NestedLoopJoinExecutor()构造函数

```C++
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(left_executor.release()),
      right_executor_(right_executor.release()) {}

```

### Init()

​	在这里，`Init()`函数完成所有的连接操作，并将得到的所有连接元组存放在缓冲区`buffer_`中。

- 其通过子计划节点的`Next()`方法得到子计划节点的元组

- 通过双层循环遍历每一对元组组合

- 当内层表遍历结束后，调用其`Init()`使其初始化，外层表进入下一元组，开始下一轮遍历。

- 在得到子计划节点元组后，如存在谓词，则调用谓词的`EvaluateJoin`验证其是否符合谓词。

- 如不存在谓词或符合谓词，则通过调用`out_schema`各`Column`的`EvaluateJoin`得到输出元组，并将其置入`buffer_`。

  ```c++
  void NestedLoopJoinExecutor::Init() {
    left_executor_->Init();
    right_executor_->Init();
    buffer_.clear();
    const Schema *left_schema = plan_->GetLeftPlan()->OutputSchema();     //左表输入模式
    const Schema *right_schema = plan_->GetRightPlan()->OutputSchema();   //右表输入模式
    const Schema *out_schema = this->GetOutputSchema();                   //表输出模式
    Tuple left_tuple;
    Tuple right_tuple;
    RID rid;
    while (left_executor_->Next(&left_tuple, &rid)) {     //左表加一
      right_executor_->Init();
      while (right_executor_->Next(&right_tuple, &rid)) { //右表遍历到末尾
        auto *predicate = plan_->Predicate();             //设置过滤谓词
        if (predicate == nullptr ||                       //如果没有谓词或过滤后仍存在数据
            predicate->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {    //评估加入
          std::vector<Value> values;
          for (const auto &col : out_schema->GetColumns()) {  //遍历表列，符合的值加入values列表中
            values.emplace_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema));
          }
          buffer_.emplace_back(values, out_schema);
        }
      }
    }
  }
  ```

### Next()

```c++
bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!buffer_.empty()) {
    *tuple = buffer_.back();
    buffer_.pop_back();
    return true;
  }
  return false;
}
```

## HashJoinExecutor.cpp

`HashJoinExecutor`使用基础哈希连接算法进行连接操作，其原理为将元组的连接键（即某些属性列的组合）作为哈希表的键，并使用其中一个子计划节点的元组构造哈希表。由于具有相同连接键的元组一定具有相同的哈希键值，因此另一个子计划节点中的元组仅需在该元组映射的桶中寻找可与其连接的元组

### HashJoin()构造函数

```c++
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_(left_child.release()), right_child_(right_child.release()) {}

```

### Init()

将左表元组的连接键（即某些属性列的组合）作为哈希表的键

```c++
void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  hash_map_.clear();
  output_buffer_.clear();
  Tuple left_tuple;
  const Schema *left_schema = left_child_->GetOutputSchema();
  RID rid;
  while (left_child_->Next(&left_tuple, &rid)) {
    HashJoinKey left_key;
    left_key.value_ = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_schema);       //设置左表的hashjoinkey
    hash_map_.emplace(left_key, left_tuple);    //加入hash_map_映射<<HashJoinKey, Tuple>>
  }
}
```

### Next()

- 通过右表元组连接键在左哈希表寻找对应键
- 通过谓词过滤获得有效值
- 放入结果中

```c++
bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!output_buffer_.empty()) {        //如果已存在结果，弹出结果，返回true
    *tuple = output_buffer_.back();
    output_buffer_.pop_back();
    return true;
  }
  Tuple right_tuple;
  const Schema *left_schema = left_child_->GetOutputSchema();
  const Schema *right_schema = right_child_->GetOutputSchema();
  const Schema *out_schema = GetOutputSchema();
  while (right_child_->Next(&right_tuple, rid)) {
    HashJoinKey right_key;
    right_key.value_ = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_schema);   //设置右表的hashjoinkey
    auto iter = hash_map_.find(right_key);                  //在映射表中寻找对应键
    uint32_t num = hash_map_.count(right_key);              //获取存在符合键值对的总数
    for (uint32_t i = 0; i < num; ++i, ++iter) {            //遍历符合键值对
      std::vector<Value> values;
      for (const auto &col : out_schema->GetColumns()) {    //通过谓词过滤获得有效值
        values.emplace_back(col.GetExpr()->EvaluateJoin(&iter->second, left_schema, &right_tuple, right_schema));   
      } 
      output_buffer_.emplace_back(values, out_schema);      //放入结果中
    }
    if (!output_buffer_.empty()) {      //如果已存在结果，弹出结果，返回true
      *tuple = output_buffer_.back();
      output_buffer_.pop_back();
      return true;
    }
  }
  return false;
}
```

## aggregation_executor.cpp

AggregationExecutor实现聚合操作，其原理为使用哈希表将所有聚合键相同的元组映射在一起，以此统计所有聚合键元组的聚合信息

### AggregationExecutor()构造函数

```c++
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(child.release()),
      hash_table_(plan->GetAggregates(), plan->GetAggregateTypes()),
      iter_(hash_table_.Begin()) {}

```

### Init()

在Init()中，

- 遍历子计划节点的元组，
- 构建哈希表及设置用于遍历该哈希表的迭代器。
- InsertCombine将当前聚合键的统计信息更新

```c++
void AggregationExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_->Init();
  while (child_->Next(&tuple, &rid)) {
    hash_table_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  iter_ = hash_table_.Begin();
}
```

### InsertCombine()

`InsertCombine`将当前聚合键的统计信息更新

```c++
  void InsertCombine(const AggregateKey &agg_key, const AggregateValue &agg_val) {
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }
    CombineAggregateValues(&ht_[agg_key], agg_val);
  }
```

### CombineAggregateValues

```c++
  void CombineAggregateValues(AggregateValue *result, const AggregateValue &input) {
    for (uint32_t i = 0; i < agg_exprs_.size(); i++) {
      switch (agg_types_[i]) {
        case AggregationType::CountAggregate:
          // Count increases by one.
          result->aggregates_[i] = result->aggregates_[i].Add(ValueFactory::GetIntegerValue(1));
          break;
        case AggregationType::SumAggregate:
          // Sum increases by addition.
          result->aggregates_[i] = result->aggregates_[i].Add(input.aggregates_[i]);
          break;
        case AggregationType::MinAggregate:
          // Min is just the min.
          result->aggregates_[i] = result->aggregates_[i].Min(input.aggregates_[i]);
          break;
        case AggregationType::MaxAggregate:
          // Max is just the max.
          result->aggregates_[i] = result->aggregates_[i].Max(input.aggregates_[i]);
          break;
      }
    }
  }
```



### Next()

- 使用迭代器遍历哈希表
- 如存在谓词，则使用谓词的EvaluateAggregate
- 判断当前聚合键是否符合谓词
- 如不符合则继续遍历直到寻找到符合谓词的聚合键。

```c++
bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != hash_table_.End()) {              //遍历哈希表
    auto *having = plan_->GetHaving();
    const auto &key = iter_.Key().group_bys_;
    const auto &val = iter_.Val().aggregates_;
    if (having == nullptr || having->EvaluateAggregate(key, val).GetAs<bool>()) {   //如果符合条件
      std::vector<Value> values;
      for (const auto &col : GetOutputSchema()->GetColumns()) {
        values.emplace_back(col.GetExpr()->EvaluateAggregate(key, val));    //放入结果
      }
      *tuple = Tuple(values, GetOutputSchema());    //设置结果元组
      ++iter_;
      return true;
    }
    ++iter_;    //如果谓词过滤后不符合条件，继续遍历
  }
  return false;
}
```

## LimitExecutor.cpp

- LimitExecutor用于限制输出元组的数量，其计划节点中定义了具体的限制数量。
- 其Init()应当调用子计划节点的Init()方法，并重置当前限制数量；
- Next()方法中若limit降至0，或子节点如法再提供的值（即子节点的值下于limit）
- 否则将子计划节点的元组返回，直至限制数量为0。

```c++

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  limit_ = plan_->GetLimit();
}

void LimitExecutor::Init() {
  child_executor_->Init();
  limit_ = plan_->GetLimit();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (limit_ == 0 || !child_executor_->Next(tuple, rid)) {	//若limit降至0，或子节点如法再提供的值（即子节点的值下于limit）
    return false;
  }
  --limit_;
  return true;
}
```

## distinct_executor.cpp

- `DistinctExecutor`用于去除相同的输入元组，并将不同的元组输出
- 在这里使用哈希表方法去重
- 在实际运行中，使用哈希表去重即
- `Init()`清空当前哈希表，并初始化子计划节点
- `Next()`判断当前元组是否已经出现在哈希表中
- 如是则遍历下一个输入元组，如非则将该元组插入哈希表并返回

```c++
DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {}

void DistinctExecutor::Init() {
  set_.clear();
  child_executor_->Init();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  while (child_executor_->Next(tuple, rid)) {
    auto key = MakeKey(tuple);
    if (set_.count(key) == 0U) {        //如果set_内不存在键值
      set_.insert(key);                 //插入
      return true;
    }
  }
  return false;
}
```

