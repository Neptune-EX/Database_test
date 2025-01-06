#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */

/*

- `Begin(Transaction*, LogManager*)`：该接口提供事务的开始方法。

  **提示**：如果是新事务，需要创建一个`Transaction`对象，并把该对象的指针加入到全局事务表中。

- `Commit(Transaction*, LogManager*)`：该接口提供事务的提交方法。

  **提示**：如果并发控制算法需要申请和释放锁，那么你需要在提交阶段完成锁的释放。

- `Abort(Transaction*, LogManager*)`：该接口提供事务的终止方法。

  在事务的终止方法中，你需要对需要对事务的所有写操作进行撤销，事务的写操作都存储在Transaction类的write_set_中，因此，你可以通过修改存储层或执行层的相关代码来维护write_set_，并在终止阶段遍历write_set_，撤销所有的写操作。

  **提示**：需要对事务的所有写操作进行撤销，如果并发控制算法需要申请和释放锁，那么你需要在终止阶段完成锁的释放。

  **思考**：在回滚删除操作的时候，是否必须插入到record的原位置，如果没有插入到原位置，会存在哪些问题？

*/

Transaction *TransactionManager::begin(Transaction *txn, LogManager *log_manager)
{
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    if (!txn)
    {
        // txn_id_t id = get_transaction_id();
        Transaction *txn = new Transaction(next_txn_id_, IsolationLevel::SERIALIZABLE);
        next_txn_id_ += 1;
        txn->set_state(TransactionState::DEFAULT);
    }
    txn_map[txn->get_transaction_id()] = txn; // 3
    return txn;                               // 4
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction *txn, LogManager *log_manager)
{
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    auto wset = txn->get_write_set();
    while (!wset->empty()) // 1
        wset->pop_back();

    auto lset = txn->get_lock_set();
    for (auto i = lset->begin(); i != lset->end(); i++) // 2
        lock_manager_->unlock(txn, *i);
    lset->clear();

    txn->set_state(TransactionState::COMMITTED); // 4
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction *txn, LogManager *log_manager)
{
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态

    auto wset = txn->get_write_set();
    while (!wset->empty())
    { // 1
        Context *ctx = new Context(lock_manager_, log_manager, txn);
        if (wset->back()->GetWriteType() == WType::INSERT_TUPLE)
            sm_manager_->rollback_insert(wset->back()->GetTableName(), wset->back()->GetRid(), ctx);
        else if (wset->back()->GetWriteType() == WType::DELETE_TUPLE)
            sm_manager_->rollback_delete(wset->back()->GetTableName(), wset->back()->GetRecord(), ctx);
        else if (wset->back()->GetWriteType() == WType::UPDATE_TUPLE)
            sm_manager_->rollback_update(wset->back()->GetTableName(), wset->back()->GetRid(), wset->back()->GetRecord(), ctx);
        wset->pop_back();
    }

    auto lset = txn->get_lock_set();
    for (auto i = lset->begin(); i != lset->end(); i++) // 2
        lock_manager_->unlock(txn, *i);
    lset->clear();

    txn->set_state(TransactionState::ABORTED); // 4
}