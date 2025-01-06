#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
#define GroupMode(id) lock_table_[id].group_lock_mode_
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_}; // 1

    if (txn->get_isolation_level() == IsolationLevel::READ_UNCOMMITTED || // 2
        txn->get_state() == TransactionState::SHRINKING)
        txn->set_state(TransactionState::ABORTED);
    if (txn->get_state() == TransactionState::ABORTED)
        return false;

    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, rid, LockDataType::RECORD);

    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    { // 3
        for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++)
            if (i->txn_id_ == txn->get_transaction_id())
                lock_table_[newid].cv_.notify_all();
        lock.unlock();
        return true;
    }
    txn->get_lock_set()->insert(newid); // 4.1 put into lock_table
    LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_table_[newid].request_queue_.push_back(*newquest);
    lock_table_[newid].shared_lock_num_++;

    while (GroupMode(newid) != GroupLockMode::S &&
           GroupMode(newid) != GroupLockMode::IS &&
           GroupMode(newid) != GroupLockMode::NON_LOCK) // 4.2&5 group mode judgement
        lock_table_[newid].cv_.wait(lock);
    newquest->granted_ = true;
    lock_table_[newid].cv_.notify_all();
    lock.unlock();
    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_}; // 1

    if (txn->get_state() == TransactionState::SHRINKING) // 2
        txn->set_state(TransactionState::ABORTED);
    if (txn->get_state() == TransactionState::ABORTED)
        return false;

    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, rid, LockDataType::RECORD);

    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    { // 3
        for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++)
            if (i->txn_id_ == txn->get_transaction_id())
            {
                GroupMode(newid) = GroupLockMode::X;
                lock_table_[newid].cv_.notify_all();
            }
        lock.unlock();
        return true;
    }
    txn->get_lock_set()->insert(newid); // 4.1 put into lock_table
    LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_table_[newid].request_queue_.push_back(*newquest);
    lock_table_[newid].shared_lock_num_++;

    while (GroupMode(newid) != GroupLockMode::NON_LOCK) // 4.2&5 group mode judgement
        lock_table_[newid].cv_.wait(lock);
    newquest->granted_ = true;
    GroupMode(newid) = GroupLockMode::X;
    lock_table_[newid].cv_.notify_all();
    lock.unlock();
    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_};

    if (txn->get_isolation_level() == IsolationLevel::READ_UNCOMMITTED ||
        txn->get_state() == TransactionState::SHRINKING)
        txn->set_state(TransactionState::ABORTED);
    if (txn->get_state() == TransactionState::ABORTED)
        return false;

    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, LockDataType::TABLE);

    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    {
        for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++)
            if (i->txn_id_ == txn->get_transaction_id())
            {
                if (GroupMode(newid) == GroupLockMode::IX)
                    GroupMode(newid) = GroupLockMode::SIX;
                else if (GroupMode(newid) == GroupLockMode::IS || GroupMode(newid) == GroupLockMode::NON_LOCK)
                    GroupMode(newid) = GroupLockMode::S;
                lock_table_[newid].cv_.notify_all();
            }
        lock.unlock();
        return true;
    }
    txn->get_lock_set()->insert(newid);
    LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_table_[newid].request_queue_.push_back(*newquest);
    lock_table_[newid].shared_lock_num_++;

    while (GroupMode(newid) != GroupLockMode::S &&
           GroupMode(newid) != GroupLockMode::IS &&
           GroupMode(newid) != GroupLockMode::NON_LOCK)
        lock_table_[newid].cv_.wait(lock);
    newquest->granted_ = true;
    if (GroupMode(newid) == GroupLockMode::IX)
        GroupMode(newid) = GroupLockMode::SIX;
    else if (GroupMode(newid) == GroupLockMode::IS || GroupMode(newid) == GroupLockMode::NON_LOCK)
        GroupMode(newid) = GroupLockMode::S;
    lock_table_[newid].cv_.notify_all();
    lock.unlock();
    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_}; // 1

    if (txn->get_state() == TransactionState::SHRINKING) // 2
        txn->set_state(TransactionState::ABORTED);
    if (txn->get_state() == TransactionState::ABORTED)
        return false;

    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, LockDataType::TABLE);

    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    { // 3
        for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++)
            if (i->txn_id_ == txn->get_transaction_id())
            {
                GroupMode(newid) = GroupLockMode::X;
                lock_table_[newid].cv_.notify_all();
            }
        lock.unlock();
        return true;
    }
    txn->get_lock_set()->insert(newid); // 4.1 put into lock_table
    LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_table_[newid].request_queue_.push_back(*newquest);
    lock_table_[newid].shared_lock_num_++;

    while (GroupMode(newid) != GroupLockMode::NON_LOCK) // 4.2&5 group mode judgement
        lock_table_[newid].cv_.wait(lock);
    newquest->granted_ = true;
    GroupMode(newid) = GroupLockMode::X;
    lock_table_[newid].cv_.notify_all();
    lock.unlock();
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_}; // 1

    if (txn->get_isolation_level() == IsolationLevel::READ_UNCOMMITTED || // 2
        txn->get_state() == TransactionState::SHRINKING)
        txn->set_state(TransactionState::ABORTED);
    if (txn->get_state() == TransactionState::ABORTED)
        return false;

    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, LockDataType::TABLE);

    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    { // 3
        for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++)
            if (i->txn_id_ == txn->get_transaction_id())
            {
                if (GroupMode(newid) == GroupLockMode::NON_LOCK)
                    GroupMode(newid) = GroupLockMode::IS;
                lock_table_[newid].cv_.notify_all();
            }
        lock.unlock();
        return true;
    }
    txn->get_lock_set()->insert(newid); // 4.1 put into lock_table
    LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_table_[newid].request_queue_.push_back(*newquest);
    lock_table_[newid].shared_lock_num_++;

    while (GroupMode(newid) == GroupLockMode::X) // 4.2&5 group mode judgement
        lock_table_[newid].cv_.wait(lock);
    newquest->granted_ = true;
    if (GroupMode(newid) == GroupLockMode::NON_LOCK)
        GroupMode(newid) = GroupLockMode::IS;
    lock_table_[newid].cv_.notify_all();
    lock.unlock();
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_}; // 1

    if (txn->get_state() == TransactionState::SHRINKING) // 2
        txn->set_state(TransactionState::ABORTED);
    if (txn->get_state() == TransactionState::ABORTED)
        return false;

    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, LockDataType::TABLE);

    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    { // 3
        for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++)
            if (i->txn_id_ == txn->get_transaction_id())
            {
                if (GroupMode(newid) == GroupLockMode::S)
                    GroupMode(newid) = GroupLockMode::SIX;
                else if (GroupMode(newid) == GroupLockMode::IS || GroupMode(newid) == GroupLockMode::NON_LOCK)
                    GroupMode(newid) = GroupLockMode::IX;
                lock_table_[newid].cv_.notify_all();
            }
        lock.unlock();
        return true;
    }
    txn->get_lock_set()->insert(newid); // 4.1 put into lock_table
    LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_table_[newid].request_queue_.push_back(*newquest);
    lock_table_[newid].shared_lock_num_++;

    while (GroupMode(newid) == GroupLockMode::X &&
           GroupMode(newid) == GroupLockMode::S &&
           GroupMode(newid) == GroupLockMode::SIX) // 4.2&5 group mode judgement
        lock_table_[newid].cv_.wait(lock);
    newquest->granted_ = true;
    if (GroupMode(newid) == GroupLockMode::S)
        GroupMode(newid) = GroupLockMode::SIX;
    else if (GroupMode(newid) == GroupLockMode::IS || GroupMode(newid) == GroupLockMode::NON_LOCK)
        GroupMode(newid) = GroupLockMode::IX;
    lock_table_[newid].cv_.notify_all();
    lock.unlock();
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock(latch_);
    txn->set_state(TransactionState::SHRINKING);
    if (txn->get_lock_set()->find(lock_data_id) != txn->get_lock_set()->end())
    {
        GroupLockMode mode = GroupLockMode::NON_LOCK;
        for (auto i = lock_table_[lock_data_id].request_queue_.begin(); i != lock_table_[lock_data_id].request_queue_.end(); i++)
        {
            if (i->granted_)
            {
                if (i->lock_mode_ == LockMode::EXLUCSIVE)
                    mode = GroupLockMode::X;
                else if (i->lock_mode_ == LockMode::SHARED && mode != GroupLockMode::SIX)
                    mode = mode == GroupLockMode::IX ? GroupLockMode::SIX : GroupLockMode::S;
                else if (i->lock_mode_ == LockMode::S_IX)
                    mode = GroupLockMode::SIX;
                else if (i->lock_mode_ == LockMode::INTENTION_EXCLUSIVE && mode != GroupLockMode::SIX)
                    mode = mode == GroupLockMode::S ? GroupLockMode::SIX : GroupLockMode::IX;
                else if (i->lock_mode_ == LockMode::INTENTION_SHARED)
                    mode = mode == GroupLockMode::NON_LOCK ? GroupLockMode::IS : mode == GroupLockMode::IS ? GroupLockMode::IS
                                                                                                           : mode;
            }
        }
        lock_table_[lock_data_id].group_lock_mode_ = mode;
        lock_table_[lock_data_id].cv_.notify_all();
        lock.unlock();
        return true;
    }
    else
    {
        lock.unlock();
        return false;
    }
    }