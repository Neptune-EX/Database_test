#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "execution_defs.h"
#include "record/rm.h"
#include "system/sm.h"
#include "common/context.h"
#include "common/common.h"
#include "optimizer/plan.h"
#include "executor_abstract.h"
#include "transaction/transaction_manager.h"


class QlManager {
   private:
    SmManager *sm_manager_;
    TransactionManager *txn_mgr_;

   public:
    QlManager(SmManager *sm_manager, TransactionManager *txn_mgr) 
        : sm_manager_(sm_manager),  txn_mgr_(txn_mgr) {}

    void run_mutli_query(std::shared_ptr<Plan> plan, Context *context);
    void run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context);
    void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                        Context *context);
    void insert_into(const std::string &tab_name, std::vector<Value> values, Context *context);
    void delete_from(const std::string &tab_name, std::vector<Condition> conds, Context *context);
    void QlManager::update_set(const std::string &tab_name, std::vector<SetClause> set_clauses, std::vector<Condition> conds, Context *context);

    

    void run_dml(std::unique_ptr<AbstractExecutor> exec);
};
