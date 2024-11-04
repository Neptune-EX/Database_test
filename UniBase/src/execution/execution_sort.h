#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    ColMeta cols_;                              // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    size_t tuple_num;
    bool is_desc_;
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc) {
        prev_ = std::move(prev);
        cols_ = prev_->get_col_offset(sel_cols);
        is_desc_ = is_desc;
        tuple_num = 0;
        used_tuple.clear();
    }

    void beginTuple() override { 
        
    }

    void nextTuple() override {
        
    }

    std::unique_ptr<RmRecord> Next() override {
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};