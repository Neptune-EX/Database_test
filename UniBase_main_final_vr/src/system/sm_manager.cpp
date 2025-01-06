#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string &db_name)
{
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string &db_name)
{
    if (is_dir(db_name))
    {
        throw DatabaseExistsError(db_name);
    }
    // 为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0)
    { // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0)
    { // 进入名为db_name的目录
        throw UnixError();
    }
    // 创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db; // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0)
    {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string &db_name)
{
    if (!is_dir(db_name))
    {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0)
    {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string &db_name)
{
    if (!is_dir(db_name))
    {
        throw DatabaseNotFoundError(db_name);
    }
    if (chdir(db_name.c_str()) < 0)
    { // 进入名为db_name的目录
        throw UnixError();
    }
    std::ifstream ifs(DB_META_NAME);
    if (!ifs.is_open())
    { // 检查文件是否成功打开
        throw FileNotFoundError(DB_META_NAME);
    }
    ifs >> db_;  // 用重载过的>>载入数据库元数据
    ifs.close(); // 关闭文件
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta()
{
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db()
{
    flush_meta();
    db_.name_.clear();
    db_.tabs_.clear();
    if (chdir("..") < 0)
    {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables(Context *context)
{
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_)
    {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::desc_table(const std::string &tab_name, Context *context)
{
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols)
    {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context
 */
void SmManager::create_table(const std::string &tab_name, const std::vector<ColDef> &col_defs, Context *context)
{
    if (db_.is_table(tab_name))
    {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs)
    {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset; // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表(已完成)
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string &tab_name, Context *context)
{
    // 判断表是否存在
    if (!db_.is_table(tab_name))
    {
        throw TableNotFoundError(tab_name);
    }
    // 删除表格的文件
    rm_manager_->destroy_file(tab_name);
    // 从数据库的元数据中移除表格信息
    db_.tabs_.erase(tab_name);
    // 删除表的文件句柄
    fhs_.erase(tab_name);
    // 将修改后的数据库元数据持久化到磁盘
    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */

// void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
//     //判断表是否存在
//     if (!db_.is_table(tab_name)) {
//         throw TableNotFoundError(tab_name);
//     }
// }

/**
//  * @description: 删除索引
//  * @param {string&} tab_name 表名称
//  * @param {vector<string>&} col_names 索引包含的字段名称
//  * @param {Context*} context
 */
// void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {

// }


// 此版本在这里需要vector中的string类型，尝试把源文件copy并修改，学会单列索引和多列索引的区别，此为gpt书写版本版本
void SmManager::create_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context)
{
    if (!db_.is_table(tab_name)){
        throw TableNotFoundError(tab_name);
    }
        TabMeta &tab = db_.get_table(tab_name);
        std::vector<std::string> col_indices;
        std::vector<ColMeta> cols;

        // Verify columns and check if index already exists
        for (const auto &col_name : col_names)
        {
            auto col = tab.get_col(col_name);
            if (col->index)
            {
                throw IndexExistsError(tab_name, {col_name});
            }
            col_indices.push_back(col_name);
            cols.push_back(*col);
        }

        // Create a unique index name based on column names
        std::string index_name = ix_manager_->get_index_name(tab_name, col_indices);

        // Create index file
        ix_manager_->create_index(tab_name, cols);

        // Open index file
        auto ih = ix_manager_->open_index(tab_name, col_indices);

        // Index all records into index
        auto file_handle = fhs_.at(tab_name).get();
        for (RmScan rm_scan(file_handle); !rm_scan.is_end(); rm_scan.next())
        {
            auto rec = file_handle->get_record(rm_scan.rid(), context);

            // Construct a composite key
            std::string composite_key;
            for (const auto &col : cols)
            {
                composite_key.append(rec->data + col.offset, col.len);
            }

            ih->insert_entry(composite_key.data(), rm_scan.rid(), context->txn_);
        }

        // Store index handle
        ihs_.emplace(index_name, std::move(ih));

        // Mark columns index as created
        for (auto col : cols)
        {
            col.index = true;
        }
}


void SmManager::drop_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context)
{
    TabMeta &tab = db_.get_table(tab_name);
    std::vector<std::string> col_indices;
    std::vector<ColMeta> cols;

    // Verify columns and check if index exists
    for (const auto &col_name : col_names)
    {
        auto col = tab.get_col(col_name);
        if (!col->index)
        {
            throw IndexNotFoundError(tab_name, {col_name});
        }
        col_indices.push_back(col_name);
        cols.push_back(*col);
    }

    // Get the unique index name for the columns
    std::string index_name = ix_manager_->get_index_name(tab_name, col_indices);

    // Close and destroy the index
    ix_manager_->close_index(ihs_.at(index_name).get());
    ix_manager_->destroy_index(tab_name, col_indices);

    // Remove the index handle
    ihs_.erase(index_name);

    // Mark columns index as removed
    for (auto col : cols)
    {
        col.index = false;
    }
}


// lab4 used Transaction rollback management
//------rucbase code
// void SmManager::rollback_insert(const std::string &tab_name, const Rid &rid, Context *context)
// {
//     auto tab = db_.get_table(tab_name);
//     auto file_handle = fhs_.at(tab_name).get();
//     for (size_t col = 0; col < tab.cols.size(); col++)
//         if (tab.cols[col].index)
//             ihs_.at(get_ix_manager()->get_index_name(tab_name, col)).get()->delete_entry(file_handle->get_record(rid, context)->data + tab.cols[col].offset, nullptr);
//     fhs_.at(tab_name).get()->delete_record(rid, context);
// }

// void SmManager::rollback_delete(const std::string &tab_name, const RmRecord &record, Context *context)
// {
//     auto tab = db_.get_table(tab_name);
//     auto file_handle = fhs_.at(tab_name).get();
//     for (size_t col = 0; col < tab.cols.size(); col++)
//         if (tab.cols[col].index)
//             ihs_.at(get_ix_manager()->get_index_name(tab_name, col)).get()->insert_entry(record.data + tab.cols[col].offset, file_handle->insert_record(record.data, context), context->txn_);
// }

// void SmManager::rollback_update(const std::string &tab_name, const Rid &rid, const RmRecord &record, Context *context)
// {
//     auto tab = db_.get_table(tab_name);
//     auto file_handle = fhs_.at(tab_name).get();
//     for (size_t col = 0; col < tab.cols.size(); col++)
//         if (tab.cols[col].index)
//             ihs_.at(get_ix_manager()->get_index_name(tab_name, col)).get()->delete_entry(file_handle->get_record(rid, context)->data + tab.cols[col].offset, nullptr);
//     fhs_.at(tab_name).get()->update_record(rid, record.data, context);
//     for (size_t col = 0; col < tab.cols.size(); col++)
//         if (tab.cols[col].index)
//             ihs_.at(get_ix_manager()->get_index_name(tab_name, col)).get()->insert_entry(record.data + tab.cols[col].offset, rid, context->txn_);
// }

// void SmManager::rollback_insert(const std::string &tab_name, const Rid &rid, Context *context)
// {
//     auto tab = db_.get_table(tab_name);
//     auto file_handle = fhs_.at(tab_name).get();
//     auto record = file_handle->get_record(rid, context);

//     // 删除已插入的索引
//     for (size_t col = 0; col < tab.cols.size(); col++)
//     {
//         if (tab.cols[col].index)
//         {
//             auto &index_handle = ihs_.at(get_ix_manager()->get_index_name(tab_name, {tab.cols[col].name}));
//             index_handle->delete_entry(record->data + tab.cols[col].offset,context->txn_);
//         }
//     }

//     // 删除记录本身
//     file_handle->delete_record(rid, context);
// }

// void SmManager::rollback_delete(const std::string &tab_name, const RmRecord &record, Context *context)
// {
//     auto tab = db_.get_table(tab_name);
//     auto file_handle = fhs_.at(tab_name).get();

//     // 重新插入记录
//     Rid rid = file_handle->insert_record(record.data, context);

//     // 更新索引
//     for (size_t col = 0; col < tab.cols.size(); col++)
//     {
//         if (tab.cols[col].index)
//         {
//             auto &index_handle = ihs_.at(get_ix_manager()->get_index_name(tab_name, {tab.cols[col].name}));
//             index_handle->insert_entry(record.data + tab.cols[col].offset, rid, context->txn_);
//         }
//     }
// }

// void SmManager::rollback_update(const std::string &tab_name, const Rid &rid, const RmRecord &record, Context *context)
// {
//     auto tab = db_.get_table(tab_name);
//     auto file_handle = fhs_.at(tab_name).get();
//     auto old_record = file_handle->get_record(rid, context);

//     // 删除旧的索引
//     for (size_t col = 0; col < tab.cols.size(); col++)
//     {
//         if (tab.cols[col].index)
//         {
//             auto &index_handle = ihs_.at(get_ix_manager()->get_index_name(tab_name, {tab.cols[col].name}));
//             index_handle->delete_entry(old_record->data + tab.cols[col].offset, context->txn_);
//         }
//     }

//     // 恢复旧记录内容
//     file_handle->update_record(rid, record.data, context);

//     // 添加新的索引
//     for (size_t col = 0; col < tab.cols.size(); col++)
//     {
//         if (tab.cols[col].index)
//         {
//             auto &index_handle = ihs_.at(get_ix_manager()->get_index_name(tab_name, {tab.cols[col].name}));
//             index_handle->insert_entry(record.data + tab.cols[col].offset, rid, context->txn_);
//         }
//     }
// }
