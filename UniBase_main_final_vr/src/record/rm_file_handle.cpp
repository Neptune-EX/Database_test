#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
    
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    if (rid.slot_no < 0 || !Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        throw RecordNotFoundError(rid.page_no,rid.slot_no);
    }
    char* record_data = page_handle.get_slot(rid.slot_no);
    std::unique_ptr<RmRecord> record = std::make_unique<RmRecord>(file_hdr_.record_size, record_data);

    return record;
    
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // Todo:
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no
    // if (file_hdr_.record_size < sizeof(buf)) {
    //     throw InvalidRecordSizeError(sizeof(buf));
    // }

    
    RmPageHandle page_handle = create_page_handle();
    //printf("%d %d\n",page_handle.page_hdr->next_free_page_no,page_handle.page_hdr->num_records);
    //printf("FIN create\n");
    int page_no = page_handle.page->get_page_id().page_no;
    //printf("FIN get_page_id %d\n",page_no);
    int free_slot = -1;
    while(true){
        int num = page_handle.file_hdr->num_records_per_page;
        //printf("%d\n",num);
        for (int slot_no = 0; slot_no < num; slot_no++) {
            if (!Bitmap::is_set(page_handle.bitmap, slot_no)) {
                free_slot = slot_no;
                break;
            }
        }
        if(free_slot!=-1)break;
        page_no = page_handle.page_hdr->next_free_page_no;
        file_hdr_.first_free_page_no = page_no;
        if(page_no == -1)break;
        page_handle = fetch_page_handle(page_no);
    }
    //printf("FIN GET free_slot %d\n",free_slot);
    if (free_slot == -1) {    
        RmPageHandle new_page_handle = create_new_page_handle();
        page_no = new_page_handle.page->get_page_id().page_no;
        page_handle.page_hdr->next_free_page_no = page_no;
        file_hdr_.first_free_page_no = page_no;
        page_handle = std::move(new_page_handle);
        free_slot = 0;
    }

    char* slot_data = page_handle.get_slot(free_slot);
    memcpy(slot_data, buf, file_hdr_.record_size);

    Bitmap::set(page_handle.bitmap, free_slot);
    page_handle.page_hdr->num_records++;

    if(page_handle.page_hdr->num_records == file_hdr_.num_records_per_page){

        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;

    }
    return Rid{page_no, free_slot};

}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    RmPageHandle page_handle = create_page_handle();                                             // 1
    int free_slot_no = Bitmap::first_bit(0, page_handle.bitmap, file_hdr_.num_records_per_page); // 2
    char *free_slot = page_handle.get_slot(free_slot_no);
    memcpy(free_slot, buf, file_hdr_.record_size); // 3
    Bitmap::set(page_handle.bitmap, free_slot_no);

    page_handle.page_hdr->num_records++;                                     // 4
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) // page is full
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;

    PageId page_id = page_handle.page->get_page_id();
    buffer_pool_manager_->unpin_page(page_id, true);
    return;
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
    //printf("This delete\n");
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    if (rid.slot_no < 0) {
        //printf("rid.slot_no < 0\n");
        throw RecordNotFoundError(rid.page_no,rid.slot_no);
    }
    if(!Bitmap::is_set(page_handle.bitmap, rid.slot_no)){
        //printf("Bitmap\n");
        throw RecordNotFoundError(rid.page_no,rid.slot_no);
    }

    Bitmap::reset(page_handle.bitmap, rid.slot_no);

    page_handle.page_hdr->num_records--;

    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page - 1) {
        release_page_handle(page_handle);
    }
    //printf("delete FIN\n");
}


/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录

    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    if (rid.slot_no < 0  || !Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        throw RecordNotFoundError(rid.page_no,rid.slot_no);
    }

    char* slot_data = page_handle.get_slot(rid.slot_no);
    memcpy(slot_data, buf, file_hdr_.record_size);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
*/
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // Todo:
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception

    std::string table_name = disk_manager_->get_file_name(fd_);
    if (page_no < 0 || page_no >= file_hdr_.num_pages) {
        throw PageNotExistError(table_name,page_no);
    }
    Page* page = buffer_pool_manager_->fetch_page((PageId){fd_,page_no});
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // Todo:
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_
    
    PageId page_id = (PageId){fd_,INVALID_PAGE_ID};
    Page* page;
    page = buffer_pool_manager_->new_page(&page_id);
    int page_no = page_id.page_no;
    file_hdr_.num_pages++;
    //printf("create_new_page_handle_page_no %d %d\n",page_no,file_hdr_.num_pages);

    if(file_hdr_.first_free_page_no==-1){
        file_hdr_.first_free_page_no = page_no;
    }
    else {
        RmPageHandle firstFreePageHandle = fetch_page_handle(file_hdr_.first_free_page_no);
        firstFreePageHandle.page_hdr->next_free_page_no = page_no;
    }
    //std::printf("create_new_page_handle\n");
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层
    if(file_hdr_.first_free_page_no==-1){
        RmPageHandle newpagehandle = create_new_page_handle();
        newpagehandle.page_hdr->next_free_page_no=-1;
        newpagehandle.page_hdr->num_records=0;
        file_hdr_.first_free_page_no = newpagehandle.page->get_page_id().page_no; 
        return newpagehandle;
    }
    int page_no = file_hdr_.first_free_page_no;
    return fetch_page_handle(page_no);
    //return RmPageHandle(&file_hdr_, nullptr);
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle&page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no

    if(file_hdr_.first_free_page_no==-1){
        file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
        page_handle.page_hdr->next_free_page_no = -1;
    }
    else{
        int oldFirstFreePage = file_hdr_.first_free_page_no;
        file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
        page_handle.page_hdr->next_free_page_no = oldFirstFreePage;
    }
}