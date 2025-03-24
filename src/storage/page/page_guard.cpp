//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <memory>

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  is_valid_ = true;
  // break dead lock
  if (!frame_->rwlatch_.try_lock_shared()){
    bpm_latch_->unlock();
    frame_->rwlatch_.lock_shared();
    bpm_latch_->lock();
  }
  // frame_->rwlatch_.lock_shared();

  // pin count++
  frame_->pin_count_.fetch_add(1);

  replacer_->SetEvictable(frame_->frame_id_, false);

  // std::cout<<"get read guard, page: "<<page_id<<std::endl;
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  page_id_ = that.page_id_;
  frame_ = that.frame_;
  replacer_ = that.replacer_;
  bpm_latch_ = that.bpm_latch_;
  is_valid_ = that.is_valid_;
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
  if (this != &that){
    this->Drop();
    page_id_ = that.page_id_;
    frame_ = that.frame_;
    replacer_ = that.replacer_;
    bpm_latch_ = that.bpm_latch_;
    is_valid_ = that.is_valid_;
    that.is_valid_ = false;
  }
  return *this; }

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Flush() { 

  // dirty data, write back
  if (frame_->is_dirty_){
    // use a promise to wait it done
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();

    DiskRequest write_request{true, frame_->GetDataMut(), page_id_, std::move(promise)};
    disk_scheduler_->Schedule(std::move(write_request));

    // wait until done
    future.get();

    frame_->is_dirty_ = false;

  }
  return ;
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Drop() { 
  if (is_valid_ == false){
    return ;
  }
  
  // std::cout << "ReadPageGuard::Drop() called, page: "<<page_id_ << std::endl;
  std::scoped_lock<std::mutex> latch(*bpm_latch_);
  frame_->pin_count_.fetch_sub(1);
  if (frame_->pin_count_.load() == 0) {
    replacer_->SetEvictable(frame_->frame_id_, true);
    // std::cout<<"read guard make it true, page: "<<page_id_ << std::endl;

  }
  frame_->rwlatch_.unlock_shared();
  is_valid_ = false;
 }

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  is_valid_ = true;
  // break dead lock
  if (!frame_->rwlatch_.try_lock()){
    bpm_latch_->unlock();
    frame_->rwlatch_.lock();
    bpm_latch_->lock();
  }
  // frame_->rwlatch_.lock();

  // pin count++
  frame_->pin_count_.fetch_add(1);
  // dirty
  frame_->is_dirty_ = true;

  // you need to set it to false!!! or before you get guard, some one drop it and make it true
  replacer_->SetEvictable(frame_->frame_id_, false);

  // std::cout<<"get write guard, page: "<<page_id<<std::endl;

}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  page_id_ = that.page_id_;
  frame_ = that.frame_;
  replacer_ = that.replacer_;
  bpm_latch_ = that.bpm_latch_;
  is_valid_ = that.is_valid_;
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { 
  if (this != &that){
    this->Drop();
    page_id_ = that.page_id_;
    frame_ = that.frame_;
    replacer_ = that.replacer_;
    bpm_latch_ = that.bpm_latch_;
    is_valid_ = that.is_valid_;
    that.is_valid_ = false;
  }
  return *this; 
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Flush() { 
  // dirty data, write back
  if (frame_->is_dirty_){
    // use a promise to wait it done
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();

    DiskRequest write_request{true, frame_->GetDataMut(), page_id_, std::move(promise)};
    disk_scheduler_->Schedule(std::move(write_request));

    // wait until done
    future.get();

    frame_->is_dirty_ = false;

  }
  return ;  
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Drop() { 
  if (is_valid_ == false){
    return ;
  }
  
  // std::cout << "WritePageGuard::Drop() called, page: "<<page_id_ << std::endl;
  std::scoped_lock<std::mutex> latch(*bpm_latch_);
  frame_->pin_count_.fetch_sub(1);
  if (frame_->pin_count_.load() == 0) {
    replacer_->SetEvictable(frame_->frame_id_, true);
    // std::cout<<"write guard make it true, page: "<<page_id_ << std::endl;
  }
  frame_->rwlatch_.unlock();
  is_valid_ = false;
 }

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
