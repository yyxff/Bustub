//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
    // std::list<frame_id_t> history_list;
    // std::priority_queue<frame_id_t> buffer_pq;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
    printf("evict\n"); 
    if (!history_list.empty()){
        std::cout<<"history"<<std::endl;
        for (frame_id_t fid : history_list) {
            printf("%d",fid);

            if (node_store_.find(fid)->second.is_evictable_){
                
                history_list.remove(fid);
                node_store_.erase(fid);
                curr_size_--;
                return fid;
            }
        }

    }else{
        // delete it
        std::cout<<"buffer"<<std::endl;
        std::cout<<"head"<<head<<std::endl;
        std::cout<<"tail"<<tail<<std::endl;
        frame_id_t start = tail;
        while (start!=-1){
            LRUKNode & node = node_store_.find(start)->second;
            std::cout<<node.fid_<<std::endl;
            if (node.is_evictable_){
                frame_id_t fid = node.fid_;
                if (node.fid_prev!=-1){
                    node_store_.find(node.fid_prev)->second.fid_next=node.fid_next;
                }else{
                    head=node.fid_next;
                }
                if (node.fid_next!=-1){
                    node_store_.find(node.fid_next)->second.fid_prev=node.fid_prev;
                }else{
                    tail=node.fid_prev;
                }
                node_store_.erase(fid);
                curr_size_--;
                return fid;
            }
            start=node.fid_prev;
        }
    }
    return std::nullopt; 
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    
    if (auto it = node_store_.find(frame_id); it != node_store_.end()){
        LRUKNode & node = it->second;
        // update time
        if (node.history_.size() >= k_){
            node.history_.pop_front();
        }
        node.history_.push_back(frame_id);
        
        // replace it
        if(node.is_inBuffer){
            // delete it
            if (node.fid_prev!=-1){
                node_store_.find(node.fid_prev)->second.fid_next=node.fid_next;
            }else{
                head=node.fid_next;
            }
            if (node.fid_next!=-1){
                node_store_.find(node.fid_next)->second.fid_prev=node.fid_prev;
            }else{
                tail=node.fid_prev;
            }

            // new head
            node.fid_next=head;
            node.fid_prev=-1;
            if (head!=-1){
                node_store_.find(head)->second.fid_prev=node.fid_;
            }
            head=node.fid_;
        }else if (node.history_.size()>=k_){
            history_list.remove(node.fid_);
            // new head
            node.fid_next=head;
            node.fid_prev=-1;
            if (head!=-1){
                node_store_.find(head)->second.fid_prev=node.fid_;
            }
            head=node.fid_;
            if (tail==-1){
                tail=node.fid_;
            }
            node.is_inBuffer=true;
        }
    }else{// no exist
        LRUKNode node;
        node.fid_ = frame_id;
        node.fid_prev = -1;
        node.fid_next = -1;
        node.history_.push_back(time(NULL));
        node_store_[frame_id] = node;
        history_list.push_back(frame_id);

    }
    
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    if (node_store_.find(frame_id) == node_store_.end()){
        return;
        throw std::invalid_argument("invalid fid");
    }
    bool now = node_store_.find(frame_id)->second.is_evictable_;
    node_store_.find(frame_id)->second.is_evictable_=set_evictable;
    if (set_evictable!=now && set_evictable)curr_size_++;
    else if (set_evictable!=now && set_evictable==false)curr_size_--;

}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {

    LRUKNode & node = node_store_.find(frame_id)->second;
    if (node.is_evictable_==false){
        throw std::invalid_argument("this frame is not evictable");
    }

    history_list.remove(frame_id);

    if (node.is_inBuffer){

    
        // delete it
        if (node.fid_prev!=-1){
            node_store_.find(node.fid_prev)->second.fid_next=node.fid_next;
        }else{
            tail=node.fid_prev;
        }
        if (node.fid_next!=-1){
            node_store_.find(node.fid_next)->second.fid_prev=node.fid_prev;
        }else{
            head=node.fid_next;
        }

        
    }
    node_store_.erase(frame_id);
    curr_size_--;
    
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
