#pragma once

#include "lib/message.h"
#include "store/benchmark/async/ycsbt/ycsb/constants.h"
#include "store/benchmark/async/ycsbt/random/zipfian_distribution.h"
#include "store/common/frontend/transaction_utils.h"
#include <inttypes.h>

#include <optional>
#include <random>
#include <memory>
#include <fstream>

#include <cstdint>
#include <cmath>
#include <string>
#include <thread>
#include <mutex>
#include <chrono>

namespace ycsbt {


class CoreWorkload {


  using KeyType = std::uint64_t;
  using ValueType = std::uint64_t;
  using ColumnRefType = std::uint64_t;

 public:
  CoreWorkload(){
    // TODO
  }

  CoreWorkload(const std::string &params_json_str){
    Init(params_json_str);
  }

  ~CoreWorkload() {
    if (m_partition_bounds != nullptr) {
      delete[] m_partition_bounds;
    }
    if (m_op_partition_nop != nullptr) {
      delete[] m_op_partition_nop;
    }
  }

  /**
   * Initialize member variables
   * Compute values for private members
   */
  bool 
  Init(const std::string &params_json_str) {

  
    m_num_rows = 0;
    m_num_columns = 0;
    m_read_weight = 1.0;
    m_write_weight = 1.0;
    m_select_weight = 1.0;
    m_scan_weight = 1.0;
    m_update_weight = 1.0;
    m_insert_weight = 1.0;
    m_delete_weight = 1.0;
    m_upsert_weight = 1.0;
    m_zipf_constant = 0.99;
    m_max_value = 1000;
    m_seed = 98;
    m_update_all = true;
    m_read_all = true;
    m_duration = std::chrono::seconds(60);
    // TODO: make it confidurable
    m_maxscanlength = 50;
    m_distr_scanlength = std::uniform_int_distribution<int>(1, m_maxscanlength);
   
    m_distribution = ZIPFIAN;
    
    // Calc weights, see comments in the private member block below
    // Calc first-level weights
    double total_weight = m_read_weight + m_write_weight;
    m_read_weight = m_read_weight / total_weight;
    m_write_weight = m_write_weight / total_weight;
    // Calc second-level read weights
    if (m_read_weight > 0) {
      double total_read_weight = m_select_weight;
      m_select_weight = m_select_weight / total_read_weight * m_read_weight;
    } else {
      m_select_weight = m_scan_weight = 0;
    }
    // Calc second-level write weights
    if (m_write_weight > 0) {
      double total_write_weight = m_update_weight + m_insert_weight;
      m_update_weight = m_update_weight / total_write_weight * m_write_weight;
      m_insert_weight = m_insert_weight / total_write_weight * m_write_weight;
    } else {
      m_update_weight = m_insert_weight = m_delete_weight = m_upsert_weight = 0;
    }
    // Initialize distribution and storage
    // I removed the scan and delete @lh
    Debug("Ops: select: %f, update: %f, insert: %f", m_select_weight, m_update_weight, m_insert_weight);
    m_distr_op = std::discrete_distribution<>(
      {m_select_weight,  m_update_weight,
      m_insert_weight, });
    m_gen.seed(m_seed);
    m_distr_value = std::uniform_int_distribution<ValueType>(0, m_max_value);
    m_distr_columns = std::uniform_int_distribution<ColumnRefType>(0, m_num_columns - 1);

    // Partitions - dividing the entire key space into serveral parts
    // Set upper bounds of partitions and index of operations
     m_partition_bounds = new uint64_t[NUM_PARTITIONS];
     for (uint64_t i = 0; i < NUM_PARTITIONS; ++i) {
      // Initilize upper bounds
      // m_num_rows represent the number of rows that need to be populated
      m_partition_bounds[i] = m_num_rows / (NUM_PARTITIONS - 1) * (i + 1);
    }
    // Initialize the assignment of operations to partitions
    // The 1st partition is READ-ONLY; 0
    // The 2nd is for deletion; 1
    // The 3rd is for updatation; 2
    // The 4th is for insertion; 3
    // In constant.h, SELECT=0, DELETE=1, UPDATE=2, INSERT=3; we use them as postal code while fixing OP=int
    m_op_partition_nop = new int[NUM_OPS]{0, 1, 2, 3};

    // TODO [Shengzhou] Temporarily using the first three partitions as the partition of read and update
    m_zipf_key = zipfian_distribution<KeyType>(0, m_partition_bounds[UPDATE]-1, m_zipf_constant);
    return true;
  }
  
  
  // TODO [Shengzhou works with Shawn] do Validate() again in Coreworkload later 
  //  when running experiments.

  /**
   * Pre-fill the database rows
   */
  // std::vector<std::pair<KeyType, ValueType>> Populate() {
  //   //LOG(INFO) << "Getting num cols in populate";
  //   std::vector<std::pair<KeyType, ValueType>> rows;


  //   //LOG(INFO) << "Initializing DB";
 
  //   //LOG(INFO) << "DB initialized";

  //   // initialize all the partitions except INSERT
  //   for (OP op : {SELECT, DELETE, UPDATE}) {
  //     // get partition index
  //     int index = m_op_partition_nop[op];
  //     // partition begin index
  //     int begin;
  //     if (index == 0) {
  //       begin = 0;
  //     } else {
  //       begin = m_partition_bounds[index-1];
  //     }
  //     for (uint64_t i = begin; i < m_partition_bounds[index]; i++) {
  //       KeyType* kBuf = db->get_key_buf();
  //       *kBuf = BuildKeyname(i);
  //       ColumnRefType* colBuf = db->get_col_buf();
  //       ValueType* valBuf = db->get_val_buf();
  //       for (uint64_t j = 0; j < m_num_columns; j++){
  //         colBuf[j] = j;
  //         valBuf[j] = BuildSingleValue();
  //       }
  //       db->Set();
  //     }
  //   }
  //   return true;
  // }


  // bool 
  // ParallelPopulate(DB *db, uint64_t worker_id, uint64_t num_workers) {
  // //  LOG(INFO) << "Parallel Populate YCSB";
  //   int32_t* numColBuf = db->get_numCol_buf();
  //   *numColBuf = m_num_columns;
  //   db->DB_Init();
  //   // int count = 0;
  //   uint64_t begin;
  //   uint64_t intervals = m_num_rows / num_workers;
  //   uint64_t end;
  //   // worker_id: 0, 1, 2..num_workers-1
  //   begin = intervals * worker_id;
  //   if (worker_id == num_workers - 1){
  //     end = m_num_rows;
  //   } else {
  //     end = intervals * (worker_id + 1);
  //   }

  //  // LOG(INFO) << "Parallel Populate before for loop";
  //   for (uint64_t i = begin; i < end; i++) {
  //       KeyType* kBuf = db->get_key_buf();
  //       *kBuf = BuildKeyname(i);
  //       ColumnRefType* colBuf = db->get_col_buf();
  //       ValueType* valBuf = db->get_val_buf();
  //       for (uint64_t j = 0; j < m_num_columns; j++){
  //         colBuf[j] = j;
  //         valBuf[j] = BuildSingleValue();
  //       }
  //       // count++;
  //       db->Set();
  //     }
  //   // LOG(INFO) << worker_id << "ParallelPopulate() rows: " << count;
  //   //LOG(INFO) << "End Parallel populate YCSB";
  //   return true;
  // }
  /**
   * Prepare and execute a select OP
   */
  Operation DoSelect() {
    
    KeyType key = BuildKeyname(GetNextKey(SELECT));
    return Get(std::to_string(key));

  }

  /**
   * Prepare and execute an update OP
   */
   Operation DoUpdate() {

    KeyType key = BuildKeyname(GetNextKey(UPDATE));
    ValueType value = BuildSingleValue();

    return Put(std::to_string(key), std::to_string(value));

  }
  /**
   * Prepare and execute an insert OP
   */
   Operation DoInsert() {
   
    KeyType key = BuildKeyname(GetNextKey(INSERT));
    ValueType value = BuildSingleValue();

    return Put(std::to_string(key), std::to_string(value));

  }

  /**
   * Determine which type of op to performance,
   * Prepare op, and
   * Perform op
   * TODO: add op/client context, e.g., a 2nd arg to Run and in each do_op
   */
  public:
  Operation NextOp() {
    // decide what operation to perform
    OP op = GetNextOperation();
    Debug("Next operation is %d", op);
    switch (op) {
    case SELECT:
      return DoSelect();
    case UPDATE:
      return DoUpdate();
    case INSERT:
      return DoInsert();
    }
  }

 protected:
  uint64_t buff = 10;
  // first-level weights: read vs. write
  // second-level weights: read = select + scan;
  //             write = update + insert + delete + upsert
  double m_read_weight;
  double m_write_weight;
  double m_select_weight;
  double m_scan_weight;
  double m_update_weight;
  double m_insert_weight;
  double m_delete_weight;
  double m_upsert_weight;
  // zipfian or uniform
  int m_distribution;
  double m_zipf_constant;
  // DB related parameters
  uint64_t m_max_value;
  uint64_t m_num_rows;
  uint64_t m_num_columns;
  uint64_t m_seed;
  uint64_t *m_partition_bounds;
  int * m_op_partition_nop;
  std::mt19937_64 m_gen;
  std::discrete_distribution <int> m_distr_op;
  zipfian_distribution<uint64_t> m_zipf_key;
  std::uniform_int_distribution<ValueType> m_distr_value;
  std::uniform_int_distribution<ColumnRefType> m_distr_columns;
  bool m_read_all; // this will be select all?
  bool m_update_all;
  // hash value
  uint64_t m_hashval;
  uint64_t m_octet;
  // number of runs
  uint64_t m_num_runs;
  // number of multiple threads to run
  uint64_t m_num_threads;
  // Mutex to protect shared resources
  std::mutex bufferMutex;
  // duration of running
  std::chrono::seconds m_duration;
  int m_maxscanlength = 1000;
  std::uniform_int_distribution<int> m_distr_scanlength;

  // Private function

  // get next operation
  OP GetNextOperation() {
    return m_distr_op(m_gen); // this returns the integer indexing
                  // the op constant, based on the weigths
  }

  /**
   * Get a key based on op type, as
   * different type op may access different key partitions
   */
  uint64_t GetNextKey(OP op) {
    // TODO [Shengzhou]  have a cost-less zipfian to use
    if (op==INSERT) {
      return ++m_num_rows;
    } 
    return m_zipf_key(m_gen);
  }

  // get a random number in keysapce
  // KeyType get_random_key() {
  //   return m_distr_key(m_gen);
  // }
  // get the random singe value between 0 - m_max_value
  ValueType BuildSingleValue() {
    return m_distr_value(m_gen);
  }
  // get the random column index between 0 - number_columns
  ColumnRefType GetRandomColumn() {
    return m_distr_columns(m_gen);
  }
  // get the keyname of key
  KeyType BuildKeyname(uint64_t keynum) {
    uint64_t hash = fnvhash64(keynum);
    return (KeyType) hash;
  }

  // FNV hash from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
  uint64_t fnvhash64(uint64_t val) {
    m_hashval = FNV_OFFSET_BASIS_64;

    for (int i = 0; i < 8; i++) {
      m_octet = val & 0x00ff;
      val = val >> 8;

      m_hashval = m_hashval ^ m_octet;
      m_hashval = m_hashval * FNV_PRIME_64;
    }
    return m_hashval;
  }
};

}   // namespace eaas::workload::ycsb

