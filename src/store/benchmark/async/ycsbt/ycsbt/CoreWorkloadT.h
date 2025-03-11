#pragma once

#include "store/benchmark/async/ycsbt/ycsb/CoreWorkload.h"
#include "store/benchmark/async/ycsbt/ycsbt/constants.h"
#include <chrono>
#include <thread>

namespace ycsbt {

bool Validate(const std::string& json_orig_params,
              const std::string& json_subst_params,
              std::string* json_final_params);

class CoreWorkloadT : public ycsbt::CoreWorkload {


 public:
  CoreWorkloadT(){};

  CoreWorkloadT(double  zipf_coefficient, int num_keys, int num_ops_txn, double ycsbt_read_percentage,double ycsbt_write_percentage) {
    Init(zipf_coefficient, num_keys, num_ops_txn, ycsbt_read_percentage, ycsbt_write_percentage);
  }

  bool
  Init(double  zipf_coefficient, int num_keys, int num_ops_txn, double ycsbt_read_percentage,double ycsbt_write_percentage) {
    ycsbt::CoreWorkload::Init(zipf_coefficient, num_keys, num_ops_txn, ycsbt_read_percentage, ycsbt_write_percentage);
    m_size_batch = 10;
    m_oneshot_weight = 1;
    m_multishot_weight = 0;
    // normalize them
    double total_weight = m_oneshot_weight + m_multishot_weight;
    m_oneshot_weight = m_oneshot_weight / total_weight;
    m_multishot_weight = m_multishot_weight / total_weight;

    m_dist_txn_type =
        std::discrete_distribution<int>(m_oneshot_weight, m_multishot_weight);
    m_distr_op = std::discrete_distribution<int>(
        {this->m_read_weight, 0.0, this->m_write_weight});
    return true;
  }

 
  // void
  // Run(DB* db) {
  //   TXN_TYPE tt = GetNextTXN();
  //   switch (tt) {
  //     case ONESHOT: {
  //       ycsb::OP op = GetNextOperation();
  //       switch (op) {
  //         case ycsb::SELECT:
  //           OneShotSelect(db);
  //           break;
  //         case ycsb::UPDATE:
  //           OneShotUpdate(db);
  //           break;
  //       }
  //       break;
  //     }
  //     case MULTISHOT: {
  //       MultiShot(db);
  //       break;
  //     }
  //   }
  //   //LOG(INFO) << "End of Run FUnction" ;
  //   std::this_thread::sleep_for(std::chrono::milliseconds(5));
  // }

  // bool
  // ParallelPopulate(DB *db, uint64_t worker_id, uint64_t num_workers){
  //   LOG(INFO) << "Parallel Populate ycsb+T";
  //   return ycsb::CoreWorkload<DB>::ParallelPopulate(db, worker_id, num_workers);
  // }


 protected:
  uint64_t m_size_batch;
  double m_oneshot_weight;
  double m_multishot_weight;
  std::discrete_distribution<int> m_distr_op;
  std::discrete_distribution<int> m_dist_txn_type;

  ycsbt::OP
  GetNextOperation() {
    return m_distr_op(this->m_gen);
  }

  TXN_TYPE
  GetNextTXN() { return m_dist_txn_type(this->m_gen); }

  // void
  // OneShotSelect(DB* db) {
  //   int32_t* numColBuf = db->get_numCol_buf();
  //   int32_t* sizeBatchBuf = db->get_sizeBatch_buf();
  //   KeyType* ksBuf = db->get_keys_buf();
  //   ColumnRefType* colBuf = db->get_col_buf();
  //   ValueType* resBuf = db->get_result_buf();

  //   if (this->m_read_all) {
  //     for (uint64_t i = 0; i < this->m_num_columns; i++) {
  //       colBuf[i] = i;
  //       *numColBuf = this->m_num_columns;
  //     }
  //   } else {
  //     colBuf[0] = this->GetRandomColumn();
  //     *numColBuf = 1;
  //   }

  //   *sizeBatchBuf = m_size_batch;
  //   int32_t i_result = 0;

  //   for (int32_t i = 0; i < m_size_batch; ++i) {
  //     ksBuf[i] = this->BuildKeyname(this->GetNextKey(ycsb::SELECT));
  //   }

  //   db->BeginTx();
  //   if (db->BatchGet() == EAAS_W_EC_SUCCESS) {
  //     db->CommitTx();
  //   } else {
  //     db->RollbackTx();
  //   }
  // }

  // void
  // OneShotUpdate(DB* db) {
  //   int32_t* numColBuf = db->get_numCol_buf();
  //   int32_t* sizeBatchBuf = db->get_sizeBatch_buf();
  //   KeyType* ksBuf = db->get_keys_buf();
  //   ColumnRefType* colBuf = db->get_col_buf();
  //   ValueType* valBuf = db->get_val_buf();

  //   if (this->m_update_all) {
  //     for (uint64_t i = 0; i < this->m_num_columns; i++) {
  //       colBuf[i] = i;
  //       *numColBuf = this->m_num_columns;
  //     }
  //   } else {
  //     colBuf[0] = this->GetRandomColumn();
  //     *numColBuf = 1;
  //   }

  //   *sizeBatchBuf = m_size_batch;
  //   int32_t i_value = 0;

  //   for (int32_t i = 0; i < m_size_batch; ++i) {
  //     ksBuf[i] = this->BuildKeyname(this->GetNextKey(ycsb::UPDATE));
  //     if (this->m_update_all) {
  //       for (uint64_t i = 0; i < this->m_num_columns; i++) {
  //         valBuf[i_value] = this->BuildSingleValue();
  //         i_value++;
  //       }
  //     } else {
  //       valBuf[i_value] = this->BuildSingleValue();
  //       i_value++;
  //     }
  //   }

  //   db->BeginTx();
  //   if (db->BatchPut() == EAAS_W_EC_SUCCESS) {
  //     db->CommitTx();
  //   } else {
  //     db->RollbackTx();
  //   }
  // }

  // void
  // MultiShotSelect(DB* db) {
  //   db->BeginTx();
  //   for (int32_t i = 0; i < m_size_batch; ++i) {
  //     if (this->DoSelect(db) != EAAS_W_EC_SUCCESS) {
  //       db->RollbackTx();
  //     }
  //   }
  //   db->CommitTx();
  // }

  // void
  // MultiShotUpdate(DB* db) {
  //   db->BeginTx();
  //   for (int32_t i = 0; i < m_size_batch; ++i) {
  //     if (this->DoUpdate(db) != EAAS_W_EC_SUCCESS) {
  //       db->RollbackTx();
  //     }
  //   }
  //   db->CommitTx();
  // }

  // void
  // MultiShot(DB* db) {
  //   ycsb::OP op;
  //   db->BeginTx();
  //   for (int32_t i = 0; i < m_size_batch; ++i) {
  //     op = GetNextOperation();
  //     switch (op) {
  //       case ycsb::SELECT:
  //         if (this->DoSelect(db) != EAAS_W_EC_SUCCESS) {
  //           db->RollbackTx();
  //         }
  //         break;
  //       case ycsb::UPDATE:
  //         if (this->DoUpdate(db) != EAAS_W_EC_SUCCESS) {
  //           db->RollbackTx();
  //         }
  //         break;
  //     }
  //   }
  //   db->CommitTx();
  // }
};

}  // namespace eaas::workload::ycsbt
