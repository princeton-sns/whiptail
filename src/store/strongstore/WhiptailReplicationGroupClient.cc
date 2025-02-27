//
// Created by jennifer on 2/25/25.
//

#include "WhiptailReplicationGroupClient.h"

namespace strongstore {

    WhiptailReplicationGroupClient(const transport::Configuration &shard_config,
                                   const transport::Configuration &replica_config,
                                   int shard_idx, int replica_idx,
                                   Transport *transport, const TrueTime &tt)
                                   : shard_config_(shard_config),
                                   replica_config_(replica_config),
                                   transport_(transport),
                                   shard_idx_(shard_idx),
                                   replica_idx_(replica_idx),
                                   tt_(tt) {

                                   transport_->Register(this, shard_config_, shard_idx_, replica_idx_);
                                   }

} // strongstore