#include "lib/tcptransport.h"
#include "store/common/truetime.h"
#include "store/strongstore/client.h"
#include "store/strongstore/common.h"
#include "store/strongstore/networkconfig.h"
#include "store/strongstore/server.h"

int main() {

  // strongstore::NetworkConfiguration net_config{};
  // transport::Configuration config;
  // transport::Configuration shard_config;
  // transport::Configuration replica_config;
  // TCPTransport transport{};
  // TrueTime tt{0};
  // strongstore::Client client(strongstore::Consistency::SS, net_config,
  //                            "client_region", config, 1, 1, 0, &transport,
  //                            nullptr, tt, true, 0.0);

  // strongstore::Server server(strongstore::Consistency::SS, shard_config,
  //                            replica_config, 1, 0, 0, &transport, tt, true);

  // return 0;
}
