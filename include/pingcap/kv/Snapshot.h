#pragma once

#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionClient.h>

namespace pingcap
{
namespace kv
{

struct Scanner;

struct Snapshot
{
    ClusterHelper cluster_helper;
    const int64_t version;

    Snapshot(Cluster * cluster_, uint64_t version_) : cluster_helper(cluster_), version(version_) {}
    Snapshot(Cluster * cluster_) : cluster_helper(cluster_), version(cluster_->pd_client->getTS()) {}

    std::string Get(const std::string & key);
    std::string Get(Backoffer & bo, const std::string & key);

    Scanner Scan(const std::string & begin, const std::string & end);
};

} // namespace kv
} // namespace pingcap
