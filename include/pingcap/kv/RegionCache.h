#pragma once

#include <kvproto/errorpb.pb.h>
#include <kvproto/metapb.pb.h>
#include <pingcap/Log.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/pd/Client.h>

#include <map>
#include <unordered_map>

namespace pingcap
{
namespace kv
{

enum StoreType
{
    TiKV,
    TiFlash
};

struct Store
{
    uint64_t id;
    std::string addr;
    std::string peer_addr;
    std::map<std::string, std::string> labels;

    Store(uint64_t id_, const std::string & addr_, const std::string & peer_addr_, const std::map<std::string, std::string> & labels_)
        : id(id_), addr(addr_), peer_addr(peer_addr_), labels(labels_)
    {}
};

struct RegionVerID
{
    uint64_t id;
    uint64_t conf_ver;
    uint64_t ver;

    RegionVerID() = default;
    RegionVerID(uint64_t id_, uint64_t conf_ver_, uint64_t ver_) : id(id_), conf_ver(conf_ver_), ver(ver_) {}

    bool operator==(const RegionVerID & rhs) const { return id == rhs.id && conf_ver == rhs.conf_ver && ver == rhs.ver; }

    // for debug output
    std::string toString() const { return "{" + std::to_string(id) + "," + std::to_string(conf_ver) + "," + std::to_string(ver) + "}"; }
};

} // namespace kv
} // namespace pingcap

namespace std
{
template <>
struct hash<pingcap::kv::RegionVerID>
{
    using argument_type = pingcap::kv::RegionVerID;
    using result_type = size_t;
    size_t operator()(const pingcap::kv::RegionVerID & key) const { return key.id; }
};
} // namespace std

namespace pingcap
{
namespace kv
{

struct Region
{
    metapb::Region meta;
    metapb::Peer peer;
    std::vector<metapb::Peer> learners;

    Region(const metapb::Region & meta_, const metapb::Peer & peer_, const std::vector<metapb::Peer> & learners_)
        : meta(meta_), peer(peer_), learners(learners_)
    {}

    const std::string & startKey() { return meta.start_key(); }

    const std::string & endKey() { return meta.end_key(); }

    bool contains(const std::string & key) { return key >= startKey() && (key < endKey() || meta.end_key() == ""); }

    RegionVerID verID()
    {
        return RegionVerID{
            meta.id(),
            meta.region_epoch().conf_ver(),
            meta.region_epoch().version(),
        };
    }

    bool switchPeer(uint64_t store_id)
    {
        for (int i = 0; i < meta.peers_size(); i++)
        {
            if (store_id == meta.peers(i).store_id())
            {
                peer = meta.peers(i);
                return true;
            }
        }
        return false;
    }
};

using RegionPtr = std::shared_ptr<Region>;

struct KeyLocation
{
    RegionVerID region;
    std::string start_key;
    std::string end_key;

    KeyLocation() = default;
    KeyLocation(const RegionVerID & region_, const std::string & start_key_, const std::string & end_key_)
        : region(region_), start_key(start_key_), end_key(end_key_)
    {}

    bool contains(const std::string & key) { return key >= start_key && (key < end_key || end_key == ""); }
};

struct RPCContext
{
    RegionVerID region;
    metapb::Region meta;
    metapb::Peer peer;
    std::string addr;

    RPCContext(const RegionVerID & region_, const metapb::Region & meta_, const metapb::Peer & peer_, const std::string & addr_)
        : region(region_), meta(meta_), peer(peer_), addr(addr_)
    {}
};

using RPCContextPtr = std::shared_ptr<RPCContext>;

class RegionCache
{
public:
    RegionCache(pd::ClientPtr pdClient_, std::string key_, std::string value_)
        : pd_client(pdClient_), learner_key(std::move(key_)), learner_value(std::move(value_)), log(&Logger::get("pingcap.tikv"))
    {}

    RPCContextPtr getRPCContext(Backoffer & bo, const RegionVerID & id, const StoreType store_type = TiKV);

    void updateLeader(Backoffer & bo, const RegionVerID & region_id, uint64_t leader_store_id);

    KeyLocation locateKey(Backoffer & bo, const std::string & key);

    void dropRegion(const RegionVerID &);

    void dropStore(uint64_t failed_store_id);

    void onSendReqFail(RPCContextPtr & ctx, const Exception & exc);

    void onRegionStale(Backoffer & bo, RPCContextPtr ctx, const errorpb::EpochNotMatch & epoch_not_match);

    RegionPtr getRegionByID(Backoffer & bo, const RegionVerID & id);

    Store getStore(Backoffer & bo, uint64_t id);

    std::pair<std::unordered_map<RegionVerID, std::vector<std::string>>, RegionVerID> groupKeysByRegion(
        Backoffer & bo, const std::vector<std::string> & keys);

private:
    RegionPtr loadRegionByKey(Backoffer & bo, const std::string & key);

    RegionPtr getRegionByIDFromCache(const RegionVerID & region_id);

    RegionPtr loadRegionByID(Backoffer & bo, uint64_t region_id);

    metapb::Store loadStore(Backoffer & bo, uint64_t id);

    Store reloadStore(Backoffer & bo, uint64_t id);

    RegionPtr searchCachedRegion(const std::string & key);

    std::vector<metapb::Peer> selectLearner(Backoffer & bo, const metapb::Region & meta);

    void insertRegionToCache(RegionPtr region);

    std::map<std::string, RegionPtr> regions_map;

    std::unordered_map<RegionVerID, RegionPtr> regions;

    std::map<uint64_t, Store> stores;

    pd::ClientPtr pd_client;

    std::shared_mutex region_mutex;

    std::mutex store_mutex;

    const std::string learner_key;

    const std::string learner_value;

    Logger * log;
};

using RegionCachePtr = std::unique_ptr<RegionCache>;

} // namespace kv
} // namespace pingcap
