#include <pingcap/Exception.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Scanner.h>
#include <pingcap/kv/Snapshot.h>
#include <spdlog/spdlog.h>

namespace pingcap
{
namespace kv
{

constexpr int scan_batch_size = 256;

//bool extractLockFromKeyErr()

std::string Snapshot::Get(const std::string & key)
{
    Backoffer bo(GetMaxBackoff);
    return Get(bo, key);
}

std::string Snapshot::Get(Backoffer & bo, const std::string & key)
{
    for (;;)
    {
        auto request = std::make_shared<kvrpcpb::GetRequest>();
        request->set_key(key);
        request->set_version(version);

        auto context = request->mutable_context();
        context->set_priority(::kvrpcpb::Normal);
        context->set_not_fill_cache(false);
        for (auto ts : cluster->min_commit_ts_pushed)
        {
            spdlog::info("snapshot GetRequest add ts: " + std::to_string(ts));
//            context->add_resolved_locks(ts);
        }

        if (context->resolved_locks().empty())
        {
            spdlog::info("resolved locks is empty");
        }
        else
        {
            for (auto & ts : context->resolved_locks())
            {
                spdlog::info("ts in cluster: " + std::to_string(ts));
            }
            spdlog::info("resolved locks is not empty");
        }
        auto location = cluster->region_cache->locateKey(bo, key);
        auto region_client = RegionClient(cluster, location.region);

        std::shared_ptr<kvrpcpb::GetResponse> response;
        try
        {
            response = region_client.sendReqToRegion(bo, request);
        }
        catch (Exception & e)
        {
            spdlog::info("Snapshot Get Request meet exception");
            bo.backoff(boRegionMiss, e);
            continue;
        }
        if (response->has_error())
        {
            auto lock = extractLockFromKeyErr(response->error());
            spdlog::info("Snapshot response has error " + getLockInfo(lock));
            std::vector<LockPtr> locks{lock};
            std::vector<uint64_t> pushed;
            auto before_expired = cluster->lock_resolver->ResolveLocks(bo, version, locks, pushed);
            spdlog::info("Snapshot finish resolve lock. pushed size " + std::to_string(pushed.size()));
            for (auto & ele : pushed)
            {
                spdlog::info("Snapshot pushed ts: " + std::to_string(ele));
            }
            if (!pushed.empty())
            {
                cluster->min_commit_ts_pushed.insert(pushed.begin(), pushed.end());
            }
            if (before_expired > 0)
            {
                bo.backoffWithMaxSleep(
                    boTxnLockFast, before_expired, Exception("key error : " + response->error().ShortDebugString(), LockError));
            }
            continue;
        }
        return response->value();
    }
}

Scanner Snapshot::Scan(const std::string & begin, const std::string & end) { return Scanner(*this, begin, end, scan_batch_size); }

} // namespace kv
} // namespace pingcap
