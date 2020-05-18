#include <pingcap/Exception.h>
#include <pingcap/kv/Scanner.h>
#include <pingcap/kv/Snapshot.h>
#include <pingcap/kv/Txn.h>

#include <iostream>
#include <spdlog/spdlog.h>

#include "../test_helper.h"

namespace {

using namespace pingcap;
using namespace pingcap::kv;

class TestWith2PCRealTiKV : public testing::Test {
protected:
    void SetUp() override {
        std::vector<std::string> pd_addrs{"127.0.0.1:2379"};

        test_cluster = createCluster(pd_addrs);
        control_cluster = createCluster(pd_addrs);
    }

    ClusterPtr test_cluster;
    ClusterPtr control_cluster;
};

TEST_F(TestWith2PCRealTiKV, testCommitRollback) {

    // Commit.
    {
        Txn txn(test_cluster.get());
        txn.set("a", "a");
        txn.set("b", "b");
        txn.set("c", "c");
        txn.commit();

        Snapshot snap(test_cluster.get());
        ASSERT_EQ(snap.Get("a"), "a");
        ASSERT_EQ(snap.Get("b"), "b");
        ASSERT_EQ(snap.Get("c"), "c");
    }

    // Write conflict.
    {
        Txn txn1(test_cluster.get());
        txn1.set("a", "a1");
        txn1.set("b", "b1");
        txn1.set("c", "c1");

        Txn txn2(test_cluster.get());
        txn2.set("c", "c2");
        txn2.commit();

        txn1.commit();

        Snapshot snap(test_cluster.get());
        ASSERT_EQ(snap.Get("a"), "a");
        ASSERT_EQ(snap.Get("b"), "b");
        ASSERT_EQ(snap.Get("c"), "c2");
    }
}

TEST_F(TestWith2PCRealTiKV, testPrewriteRollback) {

    // Commit.
    {
        Txn txn(test_cluster.get());
        txn.set("a", "a0");
        txn.set("b", "b0");
        txn.commit();

        Snapshot snap(test_cluster.get());
        ASSERT_EQ(snap.Get("a"), "a0");
        ASSERT_EQ(snap.Get("b"), "b0");
    }
    spdlog::info("commit finished");

    // Prewrite.
    {
        Txn txn1(test_cluster.get());
        txn1.set("a", "a1");
        txn1.set("b", "b1");

        spdlog::info("txn1 start ts: " + std::to_string(txn1.start_ts));

        TestTwoPhaseCommitter committer{&txn1};
        Backoffer prewrite_bo(prewriteMaxBackoff);
        committer.prewriteKeys(prewrite_bo, committer.keys());

        try
        {
            Txn txn2(test_cluster.get());
            spdlog::info("txn2 start ts: " + std::to_string(txn2.start_ts));
            auto result = txn2.get("a");
            ASSERT_EQ(result.second, true);
            ASSERT_EQ(result.first, "a0");
//            test_cluster->min_commit_ts_pushed.clear();
//            spdlog::info("min_commit_ts_pushed cleared\n\n");
//            Snapshot snap1(test_cluster.get());
//            spdlog::info("snapshot 1 start ts: " + std::to_string(snap1.version));
//            Txn txn3(test_cluster.get());
//            spdlog::info("txn3 start ts: " + std::to_string(txn3.start_ts));
//            auto result3 = txn3.get("d");
//            ASSERT_EQ(result3.second, true);
//            ASSERT_EQ(result3.first, "dd0");
//            auto result4 = txn3.get("e");
//            ASSERT_EQ(result4.second, true);
//            ASSERT_EQ(result4.first, "ee0");
            test_cluster->min_commit_ts_pushed.clear();
            spdlog::info("min_commit_ts_pushed cleared\n\n");
//            ASSERT_EQ(snap1.Get("a"), "a0");
//            spdlog::info("get a succed");
//            ASSERT_EQ(snap1.Get("b"), "b0");
        }
        catch (Exception & e)
        {
            std::cout << "\nSnapshot get Failed.\n";
        }

//        try
//        {
//            committer.setCommitTS(test_cluster->pd_client->getTS());
//            Backoffer commit_bo(commitMaxBackoff);
//            committer.commitKeys(commit_bo, committer.keys());
//        }
//        catch (Exception & e)
//        {
//            std::cout << "\nCommit Failed.\n";
//        }
//
//        test_cluster->min_commit_ts_pushed.clear();
//        spdlog::info("min_commit_ts_pushed cleared");
//        Snapshot snap2(test_cluster.get());
//        ASSERT_EQ(snap2.Get("a"), "a1");
//        ASSERT_EQ(snap2.Get("b"), "b1");
    }
}


TEST_F(TestWith2PCRealTiKV, testLargeTxn2) {

    // Commit.
    {
        Txn txn(test_cluster.get());
        txn.set("a", "a0");
        txn.set("b", "b0");
        txn.set("c", "c0");
        txn.commit();

        Snapshot snap(test_cluster.get());
        ASSERT_EQ(snap.Get("a"), "a0");
        ASSERT_EQ(snap.Get("b"), "b0");
        ASSERT_EQ(snap.Get("c"), "c0");
    }

    // Prewrite.
    {
        Txn txn1(test_cluster.get());
        txn1.set("a", "a1");
        txn1.set("b", "b1");

        TestTwoPhaseCommitter committer{&txn1};
        Backoffer prewrite_bo(prewriteMaxBackoff);
        committer.prewriteKeys(prewrite_bo, committer.keys());

        Snapshot snap1(test_cluster.get());
        ASSERT_EQ(snap1.Get("a"), "a0");
        ASSERT_EQ(snap1.Get("b"), "b0");

        try
        {
            committer.setCommitTS(test_cluster->pd_client->getTS());
            Backoffer commit_bo(commitMaxBackoff);
            committer.commitKeys(commit_bo, committer.keys());
        }
        catch (Exception & e)
        {
            std::cout << "\nCommit Failed.\n";
        }

        Snapshot snap2(test_cluster.get());
        ASSERT_EQ(snap2.Get("a"), "a1");
        ASSERT_EQ(snap2.Get("b"), "b1");
    }
}


}
