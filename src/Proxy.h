/*
 * predixy - A high performance and full features proxy for redis.
 * Copyright (C) 2017 Joyield, Inc. <joyield.com@gmail.com>
 * All rights reserved.
 */

#ifndef _PREDIXY_PROXY_H_
#define _PREDIXY_PROXY_H_

#include <vector>
#include <memory>
#include <mutex>
#include "Predixy.h"
#include "Handler.h"
#include "DC.h"
#include "ServerPool.h"
#include "ClusterServerPool.h"
#include "StandaloneServerPool.h"
#include "LatencyMonitor.h"

class Proxy
{
public:
    DefException(InitFail);
public:
    Proxy();
    ~Proxy();
    bool init(int argc, char* argv[]);
    int run();
    time_t startTime() const
    {
        return mStartTime;
    }
    std::shared_ptr<Conf> conf() 
    {
        std::lock_guard<std::mutex> g(mConfGuard);
        return mConf;
    }
    ListenSocket* listener() const
    {
        return mListener;
    }
    const Authority* authority() const
    {
        return &mAuthority;
    }
    DataCenter* dataCenter() const
    {
        return mDataCenter;
    }
    const std::vector<std::shared_ptr<ServerPool>>& serverPools()  {
        return mServPools;
    }
    const std::shared_ptr<ServerPool> serverPool(size_t i)  {
        return mServPools[i];
    }

    ServerPool* serverPool(Request* req, const String& key);
    bool isSplitMultiKey() const
    {
        return mConf->standaloneServerPool().groups.size() != 1;
    }
    bool supportTransaction() const
    {
        return mConf->standaloneServerPool().groups.size() == 1;
    }
    // bool supportSubscribe() const
    // {
    //     return mConf->standaloneServerPool().groups.size() == 1 ||
    //            mConf->clusterServerPool().servers.size() > 0;
    // }
    const std::vector<Handler*>& handlers() const
    {
        return mHandlers;
    }
    long statsVer() const
    {
        return mStatsVer;
    }
    long incrStatsVer()
    {
        return ++mStatsVer;
    }
    const LatencyMonitorSet& latencyMonitorSet() const
    {
        return mLatencyMonitorSet;
    }
private:
    class RouteCluster {
    public:
        String prefixKey;
        std::shared_ptr<ServerPool> cluster;
        std::shared_ptr<ServerPool> read_cluster;
    };
    void updateConfig();
    void initRoutes(std::shared_ptr<Conf>& conf, std::vector<RouteCluster> *routeClusters);
private:
    std::mutex mConfGuard;
    std::shared_ptr<Conf> mConf;
    std::vector<char*> mArgs;

    ListenSocket* mListener;
    Authority mAuthority;
    DataCenter* mDataCenter;
    std::vector<Handler*> mHandlers;

    std::thread *mAuxiliary = nullptr;

    std::vector<std::shared_ptr<ServerPool>> mServPools;

    std::mutex mRouteClustersGuard;
    std::shared_ptr<std::vector<RouteCluster>> mRouteClusters;

    time_t mStartTime;
    AtomicLong mStatsVer;
    LatencyMonitorSet mLatencyMonitorSet;
};


#endif
