/*
 * predixy - A high performance and full features proxy for redis.
 * Copyright (C) 2017 Joyield, Inc. <joyield.com@gmail.com>
 * All rights reserved.
 */

#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <stdlib.h>
#include <sys/types.h>
#include <iostream>
#include <thread>
#include "Proxy.h"
#include "Handler.h"
#include "Socket.h"
#include "Alloc.h"
#include "ListenSocket.h"
#include "AcceptSocket.h"
#include "RequestParser.h"
#include "Backtrace.h"

static bool Running = false;
static bool Abort = false;
static bool Stop = false;

static void abortHandler(int sig)
{
    if (!Abort) {
        traceInfo(sig);
    }
    Abort = true;
    if (!Running) {
        abort();
    }
}

static void stopHandler(int sig)
{
    Stop = true;
    if (!Running) {
        abort();
    }
}

Proxy::Proxy():
    mListener(nullptr),
    mDataCenter(nullptr),
    mStartTime(time(nullptr)),
    mStatsVer(0)
{
}

Proxy::~Proxy()
{
    for (auto h : mHandlers) {
        delete h;
    }
    mServPools.clear();

    if (mAuxiliary != nullptr) {
        delete mAuxiliary;
    }

    delete mDataCenter;
    delete mListener;
}

bool Proxy::init(int argc, char* argv[])
{
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGFPE, abortHandler);
    signal(SIGILL, abortHandler);
    signal(SIGSEGV, abortHandler);
    signal(SIGABRT, abortHandler);
    signal(SIGBUS, abortHandler);
    signal(SIGQUIT, abortHandler);
    signal(SIGINT, stopHandler);
    signal(SIGTERM, stopHandler);

    for (int i = 0; i < argc; i++) {
        mArgs.push_back(argv[i]);
    }

    mRouteClusters = std::make_shared<std::vector<RouteCluster>>();
    mConf = std::make_shared<Conf>();

    Command::init();
    
    if (!mConf->init(argc, argv)) {
        return false;
    }
    Logger::gInst = new Logger();
    Logger::gInst->setLogFile(mConf->log(), mConf->logRotateSecs(), mConf->logRotateBytes());
    Logger::gInst->setAllowMissLog(mConf->allowMissLog());
    for (int i = 0; i < LogLevel::Sentinel; ++i) {
        LogLevel::Type lvl = LogLevel::Type(i);
        Logger::gInst->setLogSample(lvl, mConf->logSample(lvl));
    }
    Logger::gInst->start();
    for (auto& ac : mConf->authConfs()) {
        mAuthority.add(ac);
    }
    if (!mConf->localDC().empty()) {
        mDataCenter = new DataCenter();
        mDataCenter->init(mConf);
    }
    AllocBase::setMaxMemory(mConf->maxMemory());
    if (mConf->bufSize() > 0) {
        Buffer::setSize(mConf->bufSize());
    }

    mLatencyMonitorSet.init(mConf->latencyMonitors());
    ListenSocket* s = new ListenSocket(mConf->bind(), SOCK_STREAM);
    if (!s->setNonBlock()) {
        logError("proxy listener set nonblock fail:%s", StrError());
        Throw(InitFail, "listener set nonblock", StrError());
    }
    s->listen();
    mListener = s;
    logNotice("predixy listen in %s", mConf->bind());

    switch (mConf->serverPoolType()) {
    case ServerPool::Cluster:
        {
            for (auto &pool : mConf->clusterServerPools()) {
                auto p = std::make_shared<ClusterServerPool>(this, pool.name);
                p->init(pool);
                mServPools.push_back(p);
            }
            initRoutes(mConf, mRouteClusters.get());
        }
        break;
    case ServerPool::Standalone:
        {
            auto p = std::make_shared<StandaloneServerPool>(this);
            p->init(mConf->standaloneServerPool());
            mServPools.push_back(p);
        }
        break;
    default:
        Throw(InitFail, "unknown server pool type");
        break;
    }
    
    // start handlers
    for (int i = 0; i < mConf->workerThreads(); ++i) {
        Handler* h = new Handler(this);
        mHandlers.push_back(h);
    }
    return true;
}



void Proxy::initRoutes(std::shared_ptr<Conf>& conf, std::vector<RouteCluster>* routeClusters) {
    if (conf->serverPoolType() == ServerPool::Cluster) {
        for (auto& route : conf->routes().routes) {
            RouteCluster rCluster;
            rCluster.prefixKey = route.prefixKey;
            for (auto serv : mServPools) {
                auto dsrv = std::dynamic_pointer_cast<ClusterServerPool>(serv);
                if (dsrv->name() == route.cluster) {
                    rCluster.cluster = serv;
                }
                if (dsrv->name() == route.read.cluster) {
                    rCluster.read_cluster = serv;
                }
            }
            routeClusters->push_back(rCluster);
        }
    }
}



int Proxy::run()
{
    logNotice("predixy running with Name:%s Workers:%d",
            mConf->name(),
            (int)mHandlers.size());
    std::vector<std::shared_ptr<std::thread>> tasks;
    for (auto h : mHandlers) {
        std::shared_ptr<std::thread> t(new std::thread([=](){h->run();}));
        tasks.push_back(t);
    }

    if (mConf->serverPoolType() == ServerPool::Cluster) {
        mAuxiliary = new std::thread([=]() {
            while (!Abort && !Stop) {
                sleep(1);
                std::shared_ptr<Conf> conf;
                {
                    std::lock_guard<std::mutex> lg(mConfGuard);
                    conf = mConf;
                }
                if (conf->updated()) {
                    this->updateConfig();
                }
            }
        });
    }

    Running = true;
    bool stop = false;
    while (!stop) {
        if (Abort) {
            stop = true;
            abort();
        } else if (Stop) {
            fprintf(stderr, "predixy will quit ASAP Bye!\n");
            stop = true;
        }
        if (!stop) {
            sleep(1);
            TimerPoint::report();
        }
    }
    for (auto h : mHandlers) {
        h->stop();
    }
    for (auto t : tasks) {
        t->join();
    }
    if (mAuxiliary != nullptr) {
        mAuxiliary->join();
    }

    Logger::gInst->stop();
    TimerPoint::report();
    if (*mConf->bind() == '/') {
        unlink(mConf->bind());
    }
    return 0;
}

ServerPool* Proxy::serverPool(Request* req, const String& key) 
{
    auto c = req->connection();
    if (c) {
        // has already attached a server connection
        if (auto s = c->connectConnection()) {
            return s->server()->pool();
        }
    }

    std::shared_ptr<std::vector<RouteCluster>> routeClusters;
    {
        std::lock_guard<std::mutex> lg(mRouteClustersGuard);
        routeClusters = mRouteClusters;
    }

    if (mServPools.size() == 0 ) {
        return nullptr;
    } 
    if (mServPools.size() == 0 || key.length() == 0) {
        return mServPools[0].get();
    }
    for (auto& cc : (*routeClusters)) {
        if (cc.prefixKey.length() == 0 || key.hasPrefix(cc.prefixKey)) {
            if (req->requireWrite()) {
                return cc.cluster.get();
            }
            if (cc.read_cluster) {
                return cc.read_cluster.get();
            }
            return cc.cluster.get();
        }
    }
    return mServPools[0].get();
}

void Proxy::updateConfig() {
    logNotice("update config begin");

    std::shared_ptr<Conf> newConf = std::make_shared<Conf>();
    newConf->init(mArgs.size(), mArgs.data());

    auto routeClusters = std::make_shared<std::vector<RouteCluster>>();

    initRoutes(newConf, routeClusters.get());

    // don't update mConf
    {   // just updating routes
        std::lock_guard<std::mutex> lg(mRouteClustersGuard);
        mRouteClusters = routeClusters;
    }
    logNotice("update config end");
}
