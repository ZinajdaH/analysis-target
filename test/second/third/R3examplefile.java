


            KeeperException ke = request.getException();
            if (ke instanceof SessionMovedException) {
                throw ke;
            }
            if (ke != null && request.type != OpCode.multi) {
                throw ke;
            }

            LOG.debug("{}", request);

            if (request.isStale()) {
                ServerMetrics.getMetrics().STALE_REPLIES.add(1);
            }

            if (request.isThrottled()) {
              throw KeeperException.create(Code.THROTTLEDOP);
            }

            AuditHelper.addAuditLog(request, rc);

            switch (request.type) {
            case OpCode.ping: {
                lastOp = "PING";
                updateStats(request, lastOp, lastZxid);

                responseSize = cnxn.sendResponse(new ReplyHeader(ClientCnxn.PING_XID, lastZxid, 0), null, "response");
                return;
            }
            case OpCode.createSession: {
                lastOp = "SESS";
                updateStats(request, lastOp, lastZxid);

                zks.finishSessionInit(request.cnxn, true);
                return;
            }
            case OpCode.multi: {
                lastOp = "MULT";
                rsp = new MultiResponse();

                for (ProcessTxnResult subTxnResult : rc.multiResult) {

                    OpResult subResult;

                    switch (subTxnResult.type) {
                    case OpCode.check:
                        subResult = new CheckResult();
                        break;
                    case OpCode.create:
                        subResult = new CreateResult(subTxnResult.path);
                        break;
                    case OpCode.create2:
                    case OpCode.createTTL:
                    case OpCode.createContainer:
                        subResult = new CreateResult(subTxnResult.path, subTxnResult.stat);
                        break;
                    case OpCode.delete:
                    case OpCode.deleteContainer:
                        subResult = new DeleteResult();
                        break;
                    case OpCode.setData:
                        subResult = new SetDataResult(subTxnResult.stat);
                        break;
                    case OpCode.error:
                        subResult = new ErrorResult(subTxnResult.err);
                        if (subTxnResult.err == Code.SESSIONMOVED.intValue()) {
                            throw new SessionMovedException();
                        }
                        break;
                    default:
                        throw new IOException("Invalid type of op");
                    }

                    ((MultiResponse) rsp).add(subResult);
                }

                break;
            }
            case OpCode.multiRead: {
                lastOp = "MLTR";
                MultiOperationRecord multiReadRecord = new MultiOperationRecord();
                ByteBufferInputStream.byteBuffer2Record(request.request, multiReadRecord);
                rsp = new MultiResponse();
                OpResult subResult;
                for (Op readOp : multiReadRecord) {
                    try {
                        Record rec;
                        switch (readOp.getType()) {
                        case OpCode.getChildren:
                            rec = handleGetChildrenRequest(readOp.toRequestRecord(), cnxn, request.authInfo);
                            subResult = new GetChildrenResult(((GetChildrenResponse) rec).getChildren());
                            break;
                        case OpCode.getData:
                            rec = handleGetDataRequest(readOp.toRequestRecord(), cnxn, request.authInfo);
                            GetDataResponse gdr = (GetDataResponse) rec;
                            subResult = new GetDataResult(gdr.getData(), gdr.getStat());
                            break;
                        default:
                            throw new IOException("Invalid type of readOp");
                        }
                    } catch (KeeperException e) {
                        subResult = new ErrorResult(e.code().intValue());
                    }
                    ((MultiResponse) rsp).add(subResult);
                }
                break;
            }
            case OpCode.create: {
                lastOp = "CREA";
                rsp = new CreateResponse(rc.path);
                err = Code.get(rc.err);
                requestPathMetricsCollector.registerRequest(request.type, rc.path);
                break;
            }
            case OpCode.create2:
            case OpCode.createTTL:
            case OpCode.createContainer: {
                lastOp = "CREA";
                rsp = new Create2Response(rc.path, rc.stat);
                err = Code.get(rc.err);
                requestPathMetricsCollector.registerRequest(request.type, rc.path);
                break;
            }
            case OpCode.delete:
            case OpCode.deleteContainer: {
                lastOp = "DELE";
                err = Code.get(rc.err);
                requestPathMetricsCollector.registerRequest(request.type, rc.path);
                break;
            }
            case OpCode.setData: {
                lastOp = "SETD";
                rsp = new SetDataResponse(rc.stat);
                err = Code.get(rc.err);
                requestPathMetricsCollector.registerRequest(request.type, rc.path);
                break;
            }
            case OpCode.reconfig: {
                lastOp = "RECO";
                rsp = new GetDataResponse(
                    ((QuorumZooKeeperServer) zks).self.getQuorumVerifier().toString().getBytes(),
                    rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.setACL: {
                lastOp = "SETA";
                rsp = new SetACLResponse(rc.stat);
                err = Code.get(rc.err);
                requestPathMetricsCollector.registerRequest(request.type, rc.path);
                break;
            }
            case OpCode.closeSession: {
                lastOp = "CLOS";
                err = Code.get(rc.err);
                break;
            }
            case OpCode.sync: {
                lastOp = "SYNC";
                SyncRequest syncRequest = new SyncRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request, syncRequest);
                rsp = new SyncResponse(syncRequest.getPath());
                requestPathMetricsCollector.registerRequest(request.type, syncRequest.getPath());
                break;
            }
            case OpCode.check: {
                lastOp = "CHEC";
                rsp = new SetDataResponse(rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.exists: {
                lastOp = "EXIS";
                // TODO we need to figure out the security requirement for this!
                ExistsRequest existsRequest = new ExistsRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request, existsRequest);
                path = existsRequest.getPath();
                if (path.indexOf('\0') != -1) {
                    throw new KeeperException.BadArgumentsException();
                }
                Stat stat = zks.getZKDatabase().statNode(path, existsRequest.getWatch() ? cnxn : null);
                rsp = new ExistsResponse(stat);
                requestPathMetricsCollector.registerRequest(request.type, path);
                break;
            }
            case OpCode.getData: {
                lastOp = "GETD";
                GetDataRequest getDataRequest = new GetDataRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request, getDataRequest);
                path = getDataRequest.getPath();
                rsp = handleGetDataRequest(getDataRequest, cnxn, request.authInfo);
                requestPathMetricsCollector.registerRequest(request.type, path);
                break;
            }
            case OpCode.setWatches: {
                lastOp = "SETW";
                SetWatches setWatches = new SetWatches();
                // TODO we really should not need this
                request.request.rewind();
                ByteBufferInputStream.byteBuffer2Record(request.request, setWatches);
                long relativeZxid = setWatches.getRelativeZxid();
                zks.getZKDatabase()
                   .setWatches(
                       relativeZxid,
                       setWatches.getDataWatches(),
                       setWatches.getExistWatches(),
                       setWatches.getChildWatches(),
                       Collections.emptyList(),
                       Collections.emptyList(),
                       cnxn);
                break;
            }
            case OpCode.setWatches2: {
                lastOp = "STW2";
                SetWatches2 setWatches = new SetWatches2();
                // TODO we really should not need this
                request.request.rewind();
                ByteBufferInputStream.byteBuffer2Record(request.request, setWatches);
                long relativeZxid = setWatches.getRelativeZxid();
                zks.getZKDatabase().setWatches(relativeZxid,
                        setWatches.getDataWatches(),
                        setWatches.getExistWatches(),
                        setWatches.getChildWatches(),
                        setWatches.getPersistentWatches(),
                        setWatches.getPersistentRecursiveWatches(),
                        cnxn);
                break;
            }
            case OpCode.addWatch: {
                lastOp = "ADDW";
                AddWatchRequest addWatcherRequest = new AddWatchRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request,
                        addWatcherRequest);
                zks.getZKDatabase().addWatch(addWatcherRequest.getPath(), cnxn, addWatcherRequest.getMode());
                rsp = new ErrorResponse(0);
                break;
            }
            case OpCode.getACL: {
                lastOp = "GETA";
                GetACLRequest getACLRequest = new GetACLRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request, getACLRequest);
                path = getACLRequest.getPath();
                DataNode n = zks.getZKDatabase().getNode(path);
                if (n == null) {
                    throw new KeeperException.NoNodeException();
                }
                zks.checkACL(
                    request.cnxn,
                    zks.getZKDatabase().aclForNode(n),
                    ZooDefs.Perms.READ | ZooDefs.Perms.ADMIN, request.authInfo, path,
                    null);

                Stat stat = new Stat();
                List<ACL> acl = zks.getZKDatabase().getACL(path, stat);
                requestPathMetricsCollector.registerRequest(request.type, getACLRequest.getPath());

                try {
                    zks.checkACL(
                        request.cnxn,
                        zks.getZKDatabase().aclForNode(n),
                        ZooDefs.Perms.ADMIN,
                        request.authInfo,
                        path,
                        null);
                    rsp = new GetACLResponse(acl, stat);
                } catch (KeeperException.NoAuthException e) {
                    List<ACL> acl1 = new ArrayList<ACL>(acl.size());
                    for (ACL a : acl) {
                        if ("digest".equals(a.getId().getScheme())) {
                            Id id = a.getId();
                            Id id1 = new Id(id.getScheme(), id.getId().replaceAll(":.*", ":x"));
                            acl1.add(new ACL(a.getPerms(), id1));
                        } else {
                            acl1.add(a);
                        }
                    }
                    rsp = new GetACLResponse(acl1, stat);
                }
                break;
            }
            case OpCode.getChildren: {
                lastOp = "GETC";
                GetChildrenRequest getChildrenRequest = new GetChildrenRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request, getChildrenRequest);
                path = getChildrenRequest.getPath();
                rsp = handleGetChildrenRequest(getChildrenRequest, cnxn, request.authInfo);
                requestPathMetricsCollector.registerRequest(request.type, path);
                break;
            }
            case OpCode.getAllChildrenNumber: {
                lastOp = "GETACN";
                GetAllChildrenNumberRequest getAllChildrenNumberRequest = new GetAllChildrenNumberRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request, getAllChildrenNumberRequest);
                path = getAllChildrenNumberRequest.getPath();
                DataNode n = zks.getZKDatabase().getNode(path);
                if (n == null) {
                    throw new KeeperException.NoNodeException();
                }
                zks.checkACL(
                    request.cnxn,
                    zks.getZKDatabase().aclForNode(n),
                    ZooDefs.Perms.READ,
                    request.authInfo,
                    path,
                    null);
                int number = zks.getZKDatabase().getAllChildrenNumber(path);
                rsp = new GetAllChildrenNumberResponse(number);
                break;
            }
            case OpCode.getChildren2: {
                lastOp = "GETC";
                GetChildren2Request getChildren2Request = new GetChildren2Request();
                ByteBufferInputStream.byteBuffer2Record(request.request, getChildren2Request);
                Stat stat = new Stat();
                path = getChildren2Request.getPath();
                DataNode n = zks.getZKDatabase().getNode(path);
                if (n == null) {
                    throw new KeeperException.NoNodeException();
                }
                zks.checkACL(
                    request.cnxn,
                    zks.getZKDatabase().aclForNode(n),
                    ZooDefs.Perms.READ,
                    request.authInfo, path,
                    null);
                List<String> children = zks.getZKDatabase()
                                           .getChildren(path, stat, getChildren2Request.getWatch() ? cnxn : null);
                rsp = new GetChildren2Response(children, stat);
                requestPathMetricsCollector.registerRequest(request.type, path);
                break;
            }
            case OpCode.checkWatches: {
                lastOp = "CHKW";
                CheckWatchesRequest checkWatches = new CheckWatchesRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request, checkWatches);
                WatcherType type = WatcherType.fromInt(checkWatches.getType());
                path = checkWatches.getPath();
                boolean containsWatcher = zks.getZKDatabase().containsWatcher(path, type, cnxn);
                if (!containsWatcher) {
                    String msg = String.format(Locale.ENGLISH, "%s (type: %s)", path, type);
                    throw new KeeperException.NoWatcherException(msg);
                }
                requestPathMetricsCollector.registerRequest(request.type, checkWatches.getPath());
                break;
            }
            case OpCode.removeWatches: {
                lastOp = "REMW";
                RemoveWatchesRequest removeWatches = new RemoveWatchesRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request, removeWatches);
                WatcherType type = WatcherType.fromInt(removeWatches.getType());
                path = removeWatches.getPath();
                boolean removed = zks.getZKDatabase().removeWatch(path, type, cnxn);
                if (!removed) {
                    String msg = String.format(Locale.ENGLISH, "%s (type: %s)", path, type);
                    throw new KeeperException.NoWatcherException(msg);
                }
                requestPathMetricsCollector.registerRequest(request.type, removeWatches.getPath());
                break;
            }
            case OpCode.getEphemerals: {
                lastOp = "GETE";
                GetEphemeralsRequest getEphemerals = new GetEphemeralsRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request, getEphemerals);
                String prefixPath = getEphemerals.getPrefixPath();
                Set<String> allEphems = zks.getZKDatabase().getDataTree().getEphemerals(request.sessionId);
                List<String> ephemerals = new ArrayList<>();
                if (StringUtils.isBlank(prefixPath) || "/".equals(prefixPath.trim())) {
                    ephemerals.addAll(allEphems);
                } else {
                    for (String p : allEphems) {
                        if (p.startsWith(prefixPath)) {
                            ephemerals.add(p);
                        }
                    }
                }
                rsp = new GetEphemeralsResponse(ephemerals);
                break;
            }
            }
        } catch (SessionMovedException e) {
            // session moved is a connection level error, we need to tear
            // down the connection otw ZOOKEEPER-710 might happen
            // ie client on slow follower starts to renew session, fails
            // before this completes, then tries the fast follower (leader)
            // and is successful, however the initial renew is then
            // successfully fwd/processed by the leader and as a result
            // the client and leader disagree on where the client is most
            // recently attached (and therefore invalid SESSION MOVED generated)
            cnxn.sendCloseSession();
            return;
        } catch (KeeperException e) {
            err = e.code();
        } catch (Exception e) {
            // log at error level as we are returning a marshalling
            // error to the user
            LOG.error("Failed to process {}", request, e);
            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            bb.rewind();
            while (bb.hasRemaining()) {
                sb.append(Integer.toHexString(bb.get() & 0xff));
            }
            LOG.error("Dumping request buffer: 0x{}", sb.toString());
            err = Code.MARSHALLINGERROR;
        }

        ReplyHeader hdr = new ReplyHeader(request.cxid, lastZxid, err.intValue());

        updateStats(request, lastOp, lastZxid);

        try {
            if (path == null || rsp == null) {
                responseSize = cnxn.sendResponse(hdr, rsp, "response");
            } else {
                int opCode = request.type;
                Stat stat = null;
                // Serialized read and get children responses could be cached by the connection
                // object. Cache entries are identified by their path and last modified zxid,
                // so these values are passed along with the response.
                switch (opCode) {
                    case OpCode.getData : {
                        GetDataResponse getDataResponse = (GetDataResponse) rsp;
                        stat = getDataResponse.getStat();
                        responseSize = cnxn.sendResponse(hdr, rsp, "response", path, stat, opCode);
                        break;
                    }
                    case OpCode.getChildren2 : {
                        GetChildren2Response getChildren2Response = (GetChildren2Response) rsp;
                        stat = getChildren2Response.getStat();
                        responseSize = cnxn.sendResponse(hdr, rsp, "response", path, stat, opCode);
                        break;
                    }
                    default:
                        responseSize = cnxn.sendResponse(hdr, rsp, "response");
                }
            }

            if (request.type == OpCode.closeSession) {
                cnxn.sendCloseSession();
            }
        } catch (IOException e) {
            LOG.error("FIXMSG", e);
        } finally {
            ServerMetrics.getMetrics().RESPONSE_BYTES.add(responseSize);
        }
    }

    private Record handleGetChildrenRequest(Record request, ServerCnxn cnxn, List<Id> authInfo) throws KeeperException, IOException {
        GetChildrenRequest getChildrenRequest = (GetChildrenRequest) request;
        String path = getChildrenRequest.getPath();
        DataNode n = zks.getZKDatabase().getNode(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        zks.checkACL(cnxn, zks.getZKDatabase().aclForNode(n), ZooDefs.Perms.READ, authInfo, path, null);
        List<String> children = zks.getZKDatabase()
                                   .getChildren(path, null, getChildrenRequest.getWatch() ? cnxn : null);
        return new GetChildrenResponse(children);
    }

    private Record handleGetDataRequest(Record request, ServerCnxn cnxn, List<Id> authInfo) throws KeeperException, IOException {
        GetDataRequest getDataRequest = (GetDataRequest) request;
        String path = getDataRequest.getPath();
        DataNode n = zks.getZKDatabase().getNode(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        zks.checkACL(cnxn, zks.getZKDatabase().aclForNode(n), ZooDefs.Perms.READ, authInfo, path, null);
        Stat stat = new Stat();
        byte[] b = zks.getZKDatabase().getData(path, stat, getDataRequest.getWatch() ? cnxn : null);
        return new GetDataResponse(b, stat);
    }

    private boolean closeSession(ServerCnxnFactory serverCnxnFactory, long sessionId) {
        if (serverCnxnFactory == null) {
            return false;
        }
        return serverCnxnFactory.closeSession(sessionId, ServerCnxn.DisconnectReason.CLIENT_CLOSED_SESSION);
    }

    private boolean connClosedByClient(Request request) {
        return request.cnxn == null;
    }

    public void shutdown() {
        // we are the final link in the chain
        LOG.info("shutdown of request processor complete");
    }

    private void updateStats(Request request, String lastOp, long lastZxid) {
        if (request.cnxn == null) {
            return;
        }
        long currentTime = Time.currentElapsedTime();
        zks.serverStats().updateLatency(request, currentTime);
        request.cnxn.updateStatsForResponse(request.cxid, lastZxid, lastOp, request.createTime, currentTime);
    }
   public void parseProperties(Properties zkProp) throws IOException, ConfigException {
        int clientPort = 0;
        int secureClientPort = 0;
        int observerMasterPort = 0;
        String clientPortAddress = null;
        String secureClientPortAddress = null;
        VerifyingFileFactory vff = new VerifyingFileFactory.Builder(LOG).warnForRelativePath().build();
        for (Entry<Object, Object> entry : zkProp.entrySet()) {
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();
            if (key.equals("dataDir")) {
                dataDir = vff.create(value);
            } else if (key.equals("dataLogDir")) {
                dataLogDir = vff.create(value);
            } else if (key.equals("clientPort")) {
                clientPort = Integer.parseInt(value);
            } else if (key.equals("localSessionsEnabled")) {
                localSessionsEnabled = parseBoolean(key, value);
            } else if (key.equals("localSessionsUpgradingEnabled")) {
                localSessionsUpgradingEnabled = parseBoolean(key, value);
            } else if (key.equals("clientPortAddress")) {
                clientPortAddress = value.trim();
            } else if (key.equals("secureClientPort")) {
                secureClientPort = Integer.parseInt(value);
            } else if (key.equals("secureClientPortAddress")) {
                secureClientPortAddress = value.trim();
            } else if (key.equals("observerMasterPort")) {
                observerMasterPort = Integer.parseInt(value);
            } else if (key.equals("clientPortListenBacklog")) {
                clientPortListenBacklog = Integer.parseInt(value);
            } else if (key.equals("tickTime")) {
                tickTime = Integer.parseInt(value);
            } else if (key.equals("maxClientCnxns")) {
                maxClientCnxns = Integer.parseInt(value);
            } else if (key.equals("minSessionTimeout")) {
                minSessionTimeout = Integer.parseInt(value);
            } else if (key.equals("maxSessionTimeout")) {
                maxSessionTimeout = Integer.parseInt(value);
            } else if (key.equals("initLimit")) {
                initLimit = Integer.parseInt(value);
            } else if (key.equals("syncLimit")) {
                syncLimit = Integer.parseInt(value);
            } else if (key.equals("connectToLearnerMasterLimit")) {
                connectToLearnerMasterLimit = Integer.parseInt(value);
            } else if (key.equals("electionAlg")) {
                electionAlg = Integer.parseInt(value);
                if (electionAlg != 3) {
                    throw new ConfigException("Invalid electionAlg value. Only 3 is supported.");
                }
            } else if (key.equals("quorumListenOnAllIPs")) {
                quorumListenOnAllIPs = parseBoolean(key, value);
            } else if (key.equals("peerType")) {
                if (value.toLowerCase().equals("observer")) {
                    peerType = LearnerType.OBSERVER;
                } else if (value.toLowerCase().equals("participant")) {
                    peerType = LearnerType.PARTICIPANT;
                } else {
                    throw new ConfigException("Unrecognised peertype: " + value);
                }
            } else if (key.equals("syncEnabled")) {
                syncEnabled = parseBoolean(key, value);
            } else if (key.equals("dynamicConfigFile")) {
                dynamicConfigFileStr = value;
            } else if (key.equals("autopurge.snapRetainCount")) {
                snapRetainCount = Integer.parseInt(value);
            } else if (key.equals("autopurge.purgeInterval")) {
                purgeInterval = Integer.parseInt(value);
            } else if (key.equals("standaloneEnabled")) {
                setStandaloneEnabled(parseBoolean(key, value));
            } else if (key.equals("reconfigEnabled")) {
                setReconfigEnabled(parseBoolean(key, value));
            } else if (key.equals("sslQuorum")) {
                sslQuorum = parseBoolean(key, value);
            } else if (key.equals("portUnification")) {
                shouldUsePortUnification = parseBoolean(key, value);
            } else if (key.equals("sslQuorumReloadCertFiles")) {
                sslQuorumReloadCertFiles = parseBoolean(key, value);
            } else if ((key.startsWith("server.") || key.startsWith("group") || key.startsWith("weight"))
                       && zkProp.containsKey("dynamicConfigFile")) {
                throw new ConfigException("parameter: " + key + " must be in a separate dynamic config file");
            } else if (key.equals(QuorumAuth.QUORUM_SASL_AUTH_ENABLED)) {
                quorumEnableSasl = parseBoolean(key, value);
            } else if (key.equals(QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED)) {
                quorumServerRequireSasl = parseBoolean(key, value);
            } else if (key.equals(QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED)) {
                quorumLearnerRequireSasl = parseBoolean(key, value);
            } else if (key.equals(QuorumAuth.QUORUM_LEARNER_SASL_LOGIN_CONTEXT)) {
                quorumLearnerLoginContext = value;
            } else if (key.equals(QuorumAuth.QUORUM_SERVER_SASL_LOGIN_CONTEXT)) {
                quorumServerLoginContext = value;
            } else if (key.equals(QuorumAuth.QUORUM_KERBEROS_SERVICE_PRINCIPAL)) {
                quorumServicePrincipal = value;
            } else if (key.equals("quorum.cnxn.threads.size")) {
                quorumCnxnThreadsSize = Integer.parseInt(value);
            } else if (key.equals(JvmPauseMonitor.INFO_THRESHOLD_KEY)) {
                jvmPauseInfoThresholdMs = Long.parseLong(value);
            } else if (key.equals(JvmPauseMonitor.WARN_THRESHOLD_KEY)) {
                jvmPauseWarnThresholdMs = Long.parseLong(value);
            } else if (key.equals(JvmPauseMonitor.SLEEP_TIME_MS_KEY)) {
                jvmPauseSleepTimeMs = Long.parseLong(value);
            } else if (key.equals(JvmPauseMonitor.JVM_PAUSE_MONITOR_FEATURE_SWITCH_KEY)) {
                jvmPauseMonitorToRun = parseBoolean(key, value);
            } else if (key.equals("metricsProvider.className")) {
                metricsProviderClassName = value;
            } else if (key.startsWith("metricsProvider.")) {
                String keyForMetricsProvider = key.substring(16);
                metricsProviderConfiguration.put(keyForMetricsProvider, value);
            } else if (key.equals("multiAddress.enabled")) {
                multiAddressEnabled = parseBoolean(key, value);
            } else if (key.equals("multiAddress.reachabilityCheckTimeoutMs")) {
                multiAddressReachabilityCheckTimeoutMs = Integer.parseInt(value);
            } else if (key.equals("multiAddress.reachabilityCheckEnabled")) {
                multiAddressReachabilityCheckEnabled = parseBoolean(key, value);
            } else {
                System.setProperty("zookeeper." + key, value);
            }
        }

        if (!quorumEnableSasl && quorumServerRequireSasl) {
            throw new IllegalArgumentException(QuorumAuth.QUORUM_SASL_AUTH_ENABLED
                                               + " is disabled, so cannot enable "
                                               + QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED);
        }
        if (!quorumEnableSasl && quorumLearnerRequireSasl) {
            throw new IllegalArgumentException(QuorumAuth.QUORUM_SASL_AUTH_ENABLED
                                               + " is disabled, so cannot enable "
                                               + QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED);
        }
        // If quorumpeer learner is not auth enabled then self won't be able to
        // join quorum. So this condition is ensuring that the quorumpeer learner
        // is also auth enabled while enabling quorum server require sasl.
        if (!quorumLearnerRequireSasl && quorumServerRequireSasl) {
            throw new IllegalArgumentException(QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED
                                               + " is disabled, so cannot enable "
                                               + QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED);
        }

        // Reset to MIN_SNAP_RETAIN_COUNT if invalid (less than 3)
        // PurgeTxnLog.purge(File, File, int) will not allow to purge less
        // than 3.
        if (snapRetainCount < MIN_SNAP_RETAIN_COUNT) {
            LOG.warn("Invalid autopurge.snapRetainCount: "
                     + snapRetainCount
                     + ". Defaulting to "
                     + MIN_SNAP_RETAIN_COUNT);
            snapRetainCount = MIN_SNAP_RETAIN_COUNT;
        }

        if (dataDir == null) {
            throw new IllegalArgumentException("dataDir is not set");
        }
        if (dataLogDir == null) {
            dataLogDir = dataDir;
        }

        if (clientPort == 0) {
            LOG.info("clientPort is not set");
            if (clientPortAddress != null) {
                throw new IllegalArgumentException("clientPortAddress is set but clientPort is not set");
            }
        } else if (clientPortAddress != null) {
            this.clientPortAddress = new InetSocketAddress(InetAddress.getByName(clientPortAddress), clientPort);
            LOG.info("clientPortAddress is {}", formatInetAddr(this.clientPortAddress));
        } else {
            this.clientPortAddress = new InetSocketAddress(clientPort);
            LOG.info("clientPortAddress is {}", formatInetAddr(this.clientPortAddress));
        }
}
    clock_serv_t cclock;
  mach_timespec_t mts;
  ret = host_get_clock_service(mach_host_self(), SYSTEM_CLOCK, &cclock);
  if (!ret) {
    ret += clock_get_time(cclock, &mts);
    ret += mach_port_deallocate(mach_task_self(), cclock);
    if (!ret) {
      tv->tv_sec = mts.tv_sec;
      tv->tv_usec = mts.tv_nsec / 1000;
    }
  }
  if (ret) {
    // Default to gettimeofday in case of failure.
    ret = gettimeofday(tv, NULL);
  }
#elif defined CLOCK_MONOTONIC_RAW
  // On Linux, CLOCK_MONOTONIC is affected by ntp slew but CLOCK_MONOTONIC_RAW
  // is not.  We want the non-slewed (constant rate) CLOCK_MONOTONIC_RAW if it
  // is available.
  struct timespec ts = { 0 };
  ret = clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  tv->tv_sec = ts.tv_sec;
  tv->tv_usec = ts.tv_nsec / 1000;
#elif _POSIX_MONOTONIC_CLOCK
  struct timespec ts = { 0 };
  ret = clock_gettime(CLOCK_MONOTONIC, &ts);
  tv->tv_sec = ts.tv_sec;
  tv->tv_usec = ts.tv_nsec / 1000;
#elif _WIN32
  LARGE_INTEGER counts, countsPerSecond, countsPerMicrosecond;
  if (QueryPerformanceFrequency(&countsPerSecond) &&
      QueryPerformanceCounter(&counts)) {
    countsPerMicrosecond.QuadPart = countsPerSecond.QuadPart / 1000000;
    tv->tv_sec = (long)(counts.QuadPart / countsPerSecond.QuadPart);
    tv->tv_usec = (long)((counts.QuadPart % countsPerSecond.QuadPart) /
        countsPerMicrosecond.QuadPart);
    ret = 0;
  } else {
    ret = gettimeofday(tv, NULL);
  }
#else
  ret = gettimeofday(tv, NULL);
#endif
  if (ret) {
    abort();
  }
}

const void *zoo_get_context(zhandle_t *zh)
{
    return zh->context;
}

void zoo_set_context(zhandle_t *zh, void *context)
{
    if (zh != NULL) {
        zh->context = context;
    }
}

int zoo_recv_timeout(zhandle_t *zh)
{
    return zh->recv_timeout;
}

/** these functions are thread unsafe, so make sure that
    zoo_lock_auth is called before you access them **/
static auth_info* get_last_auth(auth_list_head_t *auth_list) {
    auth_info *element;
    element = auth_list->auth;
    if (element == NULL) {
        return NULL;
    }
    while (element->next != NULL) {
        element = element->next;
    }
    return element;
}

