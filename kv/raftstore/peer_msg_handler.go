package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) applyEntry(e eraftpb.Entry, cb *message.Callback) {
	index := e.Index
	// term := e.Term
	data := e.Data
	var cmdResp *raft_cmdpb.RaftCmdResponse
	log.Debugf("Peer %+v apply entry %+v", d.PeerId(), e)
	if e.EntryType == eraftpb.EntryType_EntryConfChange {
		cc := &eraftpb.ConfChange{}
		err := cc.Unmarshal(data)
		if err != nil {
			panic(err)
		}
		cmdResp = d.applyConfChange(cc)
	} else if data != nil {
		msg := &raft_cmdpb.RaftCmdRequest{}
		err := msg.Unmarshal(data)
		if err != nil {
			panic(err)
		}
		// Note@wy need to check region
		// situation msg1 with region1 append to log, but need some to apply, if msg2 with region1 come to log before applying msg1, it still satisfy region request. When raft apply msg2, msg2's region don't match store's region.
		err = util.CheckRegionEpoch(msg, d.Region(), true) //Todo@wy is it suit for split request? May consider validateSplitRegion()?
		if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
			cmdResp = ErrResp(errEpochNotMatching)
		} else {
			if len(msg.Requests) > 0 {
				cmdResp = d.applyNormalRequest(msg, cb)
			}
			if msg.AdminRequest != nil {
				// Todo@wy handle AdminRequest
				cmdResp = d.applyAdminRequest(msg, cb)
			}
		}
	}
	if d.stopped {
		return
	}
	if cb != nil {
		cb.Done(cmdResp)
	}
	d.peerStorage.applyState.AppliedIndex = index
	engine_util.PutMeta(d.peerStorage.Engines.Kv, meta.ApplyStateKey(d.regionId), d.peerStorage.applyState) // Note@wy modify in kv not in raft
}

func (d *peerMsgHandler) applyNormalRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) *raft_cmdpb.RaftCmdResponse {
	kvWB := &engine_util.WriteBatch{}
	resps := make([]*raft_cmdpb.Response, 0)
	for _, req := range msg.Requests {
		var resp raft_cmdpb.Response
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			kvWB = &engine_util.WriteBatch{}
			val, err := engine_util.GetCF(d.ctx.engine.Kv, req.Get.Cf, req.Get.Key)
			if err != nil {
				log.Debug(err)
			}
			resp = raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Get,
				Get: &raft_cmdpb.GetResponse{
					Value: val,
				},
			}
		case raft_cmdpb.CmdType_Put:
			kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
			log.Debugf("write %+v %+v %+v", req.Put.Cf, req.Put.Key, req.Put.Value)
			resp = raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:     &raft_cmdpb.PutResponse{},
			}
		case raft_cmdpb.CmdType_Delete:
			kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
			resp = raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  &raft_cmdpb.DeleteResponse{},
			}
		case raft_cmdpb.CmdType_Snap:
			kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			kvWB = &engine_util.WriteBatch{}
			resp = raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap: &raft_cmdpb.SnapResponse{
					Region: d.Region(),
				},
			}
			if cb != nil {
				cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			}

		case raft_cmdpb.CmdType_Invalid:

			resp = raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Invalid,
			}
		}
		resps = append(resps, &resp)
	}
	kvWB.WriteToDB(d.peerStorage.Engines.Kv)
	header := raft_cmdpb.RaftResponseHeader{}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:    &header,
		Responses: resps,
	}
	return cmdResp
}

func (d *peerMsgHandler) applyAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) *raft_cmdpb.RaftCmdResponse {
	req := msg.AdminRequest
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
	}
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compactReq := msg.AdminRequest.CompactLog
		compactIndex := compactReq.CompactIndex
		compactTerm := compactReq.CompactTerm
		d.peer.peerStorage.applyState.TruncatedState.Index = compactIndex
		d.peer.peerStorage.applyState.TruncatedState.Term = compactTerm
		err := engine_util.PutMeta(d.peer.peerStorage.Engines.Kv, meta.ApplyStateKey(d.regionId), d.peer.peerStorage.applyState)
		log.Debugf("RegionId %+v peer %+v handle to TruncatedState.Index %+v \n", d.regionId, d.peer.PeerId(), compactIndex)
		if err != nil {
			log.Error(err)
		}
		d.ScheduleCompactLog(compactIndex)
	case raft_cmdpb.AdminCmdType_Split:
		splitReq := req.Split
		// err := d.validateSplitRegion(msg.Header.RegionEpoch, splitReq.SplitKey)
		// if err != nil {
		// 	resp = ErrResp(err)
		// 	return resp
		// }
		err := util.CheckKeyInRegion(splitReq.SplitKey, d.Region())
		if err != nil {
			resp = ErrResp(err)
			return resp
		}
		// Todo@wy change order of processing
		region := d.Region()
		if len(region.Peers) != len(splitReq.NewPeerIds) {
			resp := ErrResp(errors.New("New region's peer number is not equal old region's."))
			log.Warnf("New region's peer number is not equal old region's.")
			return resp
		}
		newPeers := make([]*metapb.Peer, len(region.Peers))
		for i, pr := range region.Peers {
			newPeers[i] = &metapb.Peer{
				Id:      splitReq.NewPeerIds[i],
				StoreId: pr.StoreId,
			}
		}
		newRegion := &metapb.Region{
			Id:       splitReq.NewRegionId,
			StartKey: splitReq.SplitKey,
			EndKey:   region.EndKey,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: InitEpochConfVer,
				Version: InitEpochVer,
			},
			Peers: newPeers,
		}
		region.RegionEpoch.Version += 1
		region.EndKey = splitReq.SplitKey
		log.Infof("Region %+v peer %+v split, old region %+v new Region %+v ", d.regionId, d.PeerId(), region, newRegion)
		newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			panic(err)
		}
		storemeta := d.ctx.storeMeta
		storemeta.Lock()
		storemeta.setRegion(newRegion, newPeer)
		storemeta.regionRanges.ReplaceOrInsert(&regionItem{
			region: region,
		})
		storemeta.regionRanges.ReplaceOrInsert(&regionItem{
			region: newRegion,
		})
		storemeta.Unlock()
		kvWB := &engine_util.WriteBatch{}
		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
		kvWB.WriteToDB(d.ctx.engine.Kv)
		// Note@wy reference to maybeCreatepeer()
		d.SizeDiffHint = 0
		d.ApproximateSize = new(uint64)
		d.ctx.router.register(newPeer)
		_ = d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})
		if d.IsLeader() { // Note@wy accerator to tell scheduler.
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_Split,
			Split: &raft_cmdpb.SplitResponse{
				Regions: []*metapb.Region{region, newRegion},
			},
		}
	}
	return resp
}

func (d *peerMsgHandler) applyConfChange(cc *eraftpb.ConfChange) *raft_cmdpb.RaftCmdResponse {
	req := &raft_cmdpb.RaftCmdRequest{}
	err := req.Unmarshal(cc.Context)
	if err != nil {
		panic(err)
	}
	// Note@wy need to check region
	// situation msg1 with region1 append to log, but need some to apply, if msg2 with region1 come to log before applying msg1, it still satisfy region request. When raft apply msg2, msg2's region don't match store's region.
	err = util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		return ErrResp(errEpochNotMatching)
	}
	d.RaftGroup.ApplyConfChange(*cc)
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		d.addNode(req.AdminRequest.ChangePeer)
	case eraftpb.ConfChangeType_RemoveNode:
		d.removeNode(req.AdminRequest.ChangePeer)
	}
	if d.stopped { //Note@wy if stopped, peerStorage have been destoryed , it is impossible to get d.peerStorage.region and there is not need to generate a resp for callback because leader don't destory itself.
		return nil
	} else {
		if d.IsLeader() { //Note@wy to sync region information.
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}
		return &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{
					Region: d.peerStorage.region,
				},
			},
		}
	}
}

func (d *peerMsgHandler) checkPeer(nodeId uint64) int {
	for i, peer := range d.Region().Peers {
		if peer.Id == nodeId {
			return i
		}
	}
	return -1
}

func (d *peerMsgHandler) addNode(req *raft_cmdpb.ChangePeerRequest) {
	nodeId := req.Peer.Id
	log.Debugf("Peer %+v prepare to add node %+v", d.PeerId(), nodeId)
	// storeId := req.Peer.StoreId
	peerIndex := d.checkPeer(nodeId)
	if peerIndex != -1 {
		log.Debugf("Nodeid %+v is exist in region's peer , don't need to add", nodeId)
		return
	}
	d.peerStorage.region.RegionEpoch.ConfVer += 1
	d.peerStorage.region.Peers = append(d.peerStorage.region.Peers, req.Peer)
	d.insertPeerCache(req.Peer)
	engine_util.PutMeta(d.ctx.engine.Kv, meta.RegionStateKey(d.regionId), &rspb.RegionLocalState{
		State:  rspb.PeerState_Normal,
		Region: d.peerStorage.region,
	})
}

func (d *peerMsgHandler) removeNode(req *raft_cmdpb.ChangePeerRequest) {
	nodeId := req.Peer.Id
	log.Debugf("Peer %+v prepare to remove node %+v", d.PeerId(), nodeId)
	peerIndex := d.checkPeer(nodeId)
	if peerIndex == -1 {
		log.Debugf("Nodeid %+v is not exist in region's peer %+v , don't need to remove", nodeId, d.Region().Peers)
		return
	}
	d.peerStorage.region.RegionEpoch.ConfVer += 1
	d.peerStorage.region.Peers = append(d.peerStorage.region.Peers[:peerIndex], d.peerStorage.region.Peers[peerIndex+1:]...)
	d.removePeerCache(req.Peer.Id)
	engine_util.PutMeta(d.ctx.engine.Kv, meta.RegionStateKey(d.regionId), &rspb.RegionLocalState{
		State:  rspb.PeerState_Normal,
		Region: d.peerStorage.region,
	})
	if nodeId == d.Meta.Id {
		d.destroyPeer()
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}
	rd := d.RaftGroup.Ready()
	log.Debugf("Region %+v peer %+v begin to handle ready:%+v ", d.regionId, d.PeerId(), rd)
	snapRes, err := d.peerStorage.SaveReadyState(&rd)
	if snapRes != nil {
		d.ctx.storeMeta.Lock()
		delete(d.ctx.storeMeta.regions, snapRes.PrevRegion.Id)
		d.ctx.storeMeta.regions[snapRes.Region.Id] = snapRes.Region // Note@wy need to update region infomation and regionId maybe change
		d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: snapRes.PrevRegion})
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: snapRes.Region}) // Note@wy need to update regionRange
		d.ctx.storeMeta.Unlock()                                                          // Note@wy don't use defer, defer will execute before exiting function, may cost dead lock because destory peer need to get the lock of meta.
	}
	if err != nil {
		log.Error(err.Error())
	}
	if len(rd.Messages) > 0 {
		d.Send(d.ctx.trans, rd.Messages)
	}
	if len(rd.CommittedEntries) > 0 {
		for _, ent := range rd.CommittedEntries {
			for len(d.proposals) > 0 && d.proposals[0].index < ent.Index {
				d.proposals[0].cb.Done(ErrResp(&util.ErrStaleCommand{}))
				d.proposals = d.proposals[1:]
			}
			if len(d.proposals) == 0 || d.proposals[0].index != ent.Index || d.proposals[0].term != ent.Term {
				var cbFind *(message.Callback)
				find := false
				if len(d.proposals) > 0 && d.proposals[0].index == ent.Index && d.proposals[0].term != ent.Term {
					// cut callback with smaller index
					// d.proposals[0].cb.Done(ErrResp(&util.ErrStaleCommand{}))
					// NotifyStaleReq(ent.Term, d.proposals[0].cb)
					// d.proposals = d.proposals[1:]
					// if len(d.proposals) >= 1 {
					// 	pi = d.proposals[0].index
					// }
					// dl := len(d.proposals)
					// for di := 0; di < dl && d.proposals[di].index == ent.Index; di += 1 {
					for len(d.proposals) != 0 && d.proposals[0].index == ent.Index {
						if d.proposals[0].term == ent.Term {
							cbFind = d.proposals[0].cb
							find = true
						} else {
							NotifyStaleReq(ent.Term, d.proposals[0].cb)
						}
						d.proposals = d.proposals[1:]
					}
				}
				if !find {
					if ent.Data != nil && d.RaftGroup.Raft.State == raft.StateLeader {
						log.Warnf("Region %+v peer %+v Index %+v Term %+v call back is nil", d.regionId, d.PeerId(), ent.Index, ent.Term)
					}
				}
				d.applyEntry(ent, cbFind)
			} else {
				d.applyEntry(ent, (d.proposals[0].cb))
				if !d.stopped {
					log.Debugf("Region %+v peer %+v process proposals %+v %+v", d.regionId, d.PeerId(), d.proposals[0].index, d.proposals[0].term)
					d.proposals = d.proposals[1:]
				}
			}
			if d.stopped {
				log.Debugf("Region %+v peer %+v have stopped", d.regionId, d.PeerId())
				return
			}
		}
	}
	d.RaftGroup.Advance(rd)
	// log.Debugf("Region %+v peer %+v handle raft ready complete", d.regionId, d.PeerId())
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		log.Debugf("region: %+v store: %+v raftCmd: %+v", d.regionId, d.storeID(), raftCMD)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) nextProposalId() (uint64, uint64) {
	prId := d.nextProposalIndex()
	prTerm := d.Term()
	log.Debugf("peer %+v handle proposal index %+v, term %+v", d.storeID(), prId, prTerm)
	if l := len(d.proposals); l > 0 {
		if prId <= d.proposals[l-1].index {
			prInfo := ""
			for _, pr := range d.proposals {
				prInfo += fmt.Sprintf("index %+v, term %+v    ", pr.index, pr.term)
			}
			prInfo += fmt.Sprintf("index %+v, term %+v    ", prId, prTerm)
			log.Warnf("Index of callback don't increasing, callbacks %+v", prInfo)
			// Note@wy maybe case like this:
			// leader A propose log 1,2,3..., but B,C,D,E don't receive. B hugup become leader with lager term ,  then A become follower, and log 1,2,3 will be discard in function maybeappend. If A become leader next time, A should clear the proposal which have been staled.
			for _, pr := range d.proposals {
				NotifyStaleReq(prTerm, pr.cb)
			}
			d.proposals = make([]*proposal, 0)
		}
	}
	return prId, prTerm
}

func (d *peerMsgHandler) addProposal(cb *message.Callback) {
	prId, prTerm := d.nextProposalId()
	d.proposals = append(d.proposals, &proposal{
		index: prId,
		term:  prTerm,
		cb:    cb,
	})
}

func (d *peerMsgHandler) handleAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	req := msg.AdminRequest
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		d.peer.RaftGroup.Propose(data)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.RaftGroup.TransferLeader(req.TransferLeader.Peer.Id)
		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			},
		})
		return
	case raft_cmdpb.AdminCmdType_ChangePeer:
		if d.RaftGroup.Raft.PendingConfIndex != raft.None && d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.AppliedIndex() {
			cb.Done(ErrResp(raft.ErrProposalDropped))
			return
		}
		changeReq := req.ChangePeer
		ctx, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		err = d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
			ChangeType: changeReq.ChangeType,
			NodeId:     changeReq.Peer.Id,
			Context:    ctx, // Note@wy need to append to log
		})
		if err != nil {
			cb.Done(ErrResp(err))
		}
	case raft_cmdpb.AdminCmdType_Split:
		splitReq := req.Split
		err := util.CheckKeyInRegion(splitReq.SplitKey, d.Region())
		if err != nil {
			if cb != nil {
				cb.Done(ErrResp(err))
			}
			return
		}
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		d.peer.RaftGroup.Propose(data)
	}
	d.addProposal(cb)
}

func (d *peerMsgHandler) handleNormalRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	d.addProposal(cb)
	d.peer.RaftGroup.Propose(data)
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	// log.Debugf("Region %+v peer %+v begin to propose", d.regionId, d.PeerId())
	if msg.AdminRequest != nil {
		d.handleAdminRequest(msg, cb)
	} else if len(msg.Requests) != 0 {
		d.handleNormalRequest(msg, cb)
	}
	// log.Debugf("Region %+v peer %+v propose complete", d.regionId, d.PeerId())
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask // Note@wy send GcLogTask to gc runner
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		// Note@wy response to gcMsg in d.handleStaleMsg()
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Note@wy
	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term) // Note@wy send snapRequest commmand
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	// Note@wy peer attempt itself
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		log.Debugf("Region %+v peer %+v d.ApproximateSize != nil && %+v < d.ctx.cfg.RegionSplitSize/8  ", d.regionId, d.PeerId(), d.SizeDiffHint)
		return
	}
	log.Debugf("Region %+v peer %+v ask for splitCheck", d.regionId, d.PeerId())
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	//Todo@wy why???
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
