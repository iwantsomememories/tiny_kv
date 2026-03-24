package raftstore

import (
	"fmt"
	"strings"
	"time"

	"github.com/Connor1996/badger"
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
	if peer.peerStorage.Engines.Kv != ctx.engine.Kv {
		log.Panicf("kv engine not equal")
	}

	if peer.peerStorage.Engines.Raft != ctx.engine.Raft {
		log.Panicf("raft engine not equal")
	}
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) matchProposal(entry *eraftpb.Entry) *proposal {
	for len(d.proposals) > 0 {
		p := d.proposals[0]

		if entry.Index < p.index {
			return nil
		}

		if entry.Index > p.index {
			NotifyStaleReq(d.Term(), p.cb)
			d.proposals = d.proposals[1:]
			continue
		}

		if entry.Term == p.term {
			return p
		}

		NotifyStaleReq(d.Term(), p.cb)
		d.proposals = d.proposals[1:]
	}

	return nil
}

// 通过心跳通知Scheduler刷新region缓存
func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}

	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) executeCompactLog(admin *raft_cmdpb.AdminRequest, resp *raft_cmdpb.RaftCmdResponse, kvWb *engine_util.WriteBatch) {
	compact := admin.CompactLog
	applyState := d.peerStorage.applyState

	if compact.CompactIndex > applyState.TruncatedState.Index && compact.CompactIndex <= applyState.AppliedIndex {
		applyState.TruncatedState.Index = compact.CompactIndex
		applyState.TruncatedState.Term = compact.CompactTerm
		kvWb.SetMeta(meta.ApplyStateKey(d.Region().Id), applyState)

		d.ScheduleCompactLog(compact.CompactIndex)
	}

	resp.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_CompactLog, CompactLog: &raft_cmdpb.CompactLogResponse{}}
}

func (d *peerMsgHandler) executeChangePeer(admin *raft_cmdpb.AdminRequest, resp *raft_cmdpb.RaftCmdResponse, kvWb *engine_util.WriteBatch) (callback func()) {
	changePeer := admin.ChangePeer
	if d.isDuplicateConfChange(changePeer.ChangeType, changePeer.Peer.StoreId) {
		BindRespError(resp, &util.ErrStaleCommand{})
		return
	}

	peers := d.peerStorage.Region().Peers
	target_store_id := changePeer.Peer.StoreId
	switch changePeer.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		peers = append(peers, changePeer.Peer)
		d.insertPeerCache(changePeer.Peer)
	case eraftpb.ConfChangeType_RemoveNode:
		for i := range peers {
			if peers[i].StoreId == target_store_id {
				if target_store_id == d.storeID() {
					d.destroyPeer()
					return
				}
				peers[i] = peers[len(peers)-1]
				peers = peers[:len(peers)-1]
				break
			}
		}
		d.removePeerCache(changePeer.Peer.Id)
	}

	d.peerStorage.region.RegionEpoch.ConfVer++
	d.peerStorage.region.Peers = peers

	meta.WriteRegionState(kvWb, d.Region(), rspb.PeerState_Normal)

	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	storeMeta.setRegion(d.Region(), d.peer)
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})

	// Debug
	// var regionRangeStrs []string
	// storeMeta.regionRanges.Ascend(func(i btree.Item) bool {
	// 	item := i.(*regionItem)
	// 	regionRangeStrs = append(regionRangeStrs, fmt.Sprintf("region %d: [%v, %v)", item.region.Id, item.region.StartKey, item.region.EndKey))
	// 	return true
	// })
	// log.DPrintfPeerMsgHandler("regionRanges: %s", strings.Join(regionRangeStrs, ","))

	storeMeta.Unlock()

	d.RaftGroup.ApplyConfChange(eraftpb.ConfChange{ChangeType: changePeer.ChangeType, NodeId: changePeer.Peer.Id})

	resp.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_ChangePeer, ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: d.Region()}}

	callback = func() {
		d.notifyHeartbeatScheduler(d.Region(), d.peer)
	}
	return
}

func (d *peerMsgHandler) executeSplitRegion(admin *raft_cmdpb.AdminRequest, resp *raft_cmdpb.RaftCmdResponse, kvWb *engine_util.WriteBatch) (callback func()) {
	leftRegion := d.Region()
	oldRegion := new(metapb.Region)
	rightRegion := new(metapb.Region)

	err := util.CloneMsg(leftRegion, oldRegion)
	if err != nil {
		log.Panic("failed to clone region")
	}

	err = util.CloneMsg(leftRegion, rightRegion)
	if err != nil {
		log.Panic("failed to clone region")
	}

	newPeerIds := admin.Split.NewPeerIds
	splitKey := admin.Split.SplitKey

	// 创建新peer
	oldPeers := d.Region().Peers
	if len(oldPeers) != len(newPeerIds) {
		log.Errorf("%s split failed -- the num of new peers and old peers doesn't match.", d.Tag)
		BindRespError(resp, errors.Errorf("the num of new peers and old peers doesn't match."))
		return
	}
	newPeers := make([]*metapb.Peer, 0)
	for i := range oldPeers {
		newPeers = append(newPeers, &metapb.Peer{Id: newPeerIds[i], StoreId: oldPeers[i].StoreId})
	}

	rightRegion.Id = admin.Split.NewRegionId
	rightRegion.Peers = newPeers
	rightRegion.StartKey = splitKey
	rightRegion.RegionEpoch.Version++

	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, rightRegion)
	if err != nil {
		log.Panicf("failed to create new peer!")
	}

	// 更新旧Region信息
	leftRegion.EndKey = splitKey
	leftRegion.RegionEpoch.Version++

	meta.WriteRegionState(kvWb, leftRegion, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWb, rightRegion, rspb.PeerState_Normal)

	d.ctx.router.register(newPeer)

	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	storeMeta.setRegion(leftRegion, d.peer)
	storeMeta.setRegion(rightRegion, newPeer)
	storeMeta.regionRanges.Delete(&regionItem{region: oldRegion})
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: leftRegion})
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: rightRegion})

	var regionRangeStrs []string
	storeMeta.regionRanges.Ascend(func(i btree.Item) bool {
		item := i.(*regionItem)
		regionRangeStrs = append(regionRangeStrs, fmt.Sprintf("region %d: [%v, %v)", item.region.Id, item.region.StartKey, item.region.EndKey))
		return true
	})
	log.DPrintfPeerMsgHandler("regionRanges: %s", strings.Join(regionRangeStrs, ","))

	storeMeta.Unlock()

	// 清理 SizeDiffHint 和 ApproximateSize
	d.SizeDiffHint = 0
	d.ApproximateSize = new(uint64)

	regions := make([]*metapb.Region, 2)
	regions[0] = leftRegion
	regions[1] = rightRegion
	resp.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_Split, Split: &raft_cmdpb.SplitResponse{Regions: regions}}

	callback = func() {
		d.notifyHeartbeatScheduler(leftRegion, d.peer)
		d.notifyHeartbeatScheduler(rightRegion, newPeer)
		_ = d.ctx.router.send(rightRegion.GetId(), message.Msg{Type: message.MsgTypeStart})
	}
	return
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).

	if d.RaftGroup.HasReady() {
		rd := d.RaftGroup.Ready()
		applySnapResult, err := d.peerStorage.SaveReadyState(&rd)
		if err != nil {
			log.Panicf("unexpected error %v when SaveReadyState.", err)
		}

		if applySnapResult != nil {
			if !util.RegionEqual(applySnapResult.PrevRegion, applySnapResult.Region) {
				log.DPrintfPeerMsgHandler("%s apply snapshot, region changed from %v to %v.", d.Tag, applySnapResult.PrevRegion, applySnapResult.Region)

				d.SetRegion(applySnapResult.Region)

				storeMeta := d.ctx.storeMeta
				storeMeta.Lock()
				storeMeta.regions[d.Region().Id] = d.Region()
				storeMeta.regionRanges.Delete(&regionItem{region: applySnapResult.PrevRegion})
				storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})

				var regionRangeStrs []string
				storeMeta.regionRanges.Ascend(func(i btree.Item) bool {
					item := i.(*regionItem)
					regionRangeStrs = append(regionRangeStrs, fmt.Sprintf("region %d: [%v, %v)", item.region.Id, item.region.StartKey, item.region.EndKey))
					return true
				})
				log.DPrintfPeerMsgHandler("regionRanges: %s", strings.Join(regionRangeStrs, ","))

				storeMeta.Unlock()
			} else {
				log.DPrintfPeerMsgHandler("%s apply snapshot, region no change.", d.Tag)
			}
		}

		// 发送消息
		for _, msg := range rd.Messages {
			err := d.sendRaftMessage(msg, d.ctx.trans)
			if err != nil {
				log.Errorf("%s failed to send raft msg %v from %v to %d.\n", d.Tag, msg.MsgType, *d.Meta, msg.To)
				d.RaftGroup.ReportUnreachable(msg.To)
				if msg.MsgType == eraftpb.MessageType_MsgSnapshot {
					d.RaftGroup.ReportSnapshot(msg.To, raft.SnapshotFailure)
				}
			}
		}

		for _, entry := range rd.CommittedEntries {
			d.process(&entry)
		}

		d.RaftGroup.Advance(rd)
	}
}

func (d *peerMsgHandler) processAdminRequest(cmd *raft_cmdpb.RaftCmdRequest, resp *raft_cmdpb.RaftCmdResponse, kvWb *engine_util.WriteBatch) (callback func()) {
	admin := cmd.AdminRequest

	switch admin.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		d.executeCompactLog(admin, resp, kvWb)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		log.Panicf("transferLeader should be processed before proposal.")
	case raft_cmdpb.AdminCmdType_ChangePeer:
		return d.executeChangePeer(admin, resp, kvWb)
	case raft_cmdpb.AdminCmdType_Split:
		return d.executeSplitRegion(admin, resp, kvWb)
	default:
		log.Panicf("unimplemented admin cmdType %s", admin.CmdType)
	}

	return nil
}

func (d *peerMsgHandler) process(entry *eraftpb.Entry) {
	if d.stopped {
		return
	}

	if entry.Index <= d.peerStorage.AppliedIndex() {
		log.Warnf("%s tried to apply an entry[index: %x] that is already applied[applied index: %x].\n", d.Tag, entry.Index, d.peerStorage.AppliedIndex())
	} else if entry.Index > d.peerStorage.AppliedIndex()+1 {
		log.Panicf("%s found gap between applied index[index: %x] and new committed entry[index: %x].\n ", d.Tag, d.peerStorage.AppliedIndex(), entry.Index)
	} else {
		kvWb := &engine_util.WriteBatch{}

		if len(entry.Data) == 0 {
			d.flushBatchWithAppliedIndex(entry.Index, kvWb)
			return
		}

		var cmd raft_cmdpb.RaftCmdRequest
		var err error
		if entry.EntryType == eraftpb.EntryType_EntryConfChange {
			var cc eraftpb.ConfChange

			if err = cc.Unmarshal(entry.Data); err != nil {
				log.Panicf("unexpected error %s.", err.Error())
			}

			if err = cmd.Unmarshal(cc.Context); err != nil {
				log.Panicf("unexpected error %s.", err.Error())
			}
		} else {
			if err = cmd.Unmarshal(entry.Data); err != nil {
				log.Panicf("unexpected error %s.", err.Error())
			}
		}

		p := d.matchProposal(entry)

		err = util.CheckRegionEpoch(&cmd, d.Region(), true)
		if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
			log.Errorf("epoch not match, %v", errEpochNotMatching)
			siblingRegion := d.findSiblingRegion()
			if siblingRegion != nil {
				errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
			}

			if p != nil {
				p.cb.Done(ErrResp(errEpochNotMatching))
				d.proposals = d.proposals[1:]
			}
			d.flushBatchWithAppliedIndex(entry.Index, kvWb)

			return
		}

		log.DPrintfPeerMsgHandler("%s process %v[index: %d, term: %d]", d.Tag, cmd, entry.Index, entry.Term)

		resp := newCmdResp()
		BindRespTerm(resp, d.Term())

		needSnapTxn := false
		var callback func()
		if cmd.AdminRequest != nil {
			callback = d.processAdminRequest(&cmd, resp, kvWb)
		} else {
			responses := make([]*raft_cmdpb.Response, 0)

			for _, req := range cmd.Requests {
				key := ([]byte)(nil)

				switch req.CmdType {
				case raft_cmdpb.CmdType_Get:
					key = req.Get.Key
				case raft_cmdpb.CmdType_Delete:
					key = req.Delete.Key
				case raft_cmdpb.CmdType_Put:
					key = req.Put.Key
				}

				if req.CmdType != raft_cmdpb.CmdType_Snap {
					if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
						BindRespError(resp, err)
						continue
					}
				}

				switch req.CmdType {
				case raft_cmdpb.CmdType_Get:
					val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, key)
					if err != nil && err != badger.ErrKeyNotFound {
						log.Panicf("unexpected error %v when GetCF[cf: %v, key: %v].\n", err, req.Get.Cf, key)
					}

					responses = append(responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Get, Get: &raft_cmdpb.GetResponse{Value: val}})
				case raft_cmdpb.CmdType_Delete:
					d.SizeDiffHint -= uint64(len(req.Delete.Key))
					kvWb.DeleteCF(req.Delete.Cf, key)

					responses = append(responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Delete, Delete: &raft_cmdpb.DeleteResponse{}})
				case raft_cmdpb.CmdType_Put:
					d.SizeDiffHint += uint64(len(key) + len(req.Put.Value))
					kvWb.SetCF(req.Put.Cf, key, req.Put.Value)

					responses = append(responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Put, Put: &raft_cmdpb.PutResponse{}})
				case raft_cmdpb.CmdType_Snap:
					if p != nil {
						needSnapTxn = true
					}

					responses = append(responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: d.Region()}})
				case raft_cmdpb.CmdType_Invalid:
					log.Warningf("%s found CmdType_Invalid request at %x.", d.Tag, entry.Index)

					responses = append(responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Invalid})
				}
			}

			resp.Responses = responses
		}

		if !d.stopped {
			d.flushBatchWithAppliedIndex(entry.Index, kvWb)
		}

		if callback != nil {
			callback()
		}

		if p != nil && needSnapTxn {
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		}

		if p != nil {
			p.cb.Done(resp)
			d.proposals = d.proposals[1:]
		}
	}
}

func (d *peerMsgHandler) flushBatchWithAppliedIndex(index uint64, kvWb *engine_util.WriteBatch) {
	d.peerStorage.applyState.AppliedIndex = index
	kvWb.SetMeta(meta.ApplyStateKey(d.Region().Id), d.peerStorage.applyState)
	err := kvWb.WriteToDB(d.peerStorage.Engines.Kv)
	if err != nil {
		log.Panicf("unexpected error %v when WriteToDB[wb: %+v].", err, kvWb)
	}

	log.DPrintfPeerMsgHandler("%s update its applied index as %d", d.Tag, index)
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

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	data, marErr := msg.Marshal()
	if marErr != nil {
		cb.Done(ErrRespWithTerm(marErr, d.Term()))
		return
	}

	log.DPrintfPeerMsgHandler("%s received raftCmdRequest %v.", d.Tag, msg)

	if msg.AdminRequest != nil {
		if len(msg.Requests) > 0 {
			log.Errorf("%s found a msg that normal requests and administrator request at same time", d.Tag)
			cb.Done(ErrRespWithTerm(errors.Errorf("illegal request contain both normal requests and administrator request"), d.Term()))
			return
		}

		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			compactLogReq := msg.AdminRequest.CompactLog
			compactIndex := compactLogReq.CompactIndex
			compactTerm := compactLogReq.CompactTerm

			if compactIndex > d.peerStorage.AppliedIndex() {
				log.Errorf("%s snapshot generation failed: compactIndex(%x) > appliedIndex(%x)", d.Tag, compactIndex, d.peerStorage.AppliedIndex())
				cb.Done(ErrRespWithTerm(errors.Errorf("illegal compactIndex(%x) when appliedIndex=%x", compactIndex, d.peerStorage.AppliedIndex()), d.Term()))
				return
			}

			if compactIndex <= d.peerStorage.truncatedIndex() {
				log.Errorf("%s snapshot generation failed: compactIndex(%x) <= truncatedIndex(%x)", d.Tag, compactIndex, d.peerStorage.truncatedIndex())
				cb.Done(ErrRespWithTerm(errors.Errorf("illegal compactIndex(%x) when truncatedIndex=%x", compactIndex, d.peerStorage.truncatedIndex()), d.Term()))
				return
			}

			t, err := d.peerStorage.Term(compactIndex)
			if err != nil || t != compactTerm {
				log.Errorf("%s snapshot generation failed: compactIndex=%x, compactTerm=%x(mismatch with storage)", d.Tag, compactIndex, compactTerm)
				cb.Done(ErrRespWithTerm(errors.Errorf("compactIndex=%x, compactTerm=%x(mismatch with storage)", compactIndex, compactTerm), d.Term()))
				return
			}

			proposalIndex := d.nextProposalIndex()
			proposalTerm := d.Term()
			if err = d.RaftGroup.Propose(data); err != nil {
				cb.Done(ErrRespWithTerm(err, d.Term()))
			} else {
				d.proposals = append(d.proposals, &proposal{index: proposalIndex, term: proposalTerm, cb: cb})
			}
		case raft_cmdpb.AdminCmdType_TransferLeader:
			d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
			resp := newCmdResp()
			BindRespTerm(resp, d.Term())
			resp.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_TransferLeader, TransferLeader: &raft_cmdpb.TransferLeaderResponse{}}
			cb.Done(resp)
		case raft_cmdpb.AdminCmdType_ChangePeer:
			changePeerReq := msg.AdminRequest.ChangePeer
			changeType := changePeerReq.ChangeType
			peer := changePeerReq.Peer

			if d.isDuplicateConfChange(changeType, peer.StoreId) {
				NotifyStaleReq(d.Term(), cb)
				return
			}

			if d.IsLeader() && changeType == eraftpb.ConfChangeType_RemoveNode && peer.StoreId == d.storeID() {
				newLeader, err := d.RaftGroup.Raft.TransferLeaderToBest()
				if err != nil {
					log.Panicf("unexpected error %v when TransferLeaderToBest", err)
				}

				log.DPrintfPeerMsgHandler("%s: remove self, transfer leader to %d", d.Tag, newLeader)

				cb.Done(ErrResp(raft.ErrProposalDropped))
				return
			}

			proposalIndex := d.nextProposalIndex()
			proposalTerm := d.Term()
			err = d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{ChangeType: changeType, NodeId: peer.Id, Context: data})
			if err != nil {
				cb.Done(ErrRespWithTerm(err, proposalTerm))
			} else {
				d.proposals = append(d.proposals, &proposal{index: proposalIndex, term: proposalTerm, cb: cb})
			}
		case raft_cmdpb.AdminCmdType_Split:
			splitReq := msg.AdminRequest.Split

			splitKey := splitReq.SplitKey
			if err := util.CheckKeyInRegion(splitKey, d.Region()); err != nil {
				cb.Done(ErrRespWithTerm(err, d.Term()))
				return
			}

			log.DPrintfPeerMsgHandler("%s received Split Request. cur regionId: %d, startKey: %v, endKey: %v, epoch: %v, peers: %v, newPeerIds: %v", d.Tag, d.Region().Id, d.Region().StartKey, d.Region().EndKey, d.Region().RegionEpoch, d.Region().Peers, splitReq.NewPeerIds)

			proposalIndex := d.nextProposalIndex()
			proposalTerm := d.Term()
			if err = d.RaftGroup.Propose(data); err != nil {
				cb.Done(ErrRespWithTerm(err, proposalTerm))
			} else {
				d.proposals = append(d.proposals, &proposal{index: proposalIndex, term: proposalTerm, cb: cb})
			}
		case raft_cmdpb.AdminCmdType_InvalidAdmin:
			log.Warningf("%s reveived InvalidAdmin.\n", d.Tag)
			cb.Done(ErrRespWithTerm(errors.Errorf("InvalidAdmin"), d.Term()))
		}
	} else if len(msg.Requests) > 0 {
		for _, req := range msg.Requests {
			key := ([]byte)(nil)

			switch req.CmdType {
			case raft_cmdpb.CmdType_Get:
				key = req.Get.Key
			case raft_cmdpb.CmdType_Delete:
				key = req.Delete.Key
			case raft_cmdpb.CmdType_Put:
				key = req.Put.Key
			case raft_cmdpb.CmdType_Snap:
			default:
				log.Errorf("unimplemented cmd type %v.", req.CmdType)
				cb.Done(ErrRespWithTerm(errors.Errorf("unimplemented cmd type %v.", req.CmdType), d.Term()))
				return
			}

			if req.CmdType != raft_cmdpb.CmdType_Snap {
				if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
					cb.Done(ErrRespWithTerm(err, d.Term()))
					return
				}
			}
		}

		proposalIndex := d.nextProposalIndex()
		proposalTerm := d.Term()
		if err = d.RaftGroup.Propose(data); err != nil {
			cb.Done(ErrRespWithTerm(err, proposalTerm))
		} else {
			d.proposals = append(d.proposals, &proposal{index: proposalIndex, term: proposalTerm, cb: cb})
		}
	} else {
		log.Errorf("%s found a msg without any requests", d.Tag)
		cb.Done(ErrRespWithTerm(errors.Errorf("empty requests"), d.Term()))
	}
}

// check whether the conf change has already been applied.
func (d *peerMsgHandler) isDuplicateConfChange(confChangeType eraftpb.ConfChangeType, storeId uint64) bool {
	peer := util.FindPeer(d.Region(), storeId)

	if confChangeType == eraftpb.ConfChangeType_AddNode {
		return peer != nil
	} else {
		return peer == nil
	}
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
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.DPrintfPeerMsgHandler("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
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
	log.DPrintfPeerMsgHandler("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
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

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	// 避免因异步传输快报导致丢失回馈信息
	if msg.Message.MsgType == eraftpb.MessageType_MsgSnapStatus {
		return false
	}

	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

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
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
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
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
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
