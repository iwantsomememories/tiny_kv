package server

import (
	"context"
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

func (server *Server) acquireLatches(keys [][]byte) {
	for {
		wg := server.Latches.AcquireLatches(keys)
		if wg == nil {
			return
		}
		wg.Wait()
	}
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	response := &kvrpcpb.GetResponse{}

	// 创建事务
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		response.RegionError = util.RaftstoreErrToPbError(err)
		return response, nil
	}
	defer reader.Close()
	startTs := req.Version
	mvccTxn := mvcc.NewMvccTxn(reader, startTs)

	// 检查是否上锁
	userKey := req.Key
	lock, err := mvccTxn.GetLock(userKey)
	if err != nil {
		if _, ok := err.(*util.ErrKeyNotInRegion); ok {
			response.RegionError = util.RaftstoreErrToPbError(err)
			return response, nil
		} else {
			// 内部错误，不返回给客户端
			return nil, err
		}
	}

	if lock.IsLockedFor(userKey, startTs, response) {
		return response, nil
	}

	val, err := mvccTxn.GetValue(userKey)
	if err != nil {
		if _, ok := err.(*util.ErrKeyNotInRegion); ok {
			response.RegionError = util.RaftstoreErrToPbError(err)
			return response, nil
		} else {
			// 内部错误，不返回给客户端
			return nil, err
		}
	}

	response.Value = val
	if val == nil {
		response.NotFound = true
	}

	return response, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	response := &kvrpcpb.PrewriteResponse{}

	// 获取相关key对应latch
	mutations := req.Mutations
	keysToLatch := make([][]byte, 0, len(mutations))
	for _, mutation := range mutations {
		keysToLatch = append(keysToLatch, mutation.Key)
	}

	server.acquireLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	// 创建事务
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		response.RegionError = util.RaftstoreErrToPbError(err)
		return response, nil
	}
	defer reader.Close()
	startTs := req.StartVersion
	mvccTxn := mvcc.NewMvccTxn(reader, startTs)

	primaryKey := req.PrimaryLock

	for _, mutation := range mutations {
		// 检查是否已上锁
		userKey := mutation.Key
		lock, err := mvccTxn.GetLock(userKey)
		if err != nil {
			if _, ok := err.(*util.ErrKeyNotInRegion); ok {
				response.RegionError = util.RaftstoreErrToPbError(err)
				return response, nil
			} else {
				// 内部错误，不返回给客户端
				return nil, err
			}
		}

		if lock != nil {
			if lock.Ts != startTs {
				// 其他事务锁住了Key
				response.Errors = append(response.Errors, &kvrpcpb.KeyError{
					Locked: lock.Info(userKey),
				})
			}
			continue
		}

		// 检查是否写冲突
		write, commitTs, err := mvccTxn.MostRecentWrite(userKey)
		if err != nil {
			// 内部错误，不返回给客户端
			return nil, err
		}

		if write != nil && commitTs > req.StartVersion {
			response.Errors = append(response.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    startTs,
					ConflictTs: commitTs,
					Key:        userKey,
					Primary:    primaryKey,
				},
			})
			continue
		}

		// 写入Lock
		newWriteKind := mvcc.WriteKindFromProto(mutation.Op)
		newLock := &mvcc.Lock{
			Primary: primaryKey,
			Ts:      startTs,
			Ttl:     req.LockTtl,
			Kind:    newWriteKind,
		}
		mvccTxn.PutLock(userKey, newLock)

		// 写入Default
		switch newWriteKind {
		case mvcc.WriteKindPut:
			mvccTxn.PutValue(userKey, mutation.Value)
		case mvcc.WriteKindDelete:
			// Delete类型不需要实际删除Default
		case mvcc.WriteKindRollback:
			// Rollback类型不需要实际写入Default
		}
	}

	// 持久化
	if len(response.Errors) == 0 {
		// 所有key都成功上锁
		err = server.storage.Write(req.Context, mvccTxn.Writes())
		if err != nil {
			response.RegionError = util.RaftstoreErrToPbError(err)
		}
	}

	return response, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	response := &kvrpcpb.CommitResponse{}

	// 获取相关key对应latch
	keysToLatch := req.Keys
	server.acquireLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	// 创建事务
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		response.RegionError = util.RaftstoreErrToPbError(err)
		return response, nil
	}
	defer reader.Close()
	startTs := req.StartVersion
	mvccTxn := mvcc.NewMvccTxn(reader, startTs)

	// 检查所有Key是否成功上锁
	for _, userKey := range keysToLatch {
		lock, err := mvccTxn.GetLock(userKey)
		if err != nil {
			if _, ok := err.(*util.ErrKeyNotInRegion); ok {
				response.RegionError = util.RaftstoreErrToPbError(err)
				return response, nil
			} else {
				// 内部错误，不返回给客户端
				return nil, err
			}
		}

		committed := false
		if lock == nil {
			// Lock不存在，检查是否已经Write
			write, _, err := mvccTxn.CurrentWrite(userKey)
			if err != nil {
				// 内部错误，不返回给客户端
				return nil, err
			}

			if write == nil {
				// 没有Prewrite的情况下直接返回成功
				continue
			} else if write.Kind == mvcc.WriteKindRollback {
				// 已回滚
				response.Error = &kvrpcpb.KeyError{Abort: fmt.Sprintf("Txn already rolled back on key %s", string(userKey))}
				return response, nil
			}
			committed = true
		} else if lock.Ts != startTs {
			// Lock属于其他事务
			response.Error = &kvrpcpb.KeyError{Retryable: fmt.Sprintf("Other txn's lock(ts=%v) on key %s", lock.Ts, string(userKey))}
			return response, nil
		}

		if !committed {
			// 写入Write并删除Lock
			mvccTxn.PutWrite(userKey, req.CommitVersion, &mvcc.Write{
				StartTS: startTs,
				Kind:    lock.Kind,
			})
			mvccTxn.DeleteLock(userKey)
		}
	}

	// 持久化
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		response.RegionError = util.RaftstoreErrToPbError(err)
	}

	return response, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	response := &kvrpcpb.ScanResponse{}
	if req.Limit == 0 {
		return response, nil
	}

	// 创建事务
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		response.RegionError = util.RaftstoreErrToPbError(err)
		return response, nil
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)

	scanner := mvcc.NewScanner(req.StartKey, mvccTxn)
	defer scanner.Close()

	pairs := []*kvrpcpb.KvPair{}
	for len(pairs) < int(req.Limit) {
		key, val, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
		pairs = append(pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})
	}

	response.Pairs = pairs
	return response, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	response := &kvrpcpb.CheckTxnStatusResponse{}

	keysToLatch := [][]byte{req.PrimaryKey}
	server.acquireLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	// 创建事务
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		response.RegionError = util.RaftstoreErrToPbError(err)
		return response, nil
	}
	defer reader.Close()
	mvccTxn := mvcc.NewMvccTxn(reader, req.LockTs)

	lock, err := mvccTxn.GetLock(req.PrimaryKey)
	if err != nil {
		// 内部错误，不返回给客户端
		return nil, err
	}

	if lock != nil && lock.Ts == req.LockTs {
		// Lock存在且属于要检查的事务
		if mvcc.PhysicalTime(req.CurrentTs)-mvcc.PhysicalTime(lock.Ts) >= lock.Ttl {
			// Lock已经过期

			// 回滚并删除锁
			mvccTxn.DeleteLock(req.PrimaryKey)
			mvccTxn.DeleteValue(req.PrimaryKey)
			mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})

			response.Action = kvrpcpb.Action_TTLExpireRollback
		} else {
			// Lock尚未过期
			response.LockTtl = lock.Ttl
			response.Action = kvrpcpb.Action_NoAction
		}
	} else {
		// 检查是否已提交或回滚
		write, commitTs, err := mvccTxn.CurrentWrite(req.PrimaryKey)
		if err != nil {
			// 内部错误，不返回给客户端
			return nil, err
		}

		if write == nil {
			// 进行回滚
			mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})
			response.Action = kvrpcpb.Action_LockNotExistRollback
		} else if write.Kind == mvcc.WriteKindRollback {
			// 已回滚
			response.Action = kvrpcpb.Action_NoAction
		} else {
			// 已提交
			response.Action = kvrpcpb.Action_NoAction
			response.CommitVersion = commitTs
		}
	}

	if len(mvccTxn.Writes()) > 0 {
		err = server.storage.Write(req.Context, mvccTxn.Writes())
		if err != nil {
			response.RegionError = util.RaftstoreErrToPbError(err)
		}
	}

	return response, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	response := &kvrpcpb.BatchRollbackResponse{}

	keysToLatch := req.Keys
	server.acquireLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	// 创建事务
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		response.RegionError = util.RaftstoreErrToPbError(err)
		return response, nil
	}
	defer reader.Close()
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range keysToLatch {
		write, commitTs, err := mvccTxn.CurrentWrite(key)
		if err != nil {
			// 内部错误，不返回给客户端
			return nil, err
		}

		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			}

			// key已提交
			response.Error = &kvrpcpb.KeyError{Abort: fmt.Sprintf("key(%s) has already commited(commitTs=%v)", string(key), commitTs)}
			return response, nil
		}

		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			// 内部错误，不返回给客户端
			return nil, err
		}

		if lock == nil {
			// 锁不存在时也要写入rollback标记
			mvccTxn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
		} else if lock.Ts == req.StartVersion {
			mvccTxn.DeleteLock(key)
			if lock.Kind == mvcc.WriteKindPut {
				mvccTxn.DeleteValue(key)
			}
			mvccTxn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
		} else {
			// key上存在其他事务的锁时也要写入rollback标记
			if lock.Kind == mvcc.WriteKindPut {
				mvccTxn.DeleteValue(key)
			}
			mvccTxn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
		}
	}

	if len(mvccTxn.Writes()) > 0 {
		err = server.storage.Write(req.Context, mvccTxn.Writes())
		if err != nil {
			response.RegionError = util.RaftstoreErrToPbError(err)
		}
	}

	return response, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	response := &kvrpcpb.ResolveLockResponse{}

	// 获取所有锁住的key
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		response.RegionError = util.RaftstoreErrToPbError(err)
		return response, nil
	}
	iter := reader.IterCF(engine_util.CfLock)

	keys := [][]byte{}
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			iter.Close()
			reader.Close()
			return nil, err
		}
		lock, err := mvcc.ParseLock(val)
		if err != nil {
			iter.Close()
			reader.Close()
			return nil, err
		}
		if lock.Ts == req.StartVersion {
			keys = append(keys, item.Key())
		}
	}
	iter.Close()
	reader.Close()

	if req.CommitVersion == 0 {
		bResponse, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})

		if err != nil {
			return nil, err
		}
		response.Error = bResponse.Error
		response.RegionError = bResponse.RegionError
	} else {
		cResponse, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		if err != nil {
			return nil, err
		}
		response.Error = cResponse.Error
		response.RegionError = cResponse.RegionError
	}

	return response, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
