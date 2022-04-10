package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
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

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.GetResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.GetResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	if lock != nil && lock.Ts <= req.GetVersion() {
		return &kvrpcpb.GetResponse{
			Error: &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         req.Key,
					LockTtl:     lock.Ttl,
				},
			},
		}, nil
	}
	value, err := txn.GetValue(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.GetResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	if value != nil {
		return &kvrpcpb.GetResponse{
			Value: value,
		}, nil
	} else {
		return &kvrpcpb.GetResponse{
			NotFound: true,
		}, nil
	}
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.PrewriteResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	keys := make([][]byte, 0)
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	//Todo@wy is it need?
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	keyErr := make([]*kvrpcpb.KeyError, 0)
	for _, mutation := range req.Mutations {
		write, ts, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.PrewriteResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		if write != nil && ts >= txn.StartTS {
			keyErr = append(keyErr, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    txn.StartTS,
					ConflictTs: ts,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}
		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.PrewriteResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		if lock != nil && lock.Ts != txn.StartTS {
			keyErr = append(keyErr, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         mutation.Key,
					LockTtl:     lock.Ttl,
				},
			})
			continue
		}
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mutation.Key, mutation.Value)
			txn.PutLock(mutation.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      txn.StartTS,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindPut,
			})
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mutation.Key)
			txn.PutLock(mutation.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      txn.StartTS,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindDelete,
			})
		}
	}
	if len(keyErr) != 0 {
		return &kvrpcpb.PrewriteResponse{
			Errors: keyErr,
		}, nil
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.PrewriteResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	return &kvrpcpb.PrewriteResponse{}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CommitResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.CommitResponse{RegionError: regionErr.RequestErr}, err
			}
		}
		if lock == nil {
			write, ts, err := txn.CurrentWrite(key)
			if err != nil {
				if regionErr, ok := err.(*raft_storage.RegionError); ok {
					return &kvrpcpb.CommitResponse{RegionError: regionErr.RequestErr}, err
				}
			}
			if write == nil {
				// Todo@wy wait for reading tinysql
				continue
			}
			if write != nil && (ts != req.CommitVersion || write.Kind == mvcc.WriteKindRollback) {
				// Todo@wy if write.Kind == mvcc.WriteKindRollback , need to write rollback for all?
				log.Warnf("Server find that key %+v is not locked, and write is %+v write.ts is %+v but request.commitversion is %+v", key, write, ts, req.CommitVersion)
				return &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{
						Abort: "Abort",
					},
				}, nil
			}
			if write != nil && ts == req.CommitVersion {
				continue
			}
		}
		if lock != nil && lock.Ts != req.StartVersion {
			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable: "Retry",
				},
			}, nil
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindPut})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CommitResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	pairs := make([]*kvrpcpb.KvPair, 0)
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.ScanResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	for i := 0; i < int(req.Limit); i++ {
		k, v, err := scanner.Next()
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.ScanResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		if k == nil && v == nil && err == nil {
			break
		}
		pairs = append(pairs, &kvrpcpb.KvPair{
			Key:   k,
			Value: v,
		})
	}
	return &kvrpcpb.ScanResponse{
		Pairs: pairs,
	}, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CheckTxnStatusResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	keys := make([][]byte, 0)
	keys = append(keys, req.PrimaryKey)
	//Todo@wy is it need?
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CheckTxnStatusResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	if write != nil {
		if write.Kind == mvcc.WriteKindRollback {
			return &kvrpcpb.CheckTxnStatusResponse{
				Action: kvrpcpb.Action_NoAction,
			}, nil
		} else {
			return &kvrpcpb.CheckTxnStatusResponse{
				CommitVersion: commitTs,
			}, nil
		}
	}
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CheckTxnStatusResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	var act kvrpcpb.Action
	if lock == nil {
		act = kvrpcpb.Action_LockNotExistRollback
		txn.PutWrite(req.PrimaryKey, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	} else {
		if mvcc.PhysicalTime(req.LockTs)+lock.Ttl >= mvcc.PhysicalTime(req.CurrentTs) {
			return &kvrpcpb.CheckTxnStatusResponse{
				LockTtl: lock.Ttl,
			}, nil
		} else {
			act = kvrpcpb.Action_TTLExpireRollback
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: lock.Ts,
				Kind:    mvcc.WriteKindRollback,
			}) // Todo@wy which ts should use for write?
			txn.DeleteValue(req.PrimaryKey)
			txn.DeleteLock(req.PrimaryKey)
		}
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CheckTxnStatusResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	return &kvrpcpb.CheckTxnStatusResponse{Action: act}, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.BatchRollbackResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.BatchRollbackResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				return &kvrpcpb.BatchRollbackResponse{
					Error: &kvrpcpb.KeyError{
						Abort: "Abort",
					}}, err
			}
			continue
		}
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.BatchRollbackResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		if lock != nil {
			if lock.Ts != txn.StartTS {
				txn.DeleteValue(key)
			} else {
				txn.DeleteValue(key)
				txn.DeleteLock(key)
			}
		} else {
			log.Infof("Server KvBatchRollback for key %+v, find no write or lock with ts %+v", key, txn.StartTS)
		}
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.BatchRollbackResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	return &kvrpcpb.BatchRollbackResponse{}, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.ResolveLockResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	iter := txn.Reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	keys := make([][]byte, 0)
	for iter.Valid() {
		value, err := iter.Item().Value()
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.ResolveLockResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.ResolveLockResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		if lock.Ts == req.StartVersion {
			keys = append(keys, iter.Item().Key())
		}
		iter.Next()
	}
	var resp kvrpcpb.ResolveLockResponse
	if req.CommitVersion != 0 {
		respCommit, err := server.KvCommit(context.TODO(), &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.ResolveLockResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		resp = kvrpcpb.ResolveLockResponse(*respCommit)
	} else {
		respRollback, err := server.KvBatchRollback(context.TODO(), &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.ResolveLockResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		resp = kvrpcpb.ResolveLockResponse(*respRollback)
	}
	return &resp, nil
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
