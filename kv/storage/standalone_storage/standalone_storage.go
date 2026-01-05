package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := conf.DBPath + "/kv"
	raftPath := conf.DBPath + "/raft"

	kvEngine := engine_util.CreateDB(kvPath, false)
	if conf.Raft {
		raftEngine := engine_util.CreateDB(raftPath, true)
		return &StandAloneStorage{engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath)}
	} else {
		return &StandAloneStorage{engine_util.NewEngines(kvEngine, nil, kvPath, raftPath)}
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	return CreateReader(s.engines.Kv), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := &engine_util.WriteBatch{}
	wb.Reset()

	for _, modify := range batch {
		cf := modify.Cf()
		key := modify.Key()
		value := modify.Value()
		if value == nil {
			wb.DeleteCF(cf, key)
		} else {
			wb.SetCF(cf, key, value)
		}
	}

	return s.engines.WriteKV(wb)
}
