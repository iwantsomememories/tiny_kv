package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandAloneStorageReader struct {
	Txn *badger.Txn
}

func CreateReader(db *badger.DB) *StandAloneStorageReader {
	return &StandAloneStorageReader{Txn: db.NewTransaction(false)}
}

func (ssr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	item, err := ssr.Txn.Get(engine_util.KeyWithCF(cf, key))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}

	val, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func (ssr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, ssr.Txn)
}

func (ssr *StandAloneStorageReader) Close() {
	ssr.Txn.Discard()
}
