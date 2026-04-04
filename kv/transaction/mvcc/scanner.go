package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	currentKey []byte
	startTs    uint64
	reader     storage.StorageReader
	iter       engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	key := append([]byte{}, startKey...)
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(key, TsMax))

	return &Scanner{
		currentKey: key,
		startTs:    txn.StartTS,
		reader:     txn.Reader,
		iter:       iter,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	if scan.iter != nil {
		scan.iter.Close()
		scan.iter = nil
	}
	if scan.reader != nil {
		scan.reader.Close()
		scan.reader = nil
	}
	scan.currentKey = nil
}

// 寻找下一个与curKey不同的userKey
func (scan *Scanner) advanceToNextUserKey(curKey []byte) {
	for ; scan.iter.Valid(); scan.iter.Next() {
		item := scan.iter.Item()
		rawKey := item.Key()

		userKey := DecodeUserKey(rawKey)
		if bytes.Equal(userKey, curKey) {
			continue
		}

		scan.currentKey = append([]byte{}, userKey...)
		return
	}

	scan.currentKey = nil
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.currentKey == nil || scan.iter == nil {
		return nil, nil, nil
	}

	for scan.currentKey != nil {
		seekKey := append([]byte{}, scan.currentKey...)

		scan.iter.Seek(EncodeKey(seekKey, TsMax))
		if !scan.iter.Valid() {
			scan.currentKey = nil
			return nil, nil, nil
		}

		for ; scan.iter.Valid(); scan.iter.Next() {
			item := scan.iter.Item()
			rawKey := item.Key()

			userKey, commitTs := DecodeUserKey(rawKey), decodeTimestamp(rawKey)
			if commitTs > scan.startTs {
				continue
			}

			rawVal, err := item.Value()
			if err != nil {
				scan.advanceToNextUserKey(userKey)
				return userKey, nil, err
			}

			write, err := ParseWrite(rawVal)
			if err != nil {
				scan.advanceToNextUserKey(userKey)
				return userKey, nil, err
			}

			switch write.Kind {
			case WriteKindRollback:
				continue
			case WriteKindDelete:
				scan.advanceToNextUserKey(userKey)
				break
			case WriteKindPut:
				userVal, err := scan.reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
				scan.advanceToNextUserKey(userKey)
				if err != nil {
					return userKey, nil, err
				}
				return userKey, userVal, nil
			}
		}

		if !scan.iter.Valid() {
			scan.currentKey = nil
			return nil, nil, nil
		}
	}

	return nil, nil, nil
}
