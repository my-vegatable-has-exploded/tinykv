package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	iter    engine_util.DBIterator
	txn     *MvccTxn
	nextKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	return &Scanner{
		iter:    txn.Reader.IterCF(engine_util.CfWrite),
		txn:     txn,
		nextKey: startKey,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	iter := scan.iter
	key := scan.nextKey
	txn := scan.txn
	for iter.Valid() {
		iter.Seek(EncodeKey(key, 0))
		if !iter.Valid() {
			return nil, nil, nil
		}
		key = DecodeUserKey(iter.Item().Key())
		scan.nextKey = key
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, nil, err
		}
		if lock != nil && lock.Ts <= txn.StartTS {
			continue
		}
		value, err := txn.GetValue(key)
		if err != nil {
			return nil, nil, err
		}
		if value == nil {
			continue
		}
		return key, value, nil
	}
	return nil, nil, nil
}
