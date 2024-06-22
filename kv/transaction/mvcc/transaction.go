package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).

	// TODO: ts和CommitTs是否相同
	modify := storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, ts),
			Cf:    engine_util.CfWrite,
			Value: write.ToBytes(),
		},
	}
	txn.writes = append(txn.writes, modify)
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	reader := txn.Reader
	value, err := reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	lock, err := ParseLock(value)
	if err != nil || lock == nil {
		return nil, nil
	}
	return lock, err
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Put{
			Key:   key,
			Cf:    engine_util.CfLock,
			Value: lock.ToBytes(),
		},
	}
	txn.writes = append(txn.writes, modify)
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf:  engine_util.CfLock,
		},
	}
	txn.writes = append(txn.writes, modify)
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.

// 函数不需要像Percolator Get函数的伪代码一样，需要key上无锁
// 函数查找在StartTs之前，key的最近一次提交的Value
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	writeIter := txn.Reader.IterCF(engine_util.CfWrite)
	for writeIter.Seek(EncodeKey(key, txn.StartTS)); writeIter.Valid(); writeIter.Next() {
		writeItem := writeIter.Item()

		writeKey := writeItem.KeyCopy(nil)
		if !bytes.Equal(DecodeUserKey(writeKey), key) {
			return nil, nil
		}

		writeValue, err := writeItem.ValueCopy(nil)
		if err != nil {
			return nil, err
		}

		write, err := ParseWrite(writeValue)
		if err != nil {
			return nil, err
		}

		// TODO: 还不理解为什么Kind == Put
		if write.Kind == WriteKindPut {
			data, err := txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
			if err != nil {
				return nil, err
			}
			return data, nil
		} else if write.Kind == WriteKindDelete {
			return nil, nil
		}
	}
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, txn.StartTS),
			Cf:    engine_util.CfDefault,
			Value: value,
		},
	}
	txn.writes = append(txn.writes, modify)
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Delete{
			Key: EncodeKey(key, txn.StartTS),
			Cf:  engine_util.CfDefault,
		},
	}
	txn.writes = append(txn.writes, modify)
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.

// CurrentWrite返回当前事务StartTs对应的Write, Write的CommitTs, err
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	writeIter := txn.Reader.IterCF(engine_util.CfWrite)

	// 按CommitTs从后向前顺序，查找对应key所以的提交，并找到txn.StartTs对应的Write
	for writeIter.Seek(EncodeKey(key, TsMax)); writeIter.Valid(); writeIter.Next() {
		writeItem := writeIter.Item()
		writeKey := DecodeUserKey(writeItem.KeyCopy(nil))
		if !bytes.Equal(writeKey, key) {
			// 迭代项的key和要查找的目标key不同了，说明目标key的write都查完了，没有符合的。
			return nil, 0, nil
		}

		writeValue, err := writeItem.ValueCopy(nil)
		if err != nil {
			return nil, 0, err
		}

		write, err := ParseWrite(writeValue)
		if err != nil {
			return nil, 0, err
		}

		if write.StartTS == txn.StartTS {
			return write, decodeTimestamp(writeItem.KeyCopy(nil)), nil
		}

		if write.StartTS < txn.StartTS {
			return nil, 0, nil
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.

// 找到给定Key的最近一次提交 （Mysql中的 "select ... for update" 当前读）
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	writeIter := txn.Reader.IterCF(engine_util.CfWrite)
	for writeIter.Seek(EncodeKey(key, TsMax)); writeIter.Valid(); writeIter.Next() {
		writeItem := writeIter.Item()
		writeKey := DecodeUserKey(writeItem.KeyCopy(nil))
		if !bytes.Equal(writeKey, key) {
			// 迭代项的key和要查找的目标key不同了，说明目标key的write都查完了，没有符合的。
			return nil, 0, nil
		}

		writeValue, err := writeItem.ValueCopy(nil)
		if err != nil {
			return nil, 0, err
		}

		write, err := ParseWrite(writeValue)
		if err != nil {
			return nil, 0, err
		}

		return write, decodeTimestamp(writeItem.KeyCopy(nil)), nil
	}

	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
