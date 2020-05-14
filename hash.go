package zdb

import (
	"errors"
	badger "github.com/dgraph-io/badger/v2"
	"github.com/gogf/gf/encoding/gjson"
	"github.com/gogf/gf/frame/g"
	"github.com/gogf/gf/os/gtime"
	"github.com/gogf/gf/util/gconv"
	"time"
)

func (d *Store) hScanData(name, prefix interface{}, limit int, mode string) (items g.MapStrAny, keys g.SliceStr, values g.SliceAny, err error) {
	cMode(mode)
	items = make(g.MapStrAny)
	err = d.Badger.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = limit // Valid only if PrefetchValues is true
		if mode == "keys" {
			opts.PrefetchValues = false
		}
		it := txn.NewIterator(opts)
		defer it.Close()
		var count int
		for it.Seek(cPrefixHash(name, prefix)); it.ValidForPrefix(cPrefixHash(name, prefix)); it.Next() {
			count += 1
			err = dealItem(it.Item(), items, &keys, &values, mode)
			if err != nil {
				return err
			}
			if limit != 0 && count >= limit {
				break
			}
		}
		return nil
	})
	return
}

func (d *Store) HClear(name interface{}) (int, error) {
	keys, err := d.HKeys(name, "")
	if err != nil {
		return 0, err
	}
	err = d.HMDel(name, keys)
	return len(keys), err
}

func (d *Store) HDel(name, key interface{}) error {
	err := d.Badger.Update(func(txn *badger.Txn) error {
		return txn.Delete(cKeyHash(name, key))
	})
	return err
}

func (d *Store) HExists(name, key interface{}) (i int, err error) {
	err = d.Badger.View(func(txn *badger.Txn) error {
		_, err := txn.Get(cKeyHash(name, key))
		if err != nil {
			if err.Error() == ErrKeyNotFound {
				return nil
			}
			return err
		}
		i = 1
		return err
	})
	return
}

func (d *Store) HGet(name, key interface{}) (value interface{}, err error) {
	err = d.HGetTo(name, key, &value)
	return
}
func (d *Store) HGetALL(name interface{}) (g.SliceAny, error) {
	return d.HValues(name, "", 0)
}
func (d *Store) HGetTo(name, key, value interface{}) (err error) {
	isPointer(value)
	var valCopy []byte
	err = d.Badger.View(func(txn *badger.Txn) error {
		item, err := txn.Get(cKeyHash(name, key))
		if err != nil {
			return err
		}
		valCopy, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return
	}
	err = gjson.DecodeTo(valCopy, value)
	return
}

func (d *Store) HGetTTL(name, key interface{}) (ttl uint64, err error) {
	err = d.Badger.View(func(txn *badger.Txn) error {
		item, err := txn.Get(cKeyHash(name, key))
		if err != nil {
			return err
		}
		ttl = item.ExpiresAt() - gconv.Uint64(gtime.Timestamp())
		return err
	})
	if err != nil {
		return
	}
	return
}

func (d *Store) HIncr(name, key interface{}, num int) (value uint64, err error) {
	seq, err := d.Badger.GetSequence(cKeyHash(name, key), 100)
	if err != nil {
		return
	}
	defer seq.Release()
	for i := 0; i < num; i++ {
		value, err = seq.Next()
		if err != nil {
			return 0, err
		}
		if value == 0 { // init ?
			value, err = seq.Next()
		}
	}
	return
}
func (d *Store) HKeys(name, prefix interface{}) (keys g.SliceStr, err error) {
	_, keys, _, err = d.hScanData(name, prefix, 0, "keys")
	return
}
func (d *Store) HLen(name, prefix interface{}) (int, error) {
	keys, err := d.HKeys(name, prefix)
	return len(keys), err
}

func (d *Store) HMDel(name interface{}, keys []string) error {
	var ks [][]byte
	for _, k := range keys {
		ks = append(ks, cKeyHash(name, k))
	}
	if len(ks) == 0 {
		return errors.New("len(keys) == 0")
	}
	wb := d.Badger.NewWriteBatch()
	defer wb.Cancel()
	for _, k := range ks {
		err := wb.Delete(k)
		if err != nil {
			return err
		}
	}
	err := wb.Flush()
	return err
}

func (d *Store) HMGet(name interface{}, keys []string) (g.SliceAny, error) {
	var values g.SliceAny
	if len(keys) == 0 {
		return values, errors.New("len(keys) == 0")
	}
	for _, k := range keys {
		v, err := d.HGet(name, k)
		if err != nil {
			return values, err
		}
		values = append(values, v)
	}
	return values, nil
}

func (d *Store) HMSet(name interface{}, keys []string, values []interface{}) error {
	if len(keys) != len(values) {
		return errors.New("len(keys) != len(values)")
	}
	var ks [][]byte
	for _, k := range keys {
		ks = append(ks, cKeyHash(name, k))
	}
	var vs [][]byte
	for _, v := range values {
		vEncode, err := gjson.Encode(v)
		if err != nil {
			return err
		}
		vs = append(vs, vEncode)
	}
	if len(vs) == 0 {
		return errors.New("len(values) == 0")
	}
	wb := d.Badger.NewWriteBatch()
	defer wb.Cancel()
	for i, k := range ks {
		err := wb.Set(k, vs[i])
		if err != nil {
			return err
		}
	}
	err := wb.Flush() // Wait for all txns to finish.
	return err
}

func (d *Store) HScan(name, prefix interface{}, limit int) (items g.MapStrAny, err error) {
	items, _, _, err = d.hScanData(name, prefix, limit, "items")
	return
}

func (d *Store) HSet(name, key, value interface{}) error {
	return d.HSetTTL(name, key, value, 0)
}

func (d *Store) HSetTTL(name, key, value interface{}, ttl uint64) error {
	err := d.Badger.Update(func(txn *badger.Txn) error {
		valueEncode, err := gjson.Encode(value)
		if err != nil {
			return err
		}
		entry := badger.NewEntry(cKeyHash(name, key), valueEncode)
		if ttl > 0 {
			entry = entry.WithTTL(time.Second * time.Duration(ttl))
		}
		err = txn.SetEntry(entry)
		return err
	})
	return err
}
func (d *Store) HValues(name, prefix interface{}, limit int) (values g.SliceAny, err error) {
	_, _, values, err = d.hScanData(name, prefix, limit, "values")
	return
}
func (d *Store) HValuesTo(name, prefix interface{}, limit int, itemsPointer interface{}) error {
	isPointer(itemsPointer)
	itemsOriginal, err := d.HValues(name, prefix, limit)
	if err != nil {
		return err
	}
	err = gconv.StructsDeep(itemsOriginal, itemsPointer)
	return err
}
