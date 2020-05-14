package zdb

import (
	badger "github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/gogf/gf/encoding/gjson"
	"github.com/gogf/gf/frame/g"
	"github.com/gogf/gf/os/glog"
	"github.com/gogf/gf/text/gregex"
	"github.com/gogf/gf/text/gstr"
	"github.com/gogf/gf/util/gconv"
	"os"
	"reflect"
	"time"
)

type Store struct {
	Badger *badger.DB
	//closeLk   sync.RWMutex
	//closed    bool
	//closeOnce sync.Once
	//closing   chan struct{}
	//
	//gcDiscardRatio float64
	//gcSleep        time.Duration
	//gcInterval     time.Duration
	//
	//syncWrites bool
}

func NewDB(path string, encryptionKey string, ) *Store {
	glog.Debug("Init DB (badger)", path)
	if len(encryptionKey) != 32 {
		glog.Fatal("len(encryptionKey)!=32")
	}
	//if !fileExists("config.toml") && !fileExists("config/config.toml") {
	//	glog.Fatal("NewDB: config.toml OR config/config.toml not exists. ")
	//}
	////dbName := g.Cfg().GetString("db.name")
	//path := g.Cfg().GetString("db.path") + "/" + name
	//if runtime.GOOS == "darwin" {
	//	path = "/Users/d/data/db/" + name
	//}
	opt := badger.DefaultOptions(path)
	//opt.ValueLogLoadingMode = options.FileIO
	opt.Compression = options.ZSTD
	opt.EncryptionKey = gconv.Bytes(encryptionKey)
	bdb, err := badger.Open(opt)
	if err != nil {
		glog.Fatal("Open DB error: ", path, err)
		//return nil, err
	}
	d := &Store{
		Badger: bdb,
	}
	err = d.RunValueLogGC(0.01)
	if err != nil {
		glog.Fatal("Open DB error(RunValueLogGC): ", path, err)
	}
	go d.gc()
	return d
}
func (d *Store) Close() {
	glog.Debug("Close DB ")
	time.Sleep(1 * time.Second) // wait
	err := d.Badger.Close()
	if err != nil {
		glog.Warning("Close DB error:", err)
	}
}
func (d *Store) Size() (lsm, vlog int64) {
	lsm, vlog = d.Badger.Size()
	return
}

func (d *Store) gc() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
	again:
		glog.Info("RunValueLogGC ... ticker 0.5")
		err := d.Badger.RunValueLogGC(0.5)
		if err == nil {
			goto again
		}
	}
}

func (d *Store) RunValueLogGC(discardRatio float64) error {
	glog.Debug("RunValueLogGC ...", discardRatio)
	err := d.Badger.RunValueLogGC(discardRatio)
	if err != nil {
		if err.Error() == ErrNoRewrite {
			return nil
		} else {
			glog.Warning("RunValueLogGC error:", err)
		}
	}
	return err
}

// common
func cNil(v interface{}) {
	if v == nil || v == "" {
		glog.Fatal("is nil? ", v)
	}
}
func c(name, key interface{}, prefix string) []byte {
	if !gregex.IsMatchString(`^(KV|HASH|INCR)$`, prefix) {
		glog.Fatal("prefix not is KV|HASH|INCR ", prefix)
	} else if gregex.IsMatchString(`__`, gconv.String(name)) {
		glog.Fatal("name include __", prefix)
	} else if gregex.IsMatchString(`__`, gconv.String(key)) {
		glog.Fatal("key include __", prefix)
	}

	v := g.SliceStr{prefix, gconv.String(name), gconv.String(key)}
	if prefix == "KV" {
		v = g.SliceStr{prefix, gconv.String(key)}
	}
	v2 := gconv.Bytes(gstr.Join(v, "__"))
	//glog.Debug(gconv.String(v2))
	return v2
}
func cKeyHash(name interface{}, key interface{}) []byte {
	cNil(name)
	cNil(key)
	return c(name, key, "HASH")
}
func cPrefixHash(name, key interface{}) []byte {
	cNil(name)
	return c(name, key, "HASH")
}

func cMode(mode string) {
	if !gregex.IsMatchString(`^(items|keys|values)$`, mode) {
		glog.Fatal("ModeLimit items/keys/values")
	}
}

func dealItem(item *badger.Item, items g.MapStrAny, keys *g.SliceStr, values *g.SliceAny, mode string) (err error) {
	key, err := gregex.ReplaceString(`^.*?__.*?__`, "", gconv.String(item.Key()))
	if err != nil {
		return
	}
	if mode == "keys" {
		*keys = append(*keys, key)
	} else {
		var valCopy []byte
		valCopy, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		value, err := gjson.Decode(valCopy)
		if err != nil {
			return err
		}
		if mode == "values" {
			*values = append(*values, value)
		} else {
			items[key] = value
		}
	}
	return err
}
func isPointer(v interface{}) {
	if reflect.ValueOf(v).Kind() != reflect.Ptr {
		glog.Fatal("v is not Pointer: " + gconv.String(reflect.ValueOf(v).Kind()))
	}
}
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
