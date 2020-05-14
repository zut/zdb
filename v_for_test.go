package zdb_test

import (
	"github.com/gogf/gf/util/grand"
	"github.com/zut/zdb"
	_ "github.com/zut/zdb"
	"math"
)

type Base struct {
	Age        int
	privateAge int // 私有属性不会进行转换
}
type User struct {
	Id   int
	Name string
	Base
}

var (
	user1 = User{Id: 1, Name: "user1", Base: Base{Age: 1, privateAge: 1}}
	user2 = User{Id: 2, Name: "user2", Base: Base{Age: 2, privateAge: 2}}
	user3 = User{Id: 3, Name: "user3", Base: Base{Age: 3, privateAge: 3}}
)
var sliceUsersTarget = []User{
	{Id: 1, Name: "user1", Base: Base{Age: 1, privateAge: 0}},
	{Id: 2, Name: "user2", Base: Base{Age: 2, privateAge: 0}},
	{Id: 3, Name: "user3", Base: Base{Age: 3, privateAge: 0}},
}

type keysTest struct {
	String  string
	Bool    string
	Int64   string
	Float64 string
	Array   string
	Slice   string
	Map     string
}

var kk = keysTest{
	String: "String", // not to
	Bool:   "Bool",   // not to

	Int64:   "Int64",
	Float64: "Float64",
	Array:   "Array",
	Slice:   "Slice",
	Map:     "Map",
}

type valuesTest struct {
	Int64   int64
	Float64 float64
	String  string
	Bool    bool
	Array   [2]float64
	Slice   []int
	Map     map[string]int
}

var vv = valuesTest{
	String:  grand.S(10),
	Bool:    true,
	Int64:   64,
	Float64: math.Pi,
	Array:   [2]float64{.03, .02},
	Slice:   []int{1, 2},
	Map:     map[string]int{"a": 1, "b": 222},
}
var name = "name"
var nameUser = "nameUser"
var key = "key"
var value = "value"
var nameNotExist = "nameNotExist"
var keyNotExist = "keyNotExist"

var db *zdb.Store

var path = "/Users/d/data/test"
var encryptionKey = "2piEs8trY0wK76XQJsAIXTbbAewRG22W"

func init() {
	db = zdb.NewDB(path, encryptionKey)
}

func initHash() {
	_, _ = db.HClear(name)
	_, _ = db.HClear(nameUser)
	_ = db.HSet(name, key, value)
	_ = db.HSet(name, kk.Int64, vv.Int64)
	_ = db.HSet(name, kk.Float64, vv.Float64)
	_ = db.HSet(name, kk.String, vv.String)
	_ = db.HSet(name, kk.Bool, vv.Bool)
	_ = db.HSet(name, kk.Array, vv.Array)
	_ = db.HSet(name, kk.Slice, vv.Slice)
	_ = db.HSet(name, kk.Map, vv.Map)
	_ = db.HSet(nameUser, user1.Name, user1)
	_ = db.HSet(nameUser, user2.Name, user2)
	_ = db.HSet(nameUser, user3.Name, user3)
}
