package zdb_test

import (
	"github.com/gogf/gf/frame/g"
	"github.com/gogf/gf/test/gtest"
	"github.com/zut/zdb"
	"testing"
	"time"
)

func Test_HDel(t *testing.T) {
	initHash()
	gtest.C(t, func(t *gtest.T) {
		v, err := db.HExists(name, key)
		t.Assert(err, nil)
		t.Assert(v, 1)
		err = db.HDel(name, key)
		t.Assert(err, nil)
		v, err = db.HExists(name, key)
		t.Assert(err, nil)
		t.Assert(v, 0)
	})
	//error
	gtest.C(t, func(t *gtest.T) {
		err := db.HDel(nameNotExist, keyNotExist)
		t.Assert(err, nil)
		err = db.HDel(name, keyNotExist)
		t.Assert(err, nil)
	})
}

func Test_HExists(t *testing.T) {
	initHash()
	gtest.C(t, func(t *gtest.T) {
		v, err := db.HExists(name, key)
		t.Assert(err, nil)
		t.Assert(v, 1)
	})
	//errors
	gtest.C(t, func(t *gtest.T) {
		v, err := db.HExists(nameNotExist, keyNotExist)
		t.Assert(err, nil)
		t.Assert(v, 0)
		v, err = db.HExists(name, keyNotExist)
		t.Assert(err, nil)
		t.Assert(v, 0)
	})
}

func Test_HGet(t *testing.T) {
	initHash()
	// vString
	gtest.C(t, func(t *gtest.T) {
		v, e := db.HGet(name, kk.String)
		t.Assert(e, nil)
		t.AssertEQ(v, vv.String)
	})
	// vBool
	gtest.C(t, func(t *gtest.T) {
		v, e := db.HGet(name, kk.Bool)
		t.Assert(e, nil)
		t.AssertEQ(v, vv.Bool)
	})
	// errors
	gtest.C(t, func(t *gtest.T) {
		v, e := db.HGet(name, keyNotExist)
		t.Assert(e, zdb.ErrKeyNotFound)
		t.Assert(v, nil)
		v, e = db.HGet(nameNotExist, keyNotExist)
		t.Assert(e, zdb.ErrKeyNotFound)
		t.Assert(v, nil)
	})
}
func Test_HGetALL(t *testing.T) {
	initHash()
	gtest.C(t, func(t *gtest.T) {
		v, e := db.HGetALL(nameUser)
		t.Assert(e, nil)
		t.Assert(v, []g.Map{
			{"Age": 1, "Id": 1, "Name": "user1"},
			{"Age": 2, "Id": 2, "Name": "user2"},
			{"Age": 3, "Id": 3, "Name": "user3"},
		})
	})
	// errors
	gtest.C(t, func(t *gtest.T) {
		v, e := db.HGetALL(nameNotExist)
		t.Assert(e, nil)
		t.Assert(v, nil)
		t.Assert(len(v), 0)
	})
}
func Test_HGetTo(t *testing.T) {
	initHash()
	// int64
	gtest.C(t, func(t *gtest.T) {
		var v int64
		e := db.HGetTo(name, kk.Int64, &v)
		t.Assert(e, nil)
		t.AssertEQ(v, vv.Int64)
	})
	// vFloat64
	gtest.C(t, func(t *gtest.T) {
		var v float64
		e := db.HGetTo(name, kk.Float64, &v)
		t.Assert(e, nil)
		t.AssertEQ(v, vv.Float64)
	})
	// vArray
	gtest.C(t, func(t *gtest.T) {
		var v [2]float64
		e := db.HGetTo(name, kk.Array, &v)
		t.Assert(e, nil)
		t.Assert(v[0], vv.Array[0])
		t.Assert(v[1], vv.Array[1])
		t.Assert(len(v), len(vv.Array))
	})
	// vSlice
	gtest.C(t, func(t *gtest.T) {
		var v []int
		e := db.HGetTo(name, kk.Slice, &v)
		t.Assert(e, nil)
		t.AssertEQ(v, vv.Slice)
	})
	// vMap
	gtest.C(t, func(t *gtest.T) {
		var v map[string]int
		e := db.HGetTo(name, kk.Map, &v)
		t.Assert(e, nil)
		t.AssertEQ(v, vv.Map)
	})
	// struct
	gtest.C(t, func(t *gtest.T) {
		var aa User
		e := db.HGetTo(nameUser, user1.Name, &aa)
		t.Assert(e, nil)
		t.Assert(user1.Name, aa.Name)
		t.Assert(user1, aa)
	})
	// error
	gtest.C(t, func(t *gtest.T) {
		var aa User
		e := db.HGetTo(name, keyNotExist, &aa)
		t.Assert(e, zdb.ErrKeyNotFound)
		e = db.HGetTo(nameNotExist, keyNotExist, &aa)
		t.Assert(e, zdb.ErrKeyNotFound)
	})
}

func Test_HGetTTL(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		err := db.HSetTTL(name, key, value, 10)
		t.Assert(err, nil)
		time.Sleep(1 * time.Second)
		ttl, err := db.HGetTTL(name, key)
		t.Assert(err, nil)
		t.Assert(ttl, 9)
	})
}

func Test_HIncr(t *testing.T) {
	_, _ = db.HClear(name)
	gtest.C(t, func(t *gtest.T) {
		v, e := db.HIncr(name, key, 1)
		t.Assert(e, nil)
		t.Assert(v, 1)
		v, e = db.HIncr(name, key, 2)
		t.Assert(e, nil)
		t.Assert(v, 3)
		v, e = db.HIncr(name, key, 3)
		t.Assert(e, nil)
		t.Assert(v, 6)
	})
}
func Test_HKeys(t *testing.T) {
	initHash()
	// all
	gtest.C(t, func(t *gtest.T) {
		keys, err := db.HKeys(name, "")
		t.Assert(err, nil)
		t.Assert(keys, []string{"Array", "Bool", "Float64", "Int64", "Map", "Slice", "String", "key"})
	})
	//  users
	gtest.C(t, func(t *gtest.T) {
		keys, err := db.HKeys(nameUser, "user")
		t.Assert(err, nil)
		t.Assert(keys, []string{"user1", "user2", "user3"})
	})
	// one
	gtest.C(t, func(t *gtest.T) {
		keys, err := db.HKeys(nameUser, "user2")
		t.Assert(err, nil)
		t.Assert(keys, []string{"user2"})
	})
	//errors
	gtest.C(t, func(t *gtest.T) {
		v, err := db.HKeys(nameNotExist, "")
		t.Assert(err, nil)
		t.Assert(v, nil)
	})
}
func Test_HLen(t *testing.T) {
	initHash()
	// all
	gtest.C(t, func(t *gtest.T) {
		v, e := db.HLen(name, "")
		t.Assert(e, nil)
		t.Assert(v, 8)
	})
	gtest.C(t, func(t *gtest.T) {
		v, e := db.HLen(nameUser, "")
		t.Assert(e, nil)
		t.Assert(v, 3)
	})
	gtest.C(t, func(t *gtest.T) {
		v, e := db.HLen(nameUser, user2.Name)
		t.Assert(e, nil)
		t.Assert(v, 1)
	})
	//errors
	gtest.C(t, func(t *gtest.T) {
		v, err := db.HLen(nameNotExist, "")
		t.Assert(err, nil)
		t.Assert(v, 0)
	})
}

func Test_HM(t *testing.T) {
	n, err := db.HClear(name)
	t.Log(n, err)
	//Set
	gtest.C(t, func(t *gtest.T) {
		e := db.HMSet(name, []string{kk.String, kk.Int64}, []interface{}{vv.String, vv.Int64})
		t.Assert(e, nil)
	})
	//Get
	gtest.C(t, func(t *gtest.T) {
		values, err := db.HMGet(name, []string{kk.String, kk.Int64})
		t.Assert(err, nil)
		t.Assert(values, []interface{}{vv.String, vv.Int64})
	})
	//Del
	gtest.C(t, func(t *gtest.T) {
		err := db.HMDel(name, []string{kk.String, kk.Int64})
		t.Assert(err, nil)
	})
	//Get
	gtest.C(t, func(t *gtest.T) {
		values, err := db.HMGet(name, []string{kk.String, kk.Int64})
		t.Assert(err, zdb.ErrKeyNotFound)
		t.Assert(values, nil)

		v, e := db.HExists(name, kk.String)
		t.Assert(e, nil)
		t.Assert(v, 0)
		v, e = db.HExists(name, kk.Int64)
		t.Assert(e, nil)
		t.Assert(v, 0)
	})
}

func Test_HScan(t *testing.T) {
	initHash()
	//all
	gtest.C(t, func(t *gtest.T) {
		items, err := db.HScan(nameUser, "user", 10)
		t.Assert(err, nil)
		t.Assert(items, g.Map{
			"user1": g.Map{"Age": 1, "Id": 1, "Name": "user1"},
			"user2": g.Map{"Age": 2, "Id": 2, "Name": "user2"},
			"user3": g.Map{"Age": 3, "Id": 3, "Name": "user3"},
		})
	})
	//one
	gtest.C(t, func(t *gtest.T) {
		items, err := db.HScan(nameUser, "user1", 10)
		t.Assert(err, nil)
		t.Assert(items, g.Map{
			"user1": g.Map{"Age": 1, "Id": 1, "Name": "user1"},
		})
	})
	//errors
	gtest.C(t, func(t *gtest.T) {
		items, err := db.HScan(nameNotExist, "", 10)
		t.Assert(err, nil)
		t.Assert(len(items), 0)
	})
}

func Test_HSet(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		err := db.HSet(name, key, value)
		t.Assert(err, nil)
		v, e := db.HGet(name, key)
		t.Assert(e, nil)
		t.Assert(v, value)
	})
}

func Test_HSetTTL(t *testing.T) {
	initHash()
	gtest.C(t, func(t *gtest.T) {
		err := db.HSetTTL(name, key, value, 2)
		t.Assert(err, nil)
		v, e := db.HGet(name, key)
		t.Assert(e, nil)
		t.Assert(v, value)

		time.Sleep(1 * time.Second)
		v, e = db.HGet(name, key)
		t.Assert(e, nil)
		t.Assert(v, value)

		time.Sleep(1 * time.Second)
		v, e = db.HGet(name, key)
		t.Assert(e, zdb.ErrKeyNotFound)
		t.Assert(v, nil)
	})
}
func Test_HValues(t *testing.T) {
	initHash()
	gtest.C(t, func(t *gtest.T) {
		values, err := db.HValues(nameUser, "", 10)
		t.Assert(err, nil)
		t.Assert(values, []g.Map{
			{"Age": 1, "Id": 1, "Name": "user1"},
			{"Age": 2, "Id": 2, "Name": "user2"},
			{"Age": 3, "Id": 3, "Name": "user3"},
		})
	})
	gtest.C(t, func(t *gtest.T) {
		values, err := db.HValues(nameUser, "user", 10)
		t.Assert(err, nil)
		t.Assert(values, []g.Map{
			{"Age": 1, "Id": 1, "Name": "user1"},
			{"Age": 2, "Id": 2, "Name": "user2"},
			{"Age": 3, "Id": 3, "Name": "user3"},
		})
	})
	// limit
	gtest.C(t, func(t *gtest.T) {
		values, err := db.HValues(nameUser, "user", 1)
		t.Assert(err, nil)
		t.Assert(values, []g.Map{
			{"Age": 1, "Id": 1, "Name": "user1"},
		})
	})
	// one
	gtest.C(t, func(t *gtest.T) {
		values, err := db.HValues(nameUser, "user2", 10)
		t.Assert(err, nil)
		t.Assert(values, []g.Map{
			{"Age": 2, "Id": 2, "Name": "user2"},
		})
	})
	// errors
	gtest.C(t, func(t *gtest.T) {
		values, err := db.HValues(nameNotExist, "user", 10)
		t.Assert(err, nil)
		t.Assert(len(values), 0)
	})
}

func Test_HValuesTo(t *testing.T) {
	initHash()
	gtest.C(t, func(t *gtest.T) {
		users := make([]User, 0)
		err := db.HValuesTo(nameUser, "", 10, &users)
		t.Assert(err, nil)
		t.Assert(len(users), 3)
		t.Assert(users, sliceUsersTarget)
	})
	gtest.C(t, func(t *gtest.T) {
		users := make([]User, 0)
		err := db.HValuesTo(nameUser, "user", 10, &users)
		t.Assert(err, nil)
		t.Assert(len(users), 3)
		t.Assert(users, sliceUsersTarget)
	})
	// two limit
	gtest.C(t, func(t *gtest.T) {
		users := make([]User, 0)
		err := db.HValuesTo(nameUser, "user", 2, &users)
		t.Assert(err, nil)
		t.Assert(len(users), 2)
		t.Assert(users, sliceUsersTarget[:2])
	})
	// one prefix
	gtest.C(t, func(t *gtest.T) {
		users := make([]User, 0)
		err := db.HValuesTo(nameUser, "user1", 10, &users)
		t.Assert(err, nil)
		t.Assert(len(users), 1)
		t.Assert(users, sliceUsersTarget[:1])
	})
	// errors
	gtest.C(t, func(t *gtest.T) {
		users := make([]User, 0)
		err := db.HValuesTo(nameNotExist, "user2", 10, &users)
		t.Assert(err, nil)
		t.Assert(len(users), 0)
	})
}
