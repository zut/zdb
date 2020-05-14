package main

import (
	"github.com/gogf/gf/os/gcache"
	"github.com/gogf/gf/os/glog"
	"github.com/gogf/gf/os/gtime"
	"github.com/gogf/gf/text/gstr"
	"github.com/gogf/gf/util/gconv"
	"github.com/zut/zdb"
	"time"
)

func t1() {
	glog.Debug("T1", gtime.Datetime())
	gcache.Set("T1.Start", time.Now(), 0) // 改成先进后出, 剥洋葱的方式, 嵌套多个
}

func t2() {
	v := gcache.Get("T1.Start")
	elapsed := time.Now().Sub(gconv.Time(v))
	glog.Debug("T2 elapsed = ", elapsed)
}
func main() {
	db := zdb.NewDB("/Users/d/data/z_test", "2piEs8trY0wK76XQJsAIXTbbAewRG22W")
	defer db.Close()
	randomString := gstr.Repeat("1", 1024*1024) // 1MB?

	name := "z"
	n, err := db.HClear(name)
	glog.Info(n, err)
	for j := 0; j < 1; j++ {
		t1()
		for i := 0; i < 1000; i++ {
			err := db.HSet(name, i, randomString)

			if err != nil {
				glog.Fatal(i, "Set", err)
			}
			_, err = db.HGet(name, i)
			if err != nil {
				glog.Fatal(i, "Get", err)
			}
			_ = db.HDel(name, i)
		}
		t2()
		lsm, vlog := db.Size()
		glog.Info(j, lsm/1024, "KB", vlog/1024/1024, "MB")
		time.Sleep(1 * time.Second)
	}

}
