package main

import (
	"fmt"
	"github.com/gogf/gf/os/glog"
	"github.com/zut/zdb"
)

func foo1(db *zdb.Store, done chan bool) {
	for i := 0; i < 10; i++ {
		e := db.HSet(1, i, i)
		glog.Info(e)
		v, e := db.HGet(1, i)
		glog.Info(v, e)
	}
	done <- true
}
func foo2(db *zdb.Store, done chan bool) {
	for i := 0; i < 10; i++ {
		e := db.HSet(1, i, i)
		glog.Info(e)
		v, e := db.HGet(1, i)
		glog.Info(v, e)
	}
	done <- true
}

func main() {
	db := zdb.NewDB("/Users/d/data/z_test", "2piEs8trY0wK76XQJsAIXTbbAewRG22W")
	defer db.Close()
	done := make(chan bool)
	go foo1(db, done)
	go foo2(db, done)
	<-done
	fmt.Println("done")
}
