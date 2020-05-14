package main

import (
	"fmt"
	"github.com/zut/zdb"
	"time"
)

func main() {
	db := zdb.NewDB("/Users/d/data/z_test", "2piEs8trY0wK76XQJsAIXTbbAewRG22W")
	defer db.Close()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
	again:
		fmt.Println(1)
		err := db.RunValueLogGC()
		if err == nil {
			goto again
		}
	}
}
