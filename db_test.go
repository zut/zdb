package zdb_test

import (
	"github.com/gogf/gf/test/gtest"
	"testing"
)

func TestNewDB(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		//db, err := zdb.NewDB("test")
		//t.Assert(err, nil)
		defer db.Close()
	})

}
