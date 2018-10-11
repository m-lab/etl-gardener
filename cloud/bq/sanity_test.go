package bq

import (
	"context"
	"log"
	"strings"
	"testing"

	"github.com/m-lab/go/dataset"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// getTableParts separates a table name into prefix/base, separator, and partition date.
func Test_getTableParts(t *testing.T) {
	parts, err := getTableParts("table$20160102")
	if err != nil {
		t.Error(err)
	} else {
		if !parts.isPartitioned {
			t.Error("Should be partitioned")
		}
		if parts.prefix != "table" {
			t.Error("incorrect prefix: " + parts.prefix)
		}
		if parts.yyyymmdd != "20160102" {
			t.Error("incorrect partition: " + parts.yyyymmdd)
		}
	}

	parts, err = getTableParts("table_20160102")
	if err != nil {
		t.Error(err)
	} else {
		if parts.isPartitioned {
			t.Error("Should not be partitioned")
		}
	}
	parts, err = getTableParts("table$2016010")
	if err == nil {
		t.Error("Should error when partition is incomplete")
	}
	parts, err = getTableParts("table$201601022")
	if err == nil {
		t.Error("Should error when partition is too long")
	}
	parts, err = getTableParts("table$20162102")
	if err == nil {
		t.Error("Should error when partition is invalid")
	}
}

func TestSanityCheckAndCopy(t *testing.T) {
	ctx := context.Background()
	ds, err := dataset.NewDataset(ctx, "project", "dataset")
	if err != nil {
		t.Fatal(err)
	}
	src := ds.Table("foo_19990101")
	dest := ds.Table("foo$19990101")
	srcAt := NewAnnotatedTable(src, &ds)
	destAt := NewAnnotatedTable(dest, &ds)

	err = SanityCheckAndCopy(ctx, srcAt, destAt)
	if err == nil {
		t.Fatal("Should have 404 error")
	}
	if !strings.HasPrefix(err.Error(), "googleapi: Error 404") {
		t.Fatal(err)
	}
}
