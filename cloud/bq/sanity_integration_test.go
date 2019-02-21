// +build integration

package bq_test

import (
	"context"
	"log"
	"testing"

	"github.com/m-lab/etl-gardener/cloud/bq"
	"github.com/m-lab/go/dataset"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestGetTableDetail(t *testing.T) {
	ctx := context.Background()
	destDS, err := dataset.NewDataset(ctx, "mlab-testing", "etl")
	if err != nil {
		t.Fatal(err)
	}

	// Check that it handles empty partitions
	detail, err := bq.GetTableDetail(ctx, &destDS, destDS.Table("DedupTest$20001229"))
	if err != nil {
		t.Error(err)
	} else if detail.TaskFileCount > 0 || detail.TestCount > 0 {
		t.Error("Should have zero counts")
	}

	// Check that it handles single partitions.
	// TODO - update to create its own test table.
	detail, err = bq.GetTableDetail(ctx, &destDS, destDS.Table("DedupTest$19990101"))
	if err != nil {
		t.Error(err)
	} else if detail.TaskFileCount != 2 || detail.TestCount != 4 {
		t.Errorf("Wrong number of tasks or tests %v\n", detail)
	}

	srcDS, err := dataset.NewDataset(ctx, "mlab-testing", "src")
	if err != nil {
		t.Fatal(err)
	}

	// Check that it handles full table.
	// TODO - update to create its own test table.
	detail, err = bq.GetTableDetail(ctx, &srcDS, srcDS.Table("DedupTest_19990101"))
	if err != nil {
		t.Error(err)
	} else if detail.TaskFileCount != 2 || detail.TestCount != 4 {
		t.Errorf("Wrong number of tasks or tests %v\n", detail)
	}
}

func TestCachedMeta(t *testing.T) {
	// TODO - Make NewDataSet return a pointer, for consistency with bigquery.
	ctx := context.Background()
	dsExt, err := dataset.NewDataset(ctx, "mlab-testing", "src")
	if err != nil {
		t.Fatal(err)
	}

	tbl := dsExt.Table("DedupTest")
	at := bq.NewAnnotatedTable(tbl, &dsExt)
	meta, err := at.CachedMeta(ctx)
	if err != nil {
		t.Error(err)
	} else if meta.NumRows != 8 {
		t.Errorf("Wrong number of rows: %d", meta.NumRows)
	}
	if meta.TimePartitioning == nil {
		t.Error("Should be partitioned")
	}

	tbl = dsExt.Table("XYZ")
	at = bq.NewAnnotatedTable(tbl, &dsExt)
	meta, err = at.CachedMeta(nil)
	if err != bq.ErrNilContext {
		t.Error("Should be an error when no context provided")
	}
	meta, err = at.CachedMeta(ctx)
	if err == nil {
		t.Error("Should be an error when fetching bad table meta")
	}
}

func TestCachedPartitionInfo(t *testing.T) {
	ctx := context.Background()
	dsExt, err := dataset.NewDataset(ctx, "mlab-testing", "src")
	if err != nil {
		t.Fatal(err)
	}

	tbl := dsExt.Table("DedupTest$19990101")

	at := bq.NewAnnotatedTable(tbl, &dsExt)
	// Fetch cache detail - which hits backend

	_, err = at.CachedPartitionInfo(ctx)
	if err != nil {
		t.Error(err)
	}
	// Fetch again, exercising the cached code path.
	_, err = at.CachedPartitionInfo(ctx)
	if err != nil {
		t.Error(err)
	}

	badTable := dsExt.Table("non-existant")

	badAT := bq.NewAnnotatedTable(badTable, &dsExt)
	// Fetch cache detail - which hits backend

	_, err = badAT.CachedPartitionInfo(ctx)
	if err == nil {
		t.Error("Should return error")
	}
	// Fetch again, exercising the already errored code path.
	_, err = badAT.CachedPartitionInfo(ctx)
	if err == nil {
		t.Error("Should return error")
	}
}

func TestGetTablesMatching(t *testing.T) {
	ctx := context.Background()
	dsExt, err := dataset.NewDataset(ctx, "mlab-testing", "src")
	if err != nil {
		t.Fatal(err)
	}

	atList, err := bq.GetTablesMatching(ctx, &dsExt, "Test")
	if err != nil {
		t.Error(err)
	} else if len(atList) != 3 {
		t.Errorf("Wrong length: %d", len(atList))
	}
}

func TestAnnotatedTableGetPartitionInfo(t *testing.T) {
	ctx := context.Background()
	dsExt, err := dataset.NewDataset(ctx, "mlab-testing", "src")
	if err != nil {
		t.Fatal(err)
	}

	tbl := dsExt.Table("DedupTest$19990101")
	at := bq.NewAnnotatedTable(tbl, &dsExt)
	info, err := at.GetPartitionInfo(ctx)
	if err != nil {
		t.Error(err)
	} else if info.PartitionID != "19990101" {
		t.Error("wrong partitionID: " + info.PartitionID)
	}

	// Check behavior for missing partition
	tbl = dsExt.Table("DedupTest$17760101")
	at = bq.NewAnnotatedTable(tbl, &dsExt)
	info, err = at.GetPartitionInfo(ctx)
	if err != nil {
		t.Error(err)
	} else if info.PartitionID != "" {
		t.Error("Non-existent partition should return empty PartitionID")
	}

}

func TestCachedDetail(t *testing.T) {
	ctx := context.Background()
	ds, err := dataset.NewDataset(ctx, "mlab-testing", "src")
	if err != nil {
		t.Fatal(err)
	}
	src := ds.Table("DedupTest")
	srcAt := bq.NewAnnotatedTable(src, &ds)
	// Fetch detail.
	_, err = srcAt.CachedDetail(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Fetch again to exercise cached code path.
	_, err = srcAt.CachedDetail(ctx)
	if err != nil {
		t.Fatal(err)
	}
}
