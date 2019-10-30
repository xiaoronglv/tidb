package statistics

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"sort"
	"strconv"
	"time"
)

// getSelectivityBySample returns selectivity based on sampling.
func getSelectivityBySample(ctx sessionctx.Context, exprs []expression.Expression, coll *HistColl) float64 {
	var err error

	// retrieve sample size from session/system configurations
	sizeString, err := variable.GetSessionSystemVar(ctx.GetSessionVars(), variable.TiDBOptimizerDynamicSamplingSize)
	if err != nil {
		return 1
	}
	size, err := strconv.Atoi(sizeString)
	if err != nil {
		return 1
	}

	sampleChunk := coll.getSampleChunk(int64(size))
	totalCount := sampleChunk.NumRows()

	if totalCount == 0 {
		return 1
	}

	// build a expression schema for resolving the column indices in expressions.
	var schemaColumns []*expression.Column
	for uniqueID, statisticColumn := range coll.Columns { // sc: statistics.Column
		expressionColumn := &expression.Column{
			UniqueID: uniqueID,
			Index:    statisticColumn.Info.Offset,
		}
		schemaColumns = append(schemaColumns, expressionColumn)
	}
	sort.Slice(schemaColumns, func(i, j int) bool {
		return schemaColumns[i].Index < schemaColumns[j].Index
	})
	schema := &expression.Schema{Columns: schemaColumns}

	// resolve indices for expressions.
	var newExprs []expression.Expression
	for _, expr := range exprs {
		newSf, _ := expr.ResolveIndices(schema)
		newExprs = append(newExprs, newSf)
	}

	// apply all expression on sample chunk.
	results := make([]bool, 0, totalCount)
	results, err = expression.VectorizedFilter(ctx, newExprs, chunk.NewIterator4Chunk(sampleChunk), results)
	if err != nil {
		return 1
	}

	// count the rows that meet all expressions.
	var selectedCount float64
	for _, result := range results {
		if result {
			selectedCount++
		}
	}

	return selectedCount / float64(totalCount)
}

// isEnabledDynamicSampling returns true when following conditions are met.
// - involved tables are not system tables.
// - value of TiDBOptimizerDynamicSampling is "1"
// - the statistics are missing. Those include histogram and count-min sketch.
func isEnabledDynamicSampling(ctx sessionctx.Context, coll *HistColl) bool {
	// Exclude system tables
	physicalID := coll.PhysicalID
	is := ctx.GetSessionVars().TxnCtx.InfoSchema.(interface {
		TableByID(id int64) (table.Table, bool)
		SchemaByTable(tableInfo *model.TableInfo) (val *model.DBInfo, ok bool)
	})
	tb, _ := is.TableByID(physicalID)
	tableInfo := tb.Meta()
	db, _ := is.SchemaByTable(tableInfo)
	systemDBs := []string{"performance_schema", "information_schema", "mysql", "user"}
	for _, systemDB := range systemDBs {
		if db.Name.L == systemDB {
			return false
		}
	}
	// retrieve Dynamic Sampling Level from session/system variables.
	dsLevel, err := variable.GetSessionSystemVar(ctx.GetSessionVars(), variable.TiDBOptimizerDynamicSampling)
	if err != nil {
		return false
	}
	// return true when involved table is not analyzed and dynamic sampling has been enabled.
	if dsLevel == "1" && (coll.IsMissing() || coll.IsStale()) {
		return true
	}

	return false
}

// getSampleChunk return sample in chunk format.
func (coll *HistColl) getSampleChunk(size int64) *chunk.Chunk {
	var columns []*Column
	for _, c := range coll.Columns {
		columns = append(columns, c)
	}
	sort.Slice(columns, func(i, j int) bool { return columns[i].Info.Offset < columns[j].Info.Offset })

	var tps []*types.FieldType
	for _, c := range columns {
		tps = append(tps, &c.Info.FieldType)
	}

	sampleChunk := chunk.NewChunkWithCapacity(tps, int(size))
	for i, c := range columns {
		sampleChunk.SetCol(int(i), c.Samples[size].SampleColumn)
	}

	return sampleChunk
}

// checkSampleExistence returns true when all samples are existent and fresh.
func checkSampleExistence(ctx sessionctx.Context, coll *HistColl) bool {
	size := getSampleSize(ctx)
	for _, column := range coll.Columns {
		sample, ok := column.Samples[size]
		// return false if any sample doesn't exist or expired.
		if !ok || sample == nil || sample.expireTime.Before(time.Now()){
			_ = analyzeSample(ctx, coll, -1, false, uint64(size), false)
			return false
		}
	}
	return true
}

// getSampleSize retrieve the session variables and global variables and return the size of dynamic sampling.
func getSampleSize(ctx sessionctx.Context) int64 {
	size, err := variable.GetSessionSystemVar(ctx.GetSessionVars(), variable.TiDBOptimizerDynamicSamplingSize)
	if err != nil {
		return 1000
	}
	sampleSize, _ := strconv.Atoi(size)
	return int64(sampleSize)
}

//  getSampleExpireTime return a future time. At that moment, cache will expire.
func getSampleExpireTime(ctx sessionctx.Context) time.Time {
	var seconds int
	ttl, err := variable.GetSessionSystemVar(ctx.GetSessionVars(), variable.TiDBOptimizerDynamicSamplingTTL)
	if err != nil {
		seconds = 120
	} else {
		seconds, _ = strconv.Atoi(ttl)
	}
	return  time.Now().Add(time.Duration(seconds) * time.Second)
}
