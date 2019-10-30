package statistics

import (
	"bytes"
	"context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/variable"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
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
		if !(ok && sample != nil && sample.expireTime.After(time.Now())) {
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


// AnalyzeSample get the sample for the place using, it is for one table
func analyzeSample(ctx sessionctx.Context, histColl *HistColl, columnID int64, isIndex bool, sampleSize uint64, fulltable bool) error {

	taskCh := make(chan *analyzeSampleTask, 3)
	resultCh := make(chan analyzeSampleResult, 3)

	// setup consumer(s)
	go func() {
		for {
			task, ok := <-taskCh
			if !ok {
				break
			}
			for _, result := range analyzeSampleExec(task.sampleExec) {
				resultCh <- result
			}
		}
		close(resultCh)
	}()

	// publish task(s) to the channel.
	buildAnalyzeSampleTask(ctx, histColl, columnID, isIndex, sampleSize, fulltable, taskCh)
	close(taskCh)

	// Get Results
	panicCnt := 0
	for panicCnt < 1 {
		result, ok := <-resultCh
		if !ok {
			break
		}
		if result.Err != nil {
			if result.Err == errAnalyzeSamplingWorkerPanic {
				panicCnt++
			}
			break
		}

		// Process the result, give the pointer of the sample to A
		if result.IsIndex == 1 {
			histColl.Indices[result.Sample[0].SID].SampleC = result.Sample[0]
		} else {
		idLoop:
			for _, samplec := range result.Sample {
				expireTime := getSampleExpireTime(ctx)
				for _, statisticsColumn := range histColl.Columns {
					if samplec.SID == int64(statisticsColumn.Info.ID) {
						samplec.expireTime = expireTime
						samplec.size = int64(sampleSize)
						key := int64(sampleSize)
						if statisticsColumn.Samples == nil {
							statisticsColumn.Samples = make(map[int64]*SampleC)
						}
						statisticsColumn.Samples[key] = samplec
						continue idLoop
					}

				}
			}
		}
	}
	return nil
}

var (
	// RandSeed is Random seed
	RandSeed                      = int64(1)
	errAnalyzeSamplingWorkerPanic = errors.New("sample worker panic")
)

type analyzeSampleTask struct {
	sampleExec *AnalyzeSampleExec
}

// AnalyzeSampleExec is a executor used to sampling
type AnalyzeSampleExec struct {
	ctx             sessionctx.Context
	physicalTableID int64
	pkInfo          *model.ColumnInfo
	colsInfo        []*model.ColumnInfo
	idxsInfo        []*model.IndexInfo
	concurrency     int
	tblInfo         *model.TableInfo
	cache           *tikv.RegionCache
	wg              *sync.WaitGroup
	sampLocs        chan *tikv.KeyLocation
	rowCount        uint64
	sampCursor      int32
	sampTasks       []*AnalyzeFastTask
	scanTasks       []*tikv.KeyLocation
	collectors      []*SampleCollector
	randSeed        int64
	sampleSize      uint64
}

// AnalyzeFastTask is the task use to sample from kv
type AnalyzeFastTask struct {
	Location    *tikv.KeyLocation
	SampSize    uint64
	BeginOffset uint64
	EndOffset   uint64
}

type analyzeSampleResult struct {
	PhysicalTableID int64
	Sample          []*SampleC
	IsIndex         int
	Err             error
}

func getTableInfoByID(ctx sessionctx.Context, ID int64) *model.TableInfo {
	is := ctx.GetSessionVars().TxnCtx.InfoSchema.(interface {
		TableByID(id int64) (table.Table, bool)
	})
	table, _ := is.TableByID(ID)
	return table.Meta()
}

func buildAnalyzeSampleTask(ctx sessionctx.Context, histColl *HistColl, columnID int64, isIndex bool, sampleSize uint64, fulltable bool, taskCh chan *analyzeSampleTask) {
	tableInfo := getTableInfoByID(ctx, histColl.PhysicalID)
	cloumsInfos := tableInfo.Columns
	indexInfos := tableInfo.Indices
	pkInfo := tableInfo.GetPkColInfo()
	concurrency := 1

	// the following lines could be deleted.
	if fulltable {
		// TODO
		// Sampling for full table
		return
	}

	// Ryan TODO: polish the following lines
	var sampleExec AnalyzeSampleExec
	sampleExec.ctx = ctx
	sampleExec.physicalTableID = histColl.PhysicalID
	sampleExec.tblInfo = tableInfo
	sampleExec.concurrency = concurrency
	sampleExec.wg = &sync.WaitGroup{}
	sampleExec.sampleSize = sampleSize
	if isIndex {
		// Do one task for one index
		sampleExec.idxsInfo = []*model.IndexInfo{indexInfos[columnID]}
		taskCh <- &analyzeSampleTask{
			sampleExec: &sampleExec,
		}
	} else {
		// Do one task for all over the columns
		sampleExec.pkInfo = pkInfo
		if pkInfo != nil {
			sampleExec.colsInfo = cloumsInfos[1:]
		} else {
			sampleExec.colsInfo = cloumsInfos
		}
		taskCh <- &analyzeSampleTask{
			sampleExec: &sampleExec,
		}
	}
	return
}

// analyzeSampleExec process samples from a single table, returning statistics on all columns
// Since the sampling for one table may be the sampling of idx samples and colm
// it may be necessary to return multiple analyzeResults.
func analyzeSampleExec(exec *AnalyzeSampleExec) []analyzeSampleResult {
	sample, err := exec.buildSample()

	if err != nil {
		return []analyzeSampleResult{{Err: err}}
	}
	var results []analyzeSampleResult
	hasPKInfo := 0
	if exec.pkInfo != nil {
		hasPKInfo = 1
	}
	// Handling the existence of an index
	if len(exec.idxsInfo) > 0 {
		for i := hasPKInfo + len(exec.idxsInfo); i < len(sample); i++ {
			idxResult := analyzeSampleResult{
				PhysicalTableID: exec.physicalTableID,
				Sample:          []*SampleC{sample[i]},
				IsIndex:         1,
			}
			results = append(results, idxResult)
		}
	}

	// Handling the sampling on the column
	if len(exec.colsInfo) > 0 {
		colResult := analyzeSampleResult{
			PhysicalTableID: exec.physicalTableID,
			Sample:          sample[:hasPKInfo+len(exec.colsInfo)],
		}
		results = append(results, colResult)
	}
	return results
}

// buildSample is enter of sampling for single task
func (e *AnalyzeSampleExec) buildSample() (sample []*SampleC, err error) {
	if RandSeed == 1 {
		e.randSeed = time.Now().UnixNano()
	} else {
		e.randSeed = RandSeed
	}
	rander := rand.New(rand.NewSource(e.randSeed))

	needRebuild, maxBuildTimes := true, 5
	regionErrorCounter := 0
	for counter := maxBuildTimes; needRebuild && counter > 0; counter-- {
		regionErrorCounter++
		needRebuild, err = e.buildSampTask()
		if err != nil {
			return nil, err
		}
	}

	if needRebuild {
		errMsg := "build Sample analyze task failed, exceed maxBuildTimes: %v"
		return nil, errors.Errorf(errMsg, maxBuildTimes)
	}

	// If the total number of rows in the table is less than 2 times the sample size
	// do full table scan
	// TODO: Maybe there is a bug（e.rowCount == 0）
	if e.rowCount < e.sampleSize*2 {
		for _, task := range e.sampTasks {
			e.scanTasks = append(e.scanTasks, task.Location)
		}
		e.sampTasks = e.sampTasks[:0]
		e.rowCount = 0
		return e.runTasks()
	}

	// Generate n random numbers, n is the sample length
	randPos := make([]uint64, 0, e.sampleSize+1)
	for i := 0; i < int(e.sampleSize); i++ {
		randPos = append(randPos, uint64(rander.Int63n(int64(e.rowCount))))
	}
	sort.Slice(randPos, func(i, j int) bool { return randPos[i] < randPos[j] })

	for _, task := range e.sampTasks {
		begin := sort.Search(len(randPos), func(i int) bool { return randPos[i] >= task.BeginOffset })
		end := sort.Search(len(randPos), func(i int) bool { return randPos[i] >= task.EndOffset })
		task.SampSize = uint64(end - begin)
	}
	return e.runTasks()
}

func (e *AnalyzeSampleExec) runTasks() ([]*SampleC, error) {
	errs := make([]error, e.concurrency)
	hasPKInfo := 0
	if e.pkInfo != nil {
		hasPKInfo = 1
	}

	length := len(e.colsInfo) + hasPKInfo + len(e.idxsInfo)
	e.collectors = make([]*SampleCollector, length)
	for i := range e.collectors {
		e.collectors[i] = &SampleCollector{
			MaxSampleSize: int64(e.sampleSize),
			Samples:       make([]*SampleItem, e.sampleSize),
		}
	}

	e.wg.Add(e.concurrency)
	bo := tikv.NewBackoffer(context.Background(), 500)
	if len(e.sampTasks) > 0 {
		for i := 0; i < e.concurrency; i++ {
			go e.handleSampTasks(bo, i, &errs[i])
		}
		e.wg.Wait()
		for _, err := range errs {
			if err != nil {
				return nil, err
			}
		}
	}

	_, err := e.handleScanTasks(bo)
	if err != nil {
		return nil, err
	}

	rowCount := int64(e.rowCount)

	// Debug
	// Can be used to judge the system table
	// fmt.Printf("--%v : %v--\n", e.tblInfo.Name, e.physicalTableID)

	// Generate sampling results
	samples := make([]*SampleC, length)
	for i := 0; i < length; i++ {
		collector := e.collectors[i]
		collector.Samples = collector.Samples[:e.sampCursor]
		sort.Slice(collector.Samples, func(i, j int) bool { return collector.Samples[i].RowID < collector.Samples[j].RowID })
		collector.CalcTotalSize()

		rowCount = mathutil.MaxInt64(rowCount, int64(len(collector.Samples)))

		if len(collector.Samples) == 0 {
			return nil, nil
		}

		collector.TotalSize *= rowCount / int64(len(collector.Samples))

		// Debug
		// fmt.Printf("-col = %v\n", i)

		if i < hasPKInfo {
			samples[i], err = e.buildColSample(e.pkInfo.ID, e.collectors[i], true)
		} else if i < hasPKInfo+len(e.colsInfo) {
			samples[i], err = e.buildColSample(e.colsInfo[i-hasPKInfo].ID, e.collectors[i], false)
		} else {
			samples[i], err = e.buildIdxSample(e.idxsInfo[i-hasPKInfo-len(e.colsInfo)].ID, e.collectors[i])
		}
		if err != nil {
			return nil, err
		}
	}
	return samples, nil
}

func (e *AnalyzeSampleExec) buildColSample(colID int64, collector *SampleCollector, isPk bool) (*SampleC, error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	sampleItems := collector.Samples
	err := SortSampleItems(sc, sampleItems)
	if err != nil {
		return nil, err
	}

	var tp types.FieldType
	if isPk {
		tp = e.pkInfo.FieldType
	} else {
		for _, colInfo := range e.colsInfo {
			if colInfo.ID == colID {
				tp = colInfo.FieldType
			}
		}
	}
	cc := chunk.NewColumn(&tp, int(e.sampleSize))

	for i := 0; i < len(sampleItems); i++ {
		datums2ChunkColumn(sampleItems[i].Value, cc)
		//fmt.Println(sampleItems[i].Value.GetValue())
	}
	return &SampleC{
		SID:          colID,
		SampleColumn: cc,
	}, nil
}

func (e *AnalyzeSampleExec) buildIdxSample(colID int64, collector *SampleCollector) (*SampleC, error) {
	// TODO
	// If is a task for index, do stx datum-->chunk.colunm
	// And return the *SampleC as result
	return nil, nil
}

func datums2ChunkColumn(dt types.Datum, cc *chunk.Column) {
	switch dt.Kind() {
	case types.KindNull:
		cc.AppendNull()
	case types.KindInt64:
		cc.AppendInt64(dt.GetInt64())
	case types.KindUint64:
		cc.AppendUint64(dt.GetUint64())
	case types.KindFloat32:
		cc.AppendFloat32(dt.GetFloat32())
	case types.KindFloat64:
		cc.AppendFloat64(dt.GetFloat64())
	case types.KindString, types.KindBytes, types.KindBinaryLiteral, types.KindRaw, types.KindMysqlBit:
		cc.AppendBytes(dt.GetBytes())
	case types.KindMysqlDecimal:
		cc.AppendMyDecimal(dt.GetMysqlDecimal())
	case types.KindMysqlDuration:
		cc.AppendDuration(dt.GetMysqlDuration())
	case types.KindMysqlEnum:
		cc.AppendEnum(dt.GetMysqlEnum())
	case types.KindMysqlSet:
		cc.AppendSet(dt.GetMysqlSet())
	case types.KindMysqlTime:
		cc.AppendTime(dt.GetMysqlTime())
	case types.KindMysqlJSON:
		cc.AppendJSON(dt.GetMysqlJSON())
	}
}

//--from executor--//

// buildSampTask returns two variables, the first bool is whether the task meets region error
// and need to rebuild.
func (e *AnalyzeSampleExec) buildSampTask() (needRebuild bool, err error) {
	// Do get regions row count.
	bo := tikv.NewBackoffer(context.Background(), 500)
	needRebuildForRoutine := make([]bool, e.concurrency)
	errs := make([]error, e.concurrency)
	sampTasksForRoutine := make([][]*AnalyzeFastTask, e.concurrency)
	e.sampLocs = make(chan *tikv.KeyLocation, e.concurrency)
	e.wg.Add(e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		go e.getSampRegionsRowCount(bo, &needRebuildForRoutine[i], &errs[i], &sampTasksForRoutine[i])
	}

	defer func() {
		close(e.sampLocs)
		e.wg.Wait()
		if err != nil {
			return
		}
		for i := 0; i < e.concurrency; i++ {
			if errs[i] != nil {
				err = errs[i]
			}
			needRebuild = needRebuild || needRebuildForRoutine[i]
			e.sampTasks = append(e.sampTasks, sampTasksForRoutine[i]...)
		}
	}()

	store, _ := e.ctx.GetStore().(tikv.Storage)
	e.cache = store.GetRegionCache()
	startKey, endKey := tablecodec.GetTableHandleKeyRange(e.physicalTableID)
	targetKey, err := e.getNextSampleKey(bo, startKey)
	if err != nil {
		return false, err
	}
	e.rowCount = 0
	for _, task := range e.sampTasks {
		cnt := task.EndOffset - task.BeginOffset
		task.BeginOffset = e.rowCount
		task.EndOffset = e.rowCount + cnt
		e.rowCount += cnt
	}
	accessRegionsCounter := 0
	for {
		// Search for the region which contains the targetKey.
		loc, err := e.cache.LocateKey(bo, targetKey)
		if err != nil {
			return false, err
		}
		if bytes.Compare(endKey, loc.StartKey) < 0 {
			break
		}
		accessRegionsCounter++

		// Set the next search key.
		targetKey = loc.EndKey

		// If the KV pairs in the region all belonging to the table, add it to the sample task.
		if bytes.Compare(startKey, loc.StartKey) <= 0 && len(loc.EndKey) != 0 && bytes.Compare(loc.EndKey, endKey) <= 0 {
			e.sampLocs <- loc
			continue
		}

		e.scanTasks = append(e.scanTasks, loc)
		if bytes.Compare(loc.StartKey, startKey) < 0 {
			loc.StartKey = startKey
		}
		if bytes.Compare(endKey, loc.EndKey) < 0 || len(loc.EndKey) == 0 {
			loc.EndKey = endKey
			break
		}
	}

	return false, nil
}

func (e *AnalyzeSampleExec) getNextSampleKey(bo *tikv.Backoffer, startKey kv.Key) (kv.Key, error) {
	if len(e.sampTasks) == 0 {
		e.scanTasks = e.scanTasks[:0]
		return startKey, nil
	}
	sort.Slice(e.sampTasks, func(i, j int) bool {
		return bytes.Compare(e.sampTasks[i].Location.StartKey, e.sampTasks[j].Location.StartKey) < 0
	})
	// The sample task should be consecutive with scan task.
	if len(e.scanTasks) > 0 && bytes.Equal(e.scanTasks[0].StartKey, startKey) && !bytes.Equal(e.scanTasks[0].EndKey, e.sampTasks[0].Location.StartKey) {
		e.scanTasks = e.scanTasks[:0]
		e.sampTasks = e.sampTasks[:0]
		return startKey, nil
	}
	prefixLen := 0
	for ; prefixLen < len(e.sampTasks)-1; prefixLen++ {
		if !bytes.Equal(e.sampTasks[prefixLen].Location.EndKey, e.sampTasks[prefixLen+1].Location.StartKey) {
			break
		}
	}
	// Find the last one that could align with region bound.
	for ; prefixLen >= 0; prefixLen-- {
		loc, err := e.cache.LocateKey(bo, e.sampTasks[prefixLen].Location.EndKey)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(loc.StartKey, e.sampTasks[prefixLen].Location.EndKey) {
			startKey = loc.StartKey
			break
		}
	}
	e.sampTasks = e.sampTasks[:prefixLen+1]
	for i := len(e.scanTasks) - 1; i >= 0; i-- {
		if bytes.Compare(startKey, e.scanTasks[i].EndKey) < 0 {
			e.scanTasks = e.scanTasks[:i]
		}
	}
	return startKey, nil
}

func (e *AnalyzeSampleExec) getSampRegionsRowCount(bo *tikv.Backoffer, needRebuild *bool, err *error, sampTasks *[]*AnalyzeFastTask) {
	defer func() {
		if *needRebuild == true {
			for ok := true; ok; _, ok = <-e.sampLocs {
				// 清空 e.sampLocs (region 的信息)
			}
		}
		e.wg.Done()
	}()
	client := e.ctx.GetStore().(tikv.Storage).GetTiKVClient()
	for {
		loc, ok := <-e.sampLocs
		if !ok {
			return
		}
		req := tikvrpc.NewRequest(tikvrpc.CmdDebugGetRegionProperties, &debugpb.GetRegionPropertiesRequest{
			RegionId: loc.Region.GetID(),
		})
		var resp *tikvrpc.Response
		var rpcCtx *tikv.RPCContext
		rpcCtx, *err = e.cache.GetTiKVRPCContext(bo, loc.Region, e.ctx.GetSessionVars().GetReplicaRead(), 0)
		if *err != nil {
			return
		}

		ctx := context.Background()
		resp, *err = client.SendRequest(ctx, rpcCtx.Addr, req, tikv.ReadTimeoutMedium)
		if *err != nil {
			return
		}
		if resp.Resp == nil || len(resp.Resp.(*debugpb.GetRegionPropertiesResponse).Props) == 0 {
			*needRebuild = true
			return
		}
		for _, prop := range resp.Resp.(*debugpb.GetRegionPropertiesResponse).Props {
			if prop.Name == "mvcc.num_rows" {
				var cnt uint64
				cnt, *err = strconv.ParseUint(prop.Value, 10, 64)
				if *err != nil {
					return
				}
				newCount := atomic.AddUint64(&e.rowCount, cnt)
				task := &AnalyzeFastTask{
					Location:    loc,
					BeginOffset: newCount - cnt,
					EndOffset:   newCount,
				}
				*sampTasks = append(*sampTasks, task)
				break
			}
		}
	}
}

func (e *AnalyzeSampleExec) handleSampTasks(bo *tikv.Backoffer, workID int, err *error) {
	defer e.wg.Done()
	var snapshot kv.Snapshot
	snapshot, *err = e.ctx.GetStore().(tikv.Storage).GetSnapshot(kv.MaxVersion)
	if *err != nil {
		return
	}
	if e.ctx.GetSessionVars().GetReplicaRead().IsFollowerRead() {
		snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}
	rander := rand.New(rand.NewSource(e.randSeed + int64(workID)))

	for i := workID; i < len(e.sampTasks); i += e.concurrency {
		task := e.sampTasks[i]
		if task.SampSize == 0 {
			continue
		}

		var tableID, minRowID, maxRowID int64
		startKey, endKey := task.Location.StartKey, task.Location.EndKey
		tableID, minRowID, *err = tablecodec.DecodeRecordKey(startKey)
		if *err != nil {
			return
		}
		_, maxRowID, *err = tablecodec.DecodeRecordKey(endKey)
		if *err != nil {
			return
		}

		keys := make([]kv.Key, 0, task.SampSize)
		for i := 0; i < int(task.SampSize); i++ {
			randKey := rander.Int63n(maxRowID-minRowID) + minRowID
			keys = append(keys, tablecodec.EncodeRowKeyWithHandle(tableID, randKey))
		}

		kvMap := make(map[string][]byte, len(keys))
		for _, key := range keys {
			var iter kv.Iterator
			iter, *err = snapshot.Iter(key, endKey)
			if *err != nil {
				return
			}
			if iter.Valid() {
				kvMap[string(iter.Key())] = iter.Value()
			}
		}

		*err = e.handleBatchSeekResponse(kvMap)
		if *err != nil {
			return
		}
	}
}

func (e *AnalyzeSampleExec) handleScanTasks(bo *tikv.Backoffer) (keysSize int, err error) {
	snapshot, err := e.ctx.GetStore().(tikv.Storage).GetSnapshot(kv.MaxVersion)
	if err != nil {
		return 0, err
	}
	if e.ctx.GetSessionVars().GetReplicaRead().IsFollowerRead() {
		snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}
	for _, t := range e.scanTasks {
		iter, err := snapshot.Iter(t.StartKey, t.EndKey)
		if err != nil {
			return keysSize, err
		}
		size, err := e.handleScanIter(iter)
		keysSize += size
		if err != nil {
			return keysSize, err
		}
	}
	return keysSize, nil
}

func (e *AnalyzeSampleExec) handleBatchSeekResponse(kvMap map[string][]byte) (err error) {
	length := int32(len(kvMap))
	newCursor := atomic.AddInt32(&e.sampCursor, length)
	hasPKInfo := 0
	if e.pkInfo != nil {
		hasPKInfo = 1
	}
	samplePos := newCursor - length
	for sKey, sValue := range kvMap {
		err = e.updateCollectorSamples(sValue, kv.Key(sKey), samplePos, hasPKInfo)
		if err != nil {
			return err
		}
		samplePos++
	}
	return nil
}

func (e *AnalyzeSampleExec) handleScanIter(iter kv.Iterator) (scanKeysSize int, err error) {
	hasPKInfo := 0
	if e.pkInfo != nil {
		hasPKInfo = 1
	}
	rander := rand.New(rand.NewSource(e.randSeed + int64(e.rowCount)))
	sampleSize := int64(e.sampleSize)

	for ; iter.Valid() && err == nil; err = iter.Next() {
		// reservoir sampling
		e.rowCount++
		scanKeysSize++
		randNum := rander.Int63n(int64(e.rowCount))
		if randNum > sampleSize && e.sampCursor == int32(sampleSize) {
			continue
		}

		p := rander.Int31n(int32(sampleSize))
		if e.sampCursor < int32(sampleSize) {
			p = e.sampCursor
			e.sampCursor++
		}

		err = e.updateCollectorSamples(iter.Value(), iter.Key(), p, hasPKInfo)
		if err != nil {
			return
		}
	}
	return
}

func (e *AnalyzeSampleExec) updateCollectorSamples(sValue []byte, sKey kv.Key, samplePos int32, hasPKInfo int) (err error) {
	// Decode the cols value in order.
	var values map[int64]types.Datum
	values, err = e.decodeValues(sValue)
	if err != nil {
		return err
	}
	var rowID int64
	rowID, err = tablecodec.DecodeRowKey(sKey)
	if err != nil {
		return err
	}
	// Update the primary key collector.
	if hasPKInfo > 0 {
		v, ok := values[e.pkInfo.ID]
		if !ok {
			var key int64
			_, key, err = tablecodec.DecodeRecordKey(sKey)
			if err != nil {
				return err
			}
			v = types.NewIntDatum(key)
		}
		if mysql.HasUnsignedFlag(e.pkInfo.Flag) {
			v.SetUint64(uint64(v.GetInt64()))
		}
		if e.collectors[0].Samples[samplePos] == nil {
			e.collectors[0].Samples[samplePos] = &SampleItem{}
		}
		e.collectors[0].Samples[samplePos].RowID = rowID
		e.collectors[0].Samples[samplePos].Value = v
	}
	// Update the columns' collectors.
	for j, colInfo := range e.colsInfo {
		v, err := e.getValueByInfo(colInfo, values)
		if err != nil {
			return err
		}
		if e.collectors[hasPKInfo+j].Samples[samplePos] == nil {
			e.collectors[hasPKInfo+j].Samples[samplePos] = &SampleItem{}
		}
		e.collectors[hasPKInfo+j].Samples[samplePos].RowID = rowID
		e.collectors[hasPKInfo+j].Samples[samplePos].Value = v
	}
	// Update the indexes' collectors.
	for j, idxInfo := range e.idxsInfo {
		idxVals := make([]types.Datum, 0, len(idxInfo.Columns))
		for _, idxCol := range idxInfo.Columns {
			for _, colInfo := range e.colsInfo {
				if colInfo.Name == idxCol.Name {
					v, err := e.getValueByInfo(colInfo, values)
					if err != nil {
						return err
					}
					idxVals = append(idxVals, v)
					break
				}
			}
		}
		var bytes []byte
		bytes, err = codec.EncodeKey(e.ctx.GetSessionVars().StmtCtx, bytes, idxVals...)
		if err != nil {
			return err
		}
		if e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos] == nil {
			e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos] = &SampleItem{}
		}
		e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos].RowID = rowID
		e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos].Value = types.NewBytesDatum(bytes)
	}
	return nil
}

func (e *AnalyzeSampleExec) decodeValues(sValue []byte) (values map[int64]types.Datum, err error) {
	colID2FieldTypes := make(map[int64]*types.FieldType, len(e.colsInfo))
	if e.pkInfo != nil {
		colID2FieldTypes[e.pkInfo.ID] = &e.pkInfo.FieldType
	}
	for _, col := range e.colsInfo {
		colID2FieldTypes[col.ID] = &col.FieldType
	}
	return tablecodec.DecodeRow(sValue, colID2FieldTypes, e.ctx.GetSessionVars().Location())
}

func (e *AnalyzeSampleExec) getValueByInfo(colInfo *model.ColumnInfo, values map[int64]types.Datum) (types.Datum, error) {
	val, ok := values[colInfo.ID]
	if !ok {
		return table.GetColOriginDefaultValue(e.ctx, colInfo)
	}
	return val, nil
}
