package executor

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv"
)

// AnalyzeSample get the sample for the place using, it is for one table
func AnalyzeSample(ctx sessionctx.Context, histColl *statistics.HistColl, columnID int64, isIndex bool, sampleSize uint64, fulltable bool) error {

	taskCh := make(chan *analyzeSampleTask)
	resultCh := make(chan analyzeSampleResult)

	buildAnalyzeSampleTask(ctx, histColl, columnID, isIndex, sampleSize, fulltable, taskCh)

	// 采样的并发度写死为 1
	concurrency := 1
	for i := 0; i < concurrency; i++ {
		go analyzeSampleWorker(taskCh, resultCh, i == 0)
	}
	close(taskCh)

	// 接收结果
	panicCnt := 0
	for panicCnt < concurrency {
		result, ok := <-resultCh
		if !ok {
			break
		}
		if result.Err != nil {
			if result.Err == errAnalyzeWorkerPanic {
				panicCnt++
			}
			continue
		}

		// 处理结果, 直接写到 histColl 中
		if result.IsIndex == 1 {
			histColl.Indices[result.Sample[0].SID].SampleC = result.Sample[0]
		} else {
			for _, samplec := range result.Sample {
				histColl.Columns[columnID].SampleC = samplec
			}
		}
	}
	return nil
}

type analyzeSampleTask struct {
	sampleExec *AnalyzeSampleExec
	//job        *statistics.AnalyzeJob
}

// AnalyzeSampleExec is a executor used to sampling
type AnalyzeSampleExec struct {
	AnalyzeFastExec
	sampleSize uint64
}

type analyzeSampleResult struct {
	PhysicalTableID int64
	Sample          []*statistics.SampleC
	//Count           int64
	IsIndex int
	Err     error
	job     *statistics.AnalyzeJob
}

// build 一个 AnalyzeSampleExec，对应于 executor/builder 中的工作
// 并且将新的 analyzeSampleTask 装入其中
func buildAnalyzeSampleTask(ctx sessionctx.Context, histColl *statistics.HistColl, columnID int64, isIndex bool, sampleSize uint64, fulltable bool, taskCh chan *analyzeSampleTask) {
	statsHandle := domain.GetDomain(ctx).StatsHandle()
	physicalID := histColl.PhysicalID
	table := statsHandle.GetTableByPID(GetInfoSchema(ctx), physicalID)
	tableInfo := table.Meta()
	cloumsInfos := tableInfo.Columns
	indexInfos := tableInfo.Indices
	pkInfo := tableInfo.GetPkColInfo()
	concurrency := 1

	if fulltable {
		// TODO
		return
	}

	var sampleExec AnalyzeSampleExec
	sampleExec.ctx = ctx
	sampleExec.physicalTableID = physicalID
	sampleExec.tblInfo = tableInfo
	sampleExec.concurrency = concurrency
	sampleExec.wg = &sync.WaitGroup{}
	sampleExec.sampleSize = sampleSize
	if isIndex {
		// 只做单个对 idx 的任务
		sampleExec.idxsInfo = []*model.IndexInfo{indexInfos[columnID]}
		taskCh <- &analyzeSampleTask{
			sampleExec: &sampleExec,
		}
	} else {
		// 做单个对表上所有列的任务
		sampleExec.colsInfo = cloumsInfos
		sampleExec.pkInfo = pkInfo
		taskCh <- &analyzeSampleTask{
			sampleExec: &sampleExec,
		}
	}
}

// analyzeWorker 负责处理 TaskCh 中的所有 analyzeTask
func analyzeSampleWorker(taskCh <-chan *analyzeSampleTask, resultCh chan<- analyzeSampleResult, isCloseChanThread bool) {
	// 从 TaskCh 中接收 analyzeTask
	// ResultCh <-analyzeSample()
	for {
		task, ok := <-taskCh
		if !ok {
			break
		}
		//task.job.Start()
		//task.sampleExec.job = task.job

		for _, result := range analyzeSampleExec(task.sampleExec) {
			resultCh <- result
		}
	}
}

// -----
// 处理单个表的采样，返回所有列上的统计信息
// 由于对于一个表的采样可能是idx采样和colm的采样，所以可能需要返回 2 个 analyzeResult
func analyzeSampleExec(exec *AnalyzeSampleExec) []analyzeSampleResult {
	sample, err := exec.buildSample()
	if err != nil {
		return []analyzeSampleResult{{Err: err, job: exec.job}}
	}
	var results []analyzeSampleResult
	hasPKInfo := 0
	if exec.pkInfo != nil {
		hasPKInfo = 1
	}
	// 处理存在索引的情况
	if len(exec.idxsInfo) > 0 {
		for i := hasPKInfo + len(exec.idxsInfo); i < len(sample); i++ {
			idxResult := analyzeSampleResult{
				PhysicalTableID: exec.physicalTableID,
				Sample:          []*statistics.SampleC{sample[i]},
				IsIndex:         1,
				job:             exec.job,
			}
			results = append(results, idxResult)
		}
	}

	// 处理列上的采集
	if len(exec.colsInfo) > 0 {
		colResult := analyzeSampleResult{
			PhysicalTableID: exec.physicalTableID,
			Sample:          sample[:hasPKInfo+len(exec.idxsInfo)],
			job:             exec.job,
		}
		results = append(results, colResult)
	}
	return results
}

// 构建采样，需要将 AnalyzeResult 中 Sample 传进去
func (e *AnalyzeSampleExec) buildSample() (sample []*statistics.SampleC, err error) {
	// 测试需要所以默认值为 1
	// if RandSeed == 1 {
	// 	e.randSeed = time.Now().UnixNano()
	// } else {
	// 	e.randSeed = RandSeed
	// }
	e.randSeed = 1
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

	defer e.job.Update(int64(e.rowCount))

	// 如果表的总行数小于样本量的2倍,直接全表扫描
	if e.rowCount < e.sampleSize*2 {
		for _, task := range e.sampTasks {
			e.scanTasks = append(e.scanTasks, task.Location)
		}
		e.sampTasks = e.sampTasks[:0]
		e.rowCount = 0
		return e.runTasks()
	}

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

func (e *AnalyzeSampleExec) runTasks() ([]*statistics.SampleC, error) {
	errs := make([]error, e.concurrency)
	hasPKInfo := 0
	if e.pkInfo != nil {
		hasPKInfo = 1
	}

	length := len(e.colsInfo) + hasPKInfo + len(e.idxsInfo)
	e.collectors = make([]*statistics.SampleCollector, length)
	for i := range e.collectors {
		e.collectors[i] = &statistics.SampleCollector{
			MaxSampleSize: int64(e.sampleSize),
			Samples:       make([]*statistics.SampleItem, e.sampleSize),
		}
	}

	e.wg.Add(e.concurrency)
	bo := tikv.NewBackoffer(context.Background(), 500)
	for i := 0; i < e.concurrency; i++ {
		go e.handleSampTasks(bo, i, &errs[i])
	}
	e.wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	_, err := e.handleScanTasks(bo)
	if err != nil {
		return nil, err
	}

	stats := domain.GetDomain(e.ctx).StatsHandle()
	rowCount := int64(e.rowCount)
	if stats.Lease() > 0 {
		rowCount = mathutil.MinInt64(stats.GetTableStats(e.tblInfo).Count, rowCount)
	}

	//fmt
	fmt.Println("sampleSize = ", e.sampleSize)

	// 生成采样结果
	samples := make([]*statistics.SampleC, length)
	for i := 0; i < length; i++ {
		// 生成收集器属性
		collector := e.collectors[i]
		collector.Samples = collector.Samples[:e.sampCursor]
		sort.Slice(collector.Samples, func(i, j int) bool { return collector.Samples[i].RowID < collector.Samples[j].RowID })
		collector.CalcTotalSize()

		rowCount = mathutil.MaxInt64(rowCount, int64(len(collector.Samples)))
		collector.TotalSize *= rowCount / int64(len(collector.Samples))

		if i < hasPKInfo {
			samples[i], err = e.buildICSample(e.pkInfo.ID, e.collectors[i])
		} else if i < hasPKInfo+len(e.colsInfo) {
			samples[i], err = e.buildICSample(e.colsInfo[i-hasPKInfo].ID, e.collectors[i])
		} else {
			samples[i], err = e.buildICSample(e.idxsInfo[i-hasPKInfo-len(e.colsInfo)].ID, e.collectors[i])
		}
		if err != nil {
			return nil, err
		}
	}
	return samples, nil
}

func (e *AnalyzeSampleExec) buildICSample(colID int64, collector *statistics.SampleCollector) (*statistics.SampleC, error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	sampleItems := collector.Samples
	err := statistics.SortSampleItems(sc, sampleItems)
	if err != nil {
		return nil, err
	}
	var sample *statistics.SampleC
	for i := 0; i < len(sampleItems); i++ {
		sample.SampleColumn.AppendBytes(sampleItems[i].Value.GetBytes())
	}
	sample.SID = colID
	return sample, nil
}

// func (e *AnalyzeSampleExec) buildColumnSample(ID int64, collector *statistics.SampleCollector, tp *types.FieldType, rowCount int64) (*chunk.Column, error) {
// 	return nil, nil
// }
