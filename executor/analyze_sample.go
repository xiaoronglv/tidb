package executor

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// step0 定义全局触发方法，相当于 executor 的 Next()，方便之后代码复用
// step1 定义一个执行器
// step2 复用现有的执行计划 Task(plannercore/)、analyzeTask、analyzeResult
// step3 定义一个 worker 函数启动多个线程执行 sampleing 过程
// AnalyzeSample 的 open 暂不实现

// 触发的入口 (对应 AnalyzeExec 的 Next()，同时放在那里作为触发)
func AnalyzeSample(e *AnalyzeExec, ctx context.Context, sampleSize uint64) error {
	// 获取表信息
	// 定义 TaskCh、ResultCh
	// build AnalyzeSampleExec(task,一个task对应一个exec)
	// 并发启(n)个 worker do Sampling
	// res <-ResultCh (返回一整个表的 Result)
	// 处理整合 res
	// 更新 cache (statsHandle.Update(GetInfoSchema(e.ctx)))
	// \---在 handle 中添加一个函数只做 cache(--statistics.Table) 的更新

	taskCh := make(chan *analyzeSampleTask, len(e.tasks))
	resultCh := make(chan analyzeSampleResult, len(e.tasks))
	for _, task := range e.tasks {
		buildAnalyzeSampleTask(e, task, taskCh, sampleSize)
	}

	concurrency, err := getBuildStatsConcurrency(e.ctx)
	if err != nil {
		return err
	}

	e.wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go analyzeSampleWorker(taskCh, resultCh, i == 0)
	}
	for _, task := range e.tasks {
		statistics.AddNewAnalyzeJob(task.job)
	}
	close(taskCh)

	// 接收结果
	statsHandle := domain.GetDomain(e.ctx).StatsHandle()
	panicCnt := 0
	for panicCnt < concurrency {
		result, ok := <-resultCh
		if !ok {
			break
		}
		if result.Err != nil {
			err = result.Err
			if err == errAnalyzeWorkerPanic {
				panicCnt++
			} else {
				logutil.Logger(ctx).Error("analyze failed", zap.Error(err))
			}
			result.job.Finish(true)
			continue
		}
		// 处理结果
		// tbl := statsHandle.GetTableStats()
	}
	return nil
}

type analyzeSampleTask struct {
	sampleExec *AnalyzeSampleExec
	job        *statistics.AnalyzeJob
}

type AnalyzeSampleExec struct {
	AnalyzeFastExec
	sampleSize uint64
}

type analyzeSampleResult struct {
	PhysicalTableID int64
	Sample          []*chunk.Column
	//Count           int64
	IsIndex int
	Err     error
	job     *statistics.AnalyzeJob
}

// build 一个 AnalyzeSampleExec，对应于 executor/builder 中的工作
// 并且将新的 analyzeSampleTask 装入其中
func buildAnalyzeSampleTask(e *AnalyzeExec, task *analyzeTask, taskCh chan *analyzeSampleTask, sampleSize uint64) {
	// 对应每个 A 的 task，建立一个 analyzeSampleTask
	// taskCh <-analyzeSampleTask

	var aspe AnalyzeSampleExec
	aspe.sampleSize = sampleSize
	aspe.ctx = e.ctx
	aspe.wg = &sync.WaitGroup{}

	// 一个 idx 对应一个任务，一个表的所有 col 对应一个任务
	switch task.taskType {
	case colTask:
		aspe.colsInfo = task.colExec.colsInfo
		aspe.concurrency = task.colExec.concurrency
		aspe.physicalTableID = task.colExec.physicalTableID
	case idxTask:
		aspe.idxsInfo = []*model.IndexInfo{task.idxExec.idxInfo}
		aspe.concurrency = task.idxExec.concurrency
	case fastTask:
		aspe.colsInfo = task.fastExec.colsInfo
		aspe.idxsInfo = task.fastExec.idxsInfo
		aspe.pkInfo = task.fastExec.pkInfo
		aspe.tblInfo = task.fastExec.tblInfo
	case pkIncrementalTask, idxIncrementalTask:
		fmt.Println("do nothing for Incremental")
	}

	taskCh <- &analyzeSampleTask{
		sampleExec: &aspe,
		job: &statistics.AnalyzeJob{
			DBName:        task.job.DBName,
			TableName:     task.job.TableName,
			PartitionName: task.job.PartitionName,
			JobInfo:       "sample analyze"},
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
		task.job.Start()
		task.sampleExec.job = task.job
		for _, result := range analyzeSampleExec(task.sampleExec) {
			resultCh <- result
		}
	}
}

//---- 🐵----//
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
				Sample:          []*chunk.Column{sample[i]},
				IsIndex:         1,
				job:             exec.job,
			}
			results = append(results, idxResult)
		}
	}
	// 处理列上的采集
	colResult := analyzeSampleResult{
		PhysicalTableID: exec.physicalTableID,
		Sample:          sample[:hasPKInfo+len(exec.idxsInfo)],
		job:             exec.job,
	}
	results = append(results, colResult)
	return nil
}

// 下面是 AnalyzeSampleExec 需要实现的一些接口函数 --> 对所有列或单个索引上的采样处理

// 构建采样，需要将 AnalyzeResult 中 Sample 传进去
func (e *AnalyzeSampleExec) buildSample() (sample []*chunk.Column, err error) {
	// 测试需要所以默认值为 1
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

func (e *AnalyzeSampleExec) runTasks() ([]*chunk.Column, error) {
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
	// fastAnalyzeHistogramScanKeys.Observe(float64(scanKeysSize))
	if err != nil {
		return nil, err
	}

	stats := domain.GetDomain(e.ctx).StatsHandle()
	rowCount := int64(e.rowCount)
	if stats.Lease() > 0 {
		rowCount = mathutil.MinInt64(stats.GetTableStats(e.tblInfo).Count, rowCount)
	}

	// 生成采样结果
	samples := make([]*chunk.Column, length)
	for i := 0; i < length; i++ {
		// 生成收集器属性
		collector := e.collectors[i]
		collector.Samples = collector.Samples[:e.sampCursor]
		sort.Slice(collector.Samples, func(i, j int) bool { return collector.Samples[i].RowID < collector.Samples[j].RowID })
		collector.CalcTotalSize()

		rowCount = mathutil.MaxInt64(rowCount, int64(len(collector.Samples)))
		collector.TotalSize *= rowCount / int64(len(collector.Samples))

		if i < hasPKInfo {
			samples[i], err = e.buildICSample(e.collectors[i])
		} else if i < hasPKInfo+len(e.colsInfo) {
			samples[i], err = e.buildICSample(e.collectors[i])
		} else {
			samples[i], err = e.buildICSample(e.collectors[i])
		}
		if err != nil {
			return nil, err
		}
	}
	return samples, nil
}

func (e *AnalyzeSampleExec) buildICSample(collector *statistics.SampleCollector) (*chunk.Column, error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	sampleItems := collector.Samples
	err := statistics.SortSampleItems(sc, sampleItems)
	if err != nil {
		return nil, err
	}
	var sample *chunk.Column
	for i := 0; i < len(sampleItems); i++ {
		sample.AppendBytes(sampleItems[i].Value.GetBytes())
	}
	return sample, nil
}

// func (e *AnalyzeSampleExec) buildColumnSample(ID int64, collector *statistics.SampleCollector, tp *types.FieldType, rowCount int64) (*chunk.Column, error) {
// 	return nil, nil
// }
