package yabf

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/hhkbp2/go-strftime"
	g "github.com/hhkbp2/yabf/generator"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Client interface {
	Main()
}

type ClientBase struct {
	Args           *Arguemnts
	DoTransactions bool
}

func NewClientBase(args *Arguemnts) *ClientBase {
	return &ClientBase{
		Args: args,
	}
}

func checkRequiredProperties(props Properties) bool {
	workload, ok := props[PropertyWorkload]
	if (!ok) || (len(workload) == 0) {
		EPrintf("Missing property: %s", PropertyWorkload)
		return false
	}
	return true
}

func (self *ClientBase) CheckProperties() {
	if !checkRequiredProperties(self.Args.Properties) {
		os.Exit(2)
	}
	propStr, ok := self.Args.Properties[PropertyTransactions]
	if ok {
		doTransactions, err := strconv.ParseBool(propStr)
		if err != nil {
			ExitOnError("invalid property %s=%s, should be bool", PropertyTransactions)
		}
		if doTransactions != self.DoTransactions {
			ExitOnError("property %s=%s conflicts with command %s", PropertyTransactions, propStr, self.Args.Command)
		}
	}
}

func (self *ClientBase) Main() {
	self.CheckProperties()

	props := self.Args.Properties
	propStr := props.GetDefault(PropertyMaxExecutionTime, PropertyMaxExecutionTimeDefault)
	maxExecutionTime, err := strconv.ParseInt(propStr, 0, 64)
	if err != nil {
		ExitOnError("invalid property %s=%s, should be integer", PropertyMaxExecutionTime, propStr)
	}
	// get number of threads, target and db
	propStr = props.GetDefault(PropertyThreadCount, PropertyThreadCountDefault)
	threadCount, err := strconv.ParseInt(propStr, 0, 64)
	if err != nil {
		ExitOnError("invalid property %s=%s, should be integer", PropertyThreadCount, propStr)
	}
	dbName := props.GetDefault(PropertyDB, PropertyDBDefault)
	propStr = props.GetDefault(PropertyTarget, PropertyTargetDefault)
	target, err := strconv.ParseInt(propStr, 0, 64)
	if err != nil {
		ExitOnError("invalid property %s=%s, should be integer", PropertyTarget, propStr)
	}
	targetPerThreadPerMS := float64(-1)
	if target > 0 {
		targetPerThread := float64(target) / float64(threadCount)
		targetPerThreadPerMS = targetPerThread / 1000.0
	}

	Printf("YCSB Client 0.1")
	Infof("Command line: \n%s", strings.Join(os.Args, " "))
	Printf("Loading workload...")

	// show a warning message that creating the workload is taking a while
	// but only do so if it is taking longer than 2 seconds
	// (showing the message right away if the setup wasn't taking very long
	// was confusing people)
	warningCh := make(chan int, 1)
	go func() {
		waitTime := time.Second * 2
		select {
		case <-warningCh:
			return
		case <-time.After(waitTime):
			Printf(" (might take a minutes for large data sets)")
		}
	}()
	// set up measurements
	SetMeasurementProperties(props)
	// load the workload
	workloadName := props.Get(PropertyWorkload)
	workload, err := NewWorkload(workloadName)
	if err != nil {
		ExitOnError("%s", err)
	}
	if err = workload.Init(props); err != nil {
		ExitOnError("%s", err)
	}
	warningCh <- 1

	// run the workload
	Printf("Starting test.")
	var opCount int64
	if self.Args.Command == "run" {
		propStr = props.GetDefault(PropertyOperationCount, PropertyOperationCountDefault)
		opCount, err = strconv.ParseInt(propStr, 0, 64)
		if err != nil {
			ExitOnError("invalid property %s=%s, should be integer", PropertyOperationCount, propStr)
		}
	} else {
		propStr, ok := props[PropertyInsertCount]
		if ok {
			opCount, err = strconv.ParseInt(propStr, 0, 64)
		} else {
			opCount, _ = strconv.ParseInt(PropertyRecordCountDefault, 0, 64)
		}
	}

	resultCh := make(chan int64, threadCount)
	// init all worker routines
	workerCh := make(chan int, threadCount)
	workers := make([]*Worker, 0, threadCount)
	startTime := NowNS()
	for i := int64(0); i < threadCount; i++ {
		db, err := NewDB(dbName, props)
		if err != nil {
			ExitOnError("fail to create db, error: %s", err)
		}
		threadOpCount := opCount / threadCount
		// ensure correct number of operations, in case opCount is not a multiple of threadCount
		if i < (opCount % threadCount) {
			threadOpCount++
		}
		worker := NewWorker(db, workload, props, self.DoTransactions, opCount, targetPerThreadPerMS, workerCh, resultCh)
		workers = append(workers, worker)
		go worker.run()
	}

	stopCh := make(chan int, 1)
	waitGroup := &sync.WaitGroup{}
	label := ""
	if l, ok := self.Args.Options["l"]; ok {
		label = l
	}
	_, status := self.Args.Options["s"]
	if status {
		standardStatus := false
		propStr, ok := props[PropertyMeasurementType]
		if ok && (propStr == "timeseries") {
			standardStatus = true
		}
		propStr = props.GetDefault(PropertyStatusInterval, PropertyStatusIntervalDefault)
		statusIntervalSeconds, err := strconv.ParseInt(propStr, 0, 64)
		if err != nil {
			ExitOnError("invalid property %s=%s, should be integer", PropertyStatusInterval, propStr)
		}
		reporter := NewStatusReporter(workers, stopCh, waitGroup, standardStatus, statusIntervalSeconds, label)
		waitGroup.Add(1)
		go reporter.run()
	}

	workerDoneCount := int64(0)
	total := int64(0)
	if maxExecutionTime > 0 {
		deadline := startTime + SecondToNanosecond(maxExecutionTime)
		now := NowNS()
		for (workerDoneCount < threadCount) && (now <= deadline) {
			select {
			case t := <-resultCh:
				total += t
				workerDoneCount++
			case <-time.After(time.Duration(deadline - now)):
				break
			}
			now = NowNS()
		}
	} else {
		for workerDoneCount < threadCount {
			select {
			case t := <-resultCh:
				total += t
				workerDoneCount++
			}
		}
	}

	// stop all worker routine
	for i := int64(0); i < threadCount; i++ {
		workerCh <- 1
	}

	if status {
		// stop status routine
		stopCh <- 1
		waitGroup.Wait()
	}
	// wait for all routine to stop
	for workerDoneCount < threadCount {
		select {
		case t := <-resultCh:
			total += t
			workerDoneCount++
		}
	}

	endTime := NowNS()

	err = workload.Cleanup()
	if err != nil {
		ExitOnError("fail to cleanup workload, error: %s", err)
	}

	err = exportMeasurements(props, total, NanosecondToMillisecond(endTime-startTime))
	if err != nil {
		ExitOnError("could not export measurements, error: %s", err)
	}
}

// A routine for executing transactions or data inserts to the database.
type Worker struct {
	db                   DB
	workload             Workload
	props                Properties
	doTransactions       bool
	opCount              int64
	targetPerThreadPerMS float64
	targetOpsPerMS       float64
	targetOpsTickNS      int64
	opDone               int64
	stopCh               chan int
	resultCh             chan int64
	measurements         Measurements
}

func NewWorker(db DB, workload Workload, props Properties, doTransactions bool, opCount int64, targetPerThreadPerMS float64, stopCh chan int, resultCh chan int64) *Worker {
	targetOpsPerMS := targetPerThreadPerMS
	targetOpsTickNS := int64(1000000.0 / targetOpsPerMS)
	return &Worker{
		db:                   db,
		workload:             workload,
		props:                props,
		opCount:              opCount,
		doTransactions:       doTransactions,
		targetPerThreadPerMS: targetPerThreadPerMS,
		targetOpsPerMS:       targetOpsPerMS,
		targetOpsTickNS:      targetOpsTickNS,
		stopCh:               stopCh,
		resultCh:             resultCh,
		measurements:         GetMeasurements(),
	}
}

func (self *Worker) run() {
	defer func() {
		self.resultCh <- self.opDone
	}()

	if err := self.db.Init(); err != nil {
		EPrintf("worker routine fail to init db, error: %s", err)
		self.resultCh <- 0
		return
	}
	workloadState, err := self.workload.InitRoutine(self.props)
	if err != nil {
		EPrintf("workload fail to init routine, error: %s", err)
		return
	}
	// NOTE: switching to using nano seconds for time management here such that
	// the measurements and the routine have the save view on time.
	// spread the thread operations out so they don't all hit the DB at the
	// same time.
	if (self.targetOpsPerMS > 0) && (self.targetOpsPerMS <= 1.0) {
		randomMinorDelay := g.NextInt64(self.targetOpsTickNS)
		time.Sleep(time.Duration(int64(time.Nanosecond) * randomMinorDelay))
	}

	startTime := NowNS()
WORKER_LOOP:
	for (self.opCount == 0) || (self.opDone < self.opCount) {
		select {
		case <-self.stopCh:
			break WORKER_LOOP
		default:
			if self.doTransactions {
				if !self.workload.DoTransaction(self.db, workloadState) {
					break WORKER_LOOP
				}
			} else {
				if !self.workload.DoInsert(self.db, workloadState) {
					break WORKER_LOOP
				}
			}
			self.opDone++
			self.throttleNanos(startTime)
		}
	}
	if err = self.db.Cleanup(); err != nil {
		EPrintf("cleanup database error: %s", err)
	}
}

// Waits util the deadline time.
func waitUtil(to int64) {
	now := NowNS()
	if now < to {
		time.Sleep(time.Duration(to - now))
	}
}

func (self *Worker) throttleNanos(startTime int64) {
	if self.targetOpsPerMS > 0 {
		// delay until next tick
		deadline := startTime + self.opDone*self.targetOpsTickNS
		waitUtil(deadline)
	}
}

// the total amount of work this routine is still expected to do.
func (self *Worker) getOpsDone() int64 {
	return self.opDone
}

// the operations left for this routine to do.
func (self *Worker) getOpsTodo() int64 {
	todo := self.opCount - self.opDone
	if todo < 0 {
		return 0
	}
	return todo
}

// A routine to periodically show the status of the experiement, to reassure
// you that process is being made.
type StatusReporter struct {
	// the worker routines that are running
	workers        []*Worker
	stopCh         chan int
	waitGroup      *sync.WaitGroup
	standardStatus bool
	// the interval for reporting status
	sleepTimeNS int64
	label       string
}

func NewStatusReporter(workers []*Worker, stopCh chan int, waitGroup *sync.WaitGroup, standardStatus bool, intervalSeconds int64, label string) *StatusReporter {
	return &StatusReporter{
		workers:        workers,
		stopCh:         stopCh,
		waitGroup:      waitGroup,
		standardStatus: standardStatus,
		sleepTimeNS:    SecondToNanosecond(intervalSeconds),
		label:          label,
	}
}

// Run and periodically report status.
func (self *StatusReporter) run() {
	defer self.waitGroup.Done()

	startTimeNS := NowNS()
	deadline := startTimeNS + self.sleepTimeNS
	startTimeMS := NowMS()
	startIntervalMS := startTimeMS

	lastTotalOps := int64(0)
REPORTER_LOOP:
	for {
		select {
		case <-self.stopCh:
			break REPORTER_LOOP
		default:
			nowMS := NowMS()
			lastTotalOps = self.computeStats(startTimeMS, startIntervalMS, nowMS, lastTotalOps)
			waitUtil(deadline)
			startIntervalMS = nowMS
			deadline += self.sleepTimeNS
		}
	}
	// Print the final stats.
	self.computeStats(startTimeMS, startIntervalMS, NowMS(), lastTotalOps)
}

// Computes and prints the stats.
func (self *StatusReporter) computeStats(startTimeMS int64, startIntervalMS int64, endIntervalMS int64, lastTotalOps int64) int64 {
	var totalOps, todoOps int64
	// Calculate the total number of operations completed.
	for _, worker := range self.workers {
		totalOps += worker.getOpsDone()
		todoOps += worker.getOpsTodo()
	}
	interval := endIntervalMS - startTimeMS
	throughput := 1000.0 * float64(totalOps) / float64(interval)
	currentThrough := 1000.0 * float64(totalOps-lastTotalOps) / float64(endIntervalMS-startIntervalMS)
	estimateRemaining := math.Ceil(float64(todoOps) / throughput)

	if interval <= 0 {
		// don't output status at startup
		return totalOps
	}
	var buf bytes.Buffer
	timestamp := strftime.Format("%Y-%m-%d %H:%M:%S:%3n", time.Now())
	buf.WriteString(fmt.Sprintf("%s%s %d sec: %d operations; ", self.label, timestamp, MillisecondToSecond(interval), totalOps))
	if totalOps != 0 {
		buf.WriteString(fmt.Sprintf("%.2f current ops/sec; ", currentThrough))
	}
	if todoOps != 0 {
		buf.WriteString(fmt.Sprintf("est completion in %s; ", formatRemaining(int64(estimateRemaining))))
	}
	buf.WriteString(GetMeasurements().GetSummary())
	Printf(buf.String())
	if self.standardStatus {
		Printf(buf.String())
	}
	return totalOps
}

// Turn seonds remaining into more usefull units.
// i.e. if there are hours or days worth of seconds, use them.
func formatRemaining(seconds int64) string {
	var buf bytes.Buffer
	d := time.Duration(seconds * int64(time.Second))
	hours := int64(d.Hours())
	days := hours / 24
	hours = hours % 24
	minutes := int64(d.Minutes()) % 60
	allSeconds := int64(d.Seconds())
	if days > 0 {
		buf.WriteString(fmt.Sprintf("%d days", days))
	}
	if hours > 0 {
		buf.WriteString(fmt.Sprintf("%d hours", hours))
	}
	// Only include minute granularity if we're < 1 day
	if (days < 1) && (minutes > 0) {
		buf.WriteString(fmt.Sprintf("%d minutes", minutes))
	}
	if allSeconds < 60 {
		seconds = allSeconds % 60
		buf.WriteString(fmt.Sprintf("%d seconds", seconds))
	}
	return buf.String()
}

type Loader struct {
	*ClientBase
}

func NewLoader(args *Arguemnts) *Loader {
	object := &Loader{
		ClientBase: NewClientBase(args),
	}
	object.DoTransactions = false
	return object
}

type Runner struct {
	*ClientBase
}

func NewRunner(args *Arguemnts) *Runner {
	object := &Runner{
		ClientBase: NewClientBase(args),
	}
	object.DoTransactions = true
	return object
}

// Exports the measurements to either stdout or a file using the exporter
// specified by conf.
func exportMeasurements(props Properties, opCount, runtime int64) error {
	var f *os.File
	propStr, ok := props[PropertyExportFile]
	var err error
	// if no destination file is specified then the results will be written to stdout.
	if ok && (len(propStr) > 0) {
		f, err = os.Open(propStr)
		if err != nil {
			return err
		}
	} else {
		f = os.Stdout
	}

	// if no exporter is provided then the default text one will be used
	propStr = props.GetDefault(PropertyExporter, PropertyExporterDefault)
	exporter, err := NewMeasurementExporter(propStr, f)
	if err != nil {
		EPrintf("Could not find exporter %s, will use default text exporter.", propStr)
		exporter = NewTextMeasurementExporter(f)
	}
	defer exporter.Close()
	exporter.Write("OVERALL", "RunTime(ms)", runtime)
	throughput := float64(opCount) * 1000.0 / float64(runtime)
	exporter.Write("OVERALL", "Throughput(ops/sec)", throughput)
	GetMeasurements().ExportMeasurements(exporter)
	return nil
}

// A simple command line client to a database, using the appropriate DB implementation.
type Shell struct {
	args *Arguemnts
}

func NewShell(args *Arguemnts) *Shell {
	return &Shell{
		args: args,
	}
}

var (
	regexCmd *regexp.Regexp
)

func init() {
	regexCmd = regexp.MustCompile(`\s+`)
}

func (self *Shell) Main() {
	Printf("YABF Command Line Client")
	Printf(`Type "help" for command line help`)

	db, err := NewDB(self.args.Database, self.args.Properties)
	if err != nil {
		ExitOnError("fail to create specified db, error: %s", err)
	}
	db.SetProperties(self.args.Properties)
	err = db.Init()
	if err != nil {
		ExitOnError("fail to init db, error: %s", err)
	}

	Printf("Connected.")
	scanner := bufio.NewScanner(os.Stdin)
	tableName := PropertyTableNameDefault
	for {
		PromptPrintf("> ")
		if !scanner.Scan() {
			break
		}
		startTime := NowMS()
		line := scanner.Text()
	READLINE:
		switch line {
		case "":
		case "help":
			self.help()
			continue
		case "quit":
			return
		default:
			parts := regexCmd.Split(line, -1)
			length := len(parts)
			switch parts[0] {
			case "table":
				switch length {
				case 1:
					Printf(`Using table "%s"`, tableName)
				case 2:
					tableName = parts[1]
					Printf(`Using table "%s"`, tableName)
				default:
					EPrintf(`Error: syntax is "table tablename"`)
				}
			case "read":
				switch length {
				case 1:
					EPrintf(`Error: syntax is "read keyname [field1 field2 ...]"`)
				default:
					key := parts[1]
					fields := make([]string, 0, length-2)
					for i := 2; i < length; i++ {
						fields = append(fields, parts[i])
					}
					ret, status := db.Read(tableName, key, fields)
					Printf("Return code: %s", status)
					for k, v := range ret {
						Printf("%s=%s", k, v)
					}
				}
			case "scan":
				if length < 3 {
					EPrintf(`Error: syntax is "scan keyname scanlength [field1 field2 ...]"`)
				} else {
					key := parts[1]
					scanLength, err := strconv.ParseInt(parts[2], 0, 64)
					if err != nil {
						EPrintf("invalid scanlength: %s", parts[2])
						break
					}
					fields := make([]string, 0, length-3)
					for i := 3; i < length; i++ {
						fields = append(fields, parts[i])
					}
					ret, status := db.Scan(tableName, key, scanLength, fields)
					Printf("Return code: %s", status)
					if len(ret) == 0 {
						Printf("0 records")
					} else {
						Printf("--------------------------------")
						count := 0
						for _, kv := range ret {
							Printf("Record %d", count)
							count++
							for k, v := range kv {
								Printf("%s=%s", k, v)
							}
							Printf("--------------------------------")
						}
					}
				}
			case "update":
				if length < 3 {
					EPrintf(`Error: syntax is "update keyname name1=value1 [name2=value2 ...]"`)
				} else {
					key := parts[1]
					values := make(map[string]Binary)
					for i := 2; i < length; i++ {
						nv := strings.Split(parts[i], "=")
						if len(nv) != 2 {
							EPrintf(`Error: invalid name=value %s`, parts[i])
							break READLINE
						}
						values[nv[0]] = []byte(nv[1])
					}
					status := db.Update(tableName, key, values)
					Printf("Result: %s", status)
				}
			case "insert":
				if length < 3 {
					EPrintf(`Error: syntax is "insert keyname name1=value1 [name2=value2 ...]"`)
				} else {
					key := parts[1]
					values := make(map[string]Binary)
					for i := 2; i < length; i++ {
						nv := strings.Split(parts[i], "=")
						if len(nv) != 2 {
							EPrintf(`Error: invalid name=value %s`, parts[i])
							break READLINE
						}
						values[nv[0]] = []byte(nv[1])
					}
					status := db.Insert(tableName, key, values)
					Printf("Result: %s", status)
				}
			case "delete":
				if length != 2 {
					EPrintf(`Error: syntax is "delete keyname"`)
				} else {
					status := db.Delete(tableName, parts[1])
					Printf("Result: %s", status)
				}
			default:
				EPrintf(`Error: unknown command "%s"`, parts[0])
			}
		}
		Printf("%d ms", NowMS()-startTime)
	}
}

func (self *Shell) help() {
	helpFormat := `Commands
  read key [field1 field2 ...] - Read a record
  scan key recordcount [field1 field2 ...] - Scan starting at key
  insert key name1=value1 [name2=value2 ...] - Insert a new record
  update key name1=value1 [name2=value2 ...] - Update a record
  delete key - Delete a record
  table [tablename] - Get or [set] the name of the table
  quit - Quit`
	Printf(helpFormat)
}
