package yabf

import (
	"bufio"
	g "github.com/hhkbp2/yabf/generator"
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

	Println("YCSB Client 0.1")
	Println("Command line: ")
	Println(strings.Join(os.Args, " "))
	Println("")
	Println("Loading workload...")

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
			Println(" (might take a minutes for large data sets)")
		}
	}()
	SetMeasurementProperties(props)
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
	Println("Starting test.")
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
		if i < (opCount % threadCount) {
			threadOpCount++
		}
		worker := NewWorker(db, workload, props, self.DoTransactions, opCount, targetPerThreadPerMS, workerCh, resultCh)
		workers = append(workers, worker)
		go worker.run()
	}

	stopCh := make(chan int, 1)
	waitGroup := &sync.WaitGroup{}
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
		reporter := NewStatusReporter(workers, stopCh, waitGroup, standardStatus, statusIntervalSeconds)
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

	err = exportMeasurements(props, total, (endTime-startTime)/1000)
	if err != nil {
		ExitOnError("could not export measurements, error: %s", err)
	}
}

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
		EPrintln("worker routine fail to init db, error: %s", err)
		self.resultCh <- 0
		return
	}
	workloadState, err := self.workload.InitRoutine(self.props)
	if err != nil {
		EPrintln("workload fail to init routine, error: %s", err)
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
	self.measurements.SetIntendedStartTime(0)
	if err = self.db.Cleanup(); err != nil {
		EPrintln("cleanup database error: %s", err)
	}
}

func waitUtil(to int64) {
	now := NowNS()
	if now < to {
		time.Sleep(time.Duration(to - now))
	}
}

func (self *Worker) throttleNanos(startTime int64) {
	if self.targetOpsPerMS > 0 {
		deadline := startTime + self.opDone*self.targetOpsTickNS
		waitUtil(deadline)
		self.measurements.SetIntendedStartTime(deadline)
	}
}

func (self *Worker) getOpsDone() int64 {
	return self.opDone
}

func (self *Worker) getOpsTodo() int64 {
	todo := self.opCount - self.opDone
	if todo < 0 {
		return 0
	}
	return todo
}

type StatusReporter struct {
	workers        []*Worker
	stopCh         chan int
	waitGroup      *sync.WaitGroup
	standardStatus bool
	sleepTimeNS    int64
}

func NewStatusReporter(workers []*Worker, stopCh chan int, waitGroup *sync.WaitGroup, standardStatus bool, intervalSeconds int64) *StatusReporter {
	sleepTimeNS := intervalSeconds * 1000 * 1000 * 1000
	return &StatusReporter{
		workers:        workers,
		stopCh:         stopCh,
		waitGroup:      waitGroup,
		standardStatus: standardStatus,
		sleepTimeNS:    sleepTimeNS,
	}
}

func (self *StatusReporter) run() {
	defer self.waitGroup.Done()

	startTimeMS := NowMS()
	startTimeNS := NowNS()
	deadline := startTimeNS + self.sleepTimeNS
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
	self.computeStats(startTimeMS, startIntervalMS, NowMS(), lastTotalOps)
}

func (self *StatusReporter) computeStats(startTimeMS int64, startIntervalMS int64, endIntervalMS int64, lastTotalOps int64) int64 {
	// TODO
	return lastTotalOps
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

func checkRequiredProperties(props Properties) bool {
	workload, ok := props[PropertyWorkload]
	if (!ok) || (len(workload) == 0) {
		EPrintln("Missing property: %s", PropertyWorkload)
		return false
	}
	return true
}

func exportMeasurements(props Properties, opCount, runtime int64) error {
	var f *os.File
	propStr, ok := props[PropertyExportFile]
	var err error
	if ok && (len(propStr) > 0) {
		f, err = os.Open(propStr)
		if err != nil {
			return err
		}
	} else {
		f = os.Stdout
	}

	propStr = props.GetDefault(PropertyExporter, PropertyExporterDefault)
	exporter, err := NewMeasurementExporter(propStr, f)
	if err != nil {
		EPrintln("Could not find exporter %s, will use default text exporter.", propStr)
		exporter = NewTextMeasurementExporter(f)
	}
	defer exporter.Close()
	exporter.Write("OVERALL", "RunTime(ms)", runtime)
	throughput := float64(opCount) * 1000.0 / float64(runtime)
	exporter.Write("OVERALL", "Throughput(ops/sec)", throughput)
	GetMeasurements().ExportMeasurements(exporter)
	return nil
}

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
	Println("YABF Command Line Client")
	Println(`Type "help" for command line help`)

	db, err := NewDB(self.args.Database, self.args.Properties)
	if err != nil {
		ExitOnError("fail to create specified db, error: %s", err)
	}
	db.SetProperties(self.args.Properties)
	err = db.Init()
	if err != nil {
		ExitOnError("fail to init db, error: %s", err)
	}

	Println("Connected.")
	scanner := bufio.NewScanner(os.Stdin)
	tableName := PropertyTableNameDefault
	for {
		Printf("> ")
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
					Println(`Using table "%s"`, tableName)
				case 2:
					tableName = parts[1]
					Println(`Using table "%s"`, tableName)
				default:
					Println(`Error: syntax is "table tablename"`)
				}
			case "read":
				switch length {
				case 1:
					Println(`Error: syntax is "read keyname [field1 field2 ...]"`)
				default:
					key := parts[1]
					fields := make([]string, 0, length-2)
					for i := 2; i < length; i++ {
						fields = append(fields, parts[i])
					}
					ret, status := db.Read(tableName, key, fields)
					Println("Return code: %s", status)
					for k, v := range ret {
						Println("%s=%s", k, v)
					}
				}
			case "scan":
				if length < 3 {
					EPrintln(`Error: syntax is "scan keyname scanlength [field1 field2 ...]"`)
				} else {
					key := parts[1]
					scanLength, err := strconv.ParseInt(parts[2], 0, 64)
					if err != nil {
						EPrintln("invalid scanlength: %s", parts[2])
						break
					}
					fields := make([]string, 0, length-3)
					for i := 3; i < length; i++ {
						fields = append(fields, parts[i])
					}
					ret, status := db.Scan(tableName, key, scanLength, fields)
					Println("Return code: %s", status)
					if len(ret) == 0 {
						Println("0 records")
					} else {
						Println("--------------------------------")
						count := 0
						for _, kv := range ret {
							Println("Record %d", count)
							count++
							for k, v := range kv {
								Println("%s=%s", k, v)
							}
							Println("--------------------------------")
						}
					}
				}
			case "update":
				if length < 3 {
					EPrintln(`Error: syntax is "update keyname name1=value1 [name2=value2 ...]"`)
				} else {
					key := parts[1]
					values := make(map[string]Binary)
					for i := 2; i < length; i++ {
						nv := strings.Split(parts[i], "=")
						if len(nv) != 2 {
							EPrintln(`Error: invalid name=value %s`, parts[i])
							break READLINE
						}
						values[nv[0]] = []byte(nv[1])
					}
					status := db.Update(tableName, key, values)
					Println("Result: %s", status)
				}
			case "insert":
				if length < 3 {
					EPrintln(`Error: syntax is "insert keyname name1=value1 [name2=value2 ...]"`)
				} else {
					key := parts[1]
					values := make(map[string]Binary)
					for i := 2; i < length; i++ {
						nv := strings.Split(parts[i], "=")
						if len(nv) != 2 {
							EPrintln(`Error: invalid name=value %s`, parts[i])
							break READLINE
						}
						values[nv[0]] = []byte(nv[1])
					}
					status := db.Insert(tableName, key, values)
					Println("Result: %s", status)
				}
			case "delete":
				if length != 2 {
					EPrintln(`Error: syntax is "delete keyname"`)
				} else {
					status := db.Delete(tableName, parts[1])
					Println("Result: %s", status)
				}
			default:
				EPrintln(`Error: unknown command "%s"`, parts[0])
			}
		}
		Println("%d ms", NowMS()-startTime)
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
	Println(helpFormat)
}
