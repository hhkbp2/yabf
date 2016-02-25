package yabf

import (
	"bytes"
	"fmt"
	g "github.com/hhkbp2/yabf/generator"
	"math"
	"math/rand"
	"strconv"
	"time"
)

type MakeWorkloadFunc func() Workload

var (
	Workloads map[string]MakeWorkloadFunc
)

func init() {
	Workloads = map[string]MakeWorkloadFunc{
		"CoreWorkload": func() Workload {
			return NewCoreWorkload()
		},
		"ConstantOccupancyWorkload": func() Workload {
			return NewConstantOccupancyWorkload()
		},
	}
}

func NewWorkload(className string) (Workload, error) {
	f, ok := Workloads[className]
	if !ok {
		return nil, g.NewErrorf("unsupported workload: %s", className)
	}
	w := f()
	return w, nil
}

// Workload represents One experiment scenario.
// One object of this type will be instantiated and
// shared among all client routines.
// This class should be constructed using a no-argument constructor,
// so we can load it dynamically. Any argument-based initialization
// should be done by init().
type Workload interface {
	// Initialize the scenario. Create any generators and other shared
	// objects here.
	// Called once in the main client routine, before any operations
	// are started.
	Init(p Properties) error

	// Initialize any state for a particular client routine.
	// Since the scenario object will be shared among all threads,
	// this is the place to create any state that is specific to one routine.
	// To be clear, this means the returned object should be created anew
	// on each call to InitRoutine(); do not return the same object multiple
	// times. The returned object will be passed to invocations of DoInsert()
	// and DoTransaction() for this routine. There should be no side effects
	// from this call; all state should be encapsulated in the returned object.
	// If you have no state to retain for this routine, return null.
	// (But if you have no state to retain for this routine, probably
	// you don't need to override this function.)
	InitRoutine(p Properties) (interface{}, error)

	// Cleanup the scenario.
	// Called once, in the main client routine, after all operations
	// have completed.
	Cleanup() error

	// Do one insert operation. Because it will be called concurrently from
	// multiple routines, this function must be routine safe.
	// However, avoid synchronized, or the routines will block waiting for
	// each other, and it will be difficult to reah the target throughput.
	// Ideally, this function would have no side effects other than
	// DB operations and mutations on object. Mutations to object do not need
	// to be synchronized, since each routine has its own object instance.
	DoInsert(db DB, object interface{}) bool

	// Do one transaction operation. Because it will be called concurrently
	// from multiple client routines, this function must be routine safe.
	// However, avoid synchronized, or the routines will block waiting for
	// each other, and it will be difficult to reach the target throughtput.
	// Ideally, this function would have no side effects other than
	// DB operations and mutations on object. Mutations to object do not need
	// to be synchronized, since each routine has its own object instance.
	DoTransaction(db DB, object interface{}) bool
}

// CoreWorkload represents the core benchmark scenario.
// It's a set of clients doing simple CRUD operations. The relative proportion
// of different kinds of operations, and other properties of the workload,
// are controlled by parameters specified at runtime.
// Properties to control the client:
//   fieldcount: the number of fields in a second (default: 10)
//   fieldlength: the size of each field (default: 100)
//   readallfields: should reads read all fields (true) or just one (false)
//                  (default: true)
//   writeallfields: should updates and read/modify/writes update all fields
//                   (true) or just one (false) (default: false)
//   readproportion: what proportion of operations should be reads
//                   (default: 0.95)
//   updateproportion: what proportion of operations should be updates
//                     (default: 0.05)
//   insertproportion: what porportion of operations should be inserts
//                     (default: 0)
//   scanproportion: what proportion of operations should be scans (default: 0)
//   readmodifywriteproportion: what proportion of operations should be read a
//                              record, modify it, write it back (default: 0)
//   requestdistribution: what distribution should be used to select the records
//                        to operate on - uniform, zipfian, hotspot or latest
//                        (default: uniform)
//   maxscanlength: for scans, what is the maximum number of records to scan
//                  (default: 1000)
//   scanlengthdistribution: for scans, what distribution should be used to
//                           choose the number of records to scan, for
//                           each scan, between 1 and maxscanlength
//                           (default: uniform)
//   insertorder: should records be inserted in order by key ("ordered"), or in
//                hashed order ("hashed") (default: hashed)
type CoreWorkload struct {
	table                        string
	fieldCount                   int64
	fieldNames                   []string
	fieldLengthGenerator         g.IntegerGenerator
	readAllFields                bool
	writeAllFields               bool
	dataIntegrity                bool
	keySequence                  g.IntegerGenerator
	operationChooser             *g.DiscreteGenerator
	keyChooser                   g.IntegerGenerator
	fieldChooser                 g.IntegerGenerator
	transactionInsertKeySequence *g.AcknowledgedCounterGenerator
	scanLengthChooser            g.IntegerGenerator
	orderedInserts               bool
	recordCount                  int64
	insertionRetryLimit          int64
	insertionRetryInterval       int64
	measurements                 Measurements
}

func NewCoreWorkload() *CoreWorkload {
	return &CoreWorkload{
		measurements: GetMeasurements(),
	}
}

func (self *CoreWorkload) Init(p Properties) error {
	table := p.GetDefault(PropertyTableName, PropertyTableNameDefault)

	propStr := p.GetDefault(PropertyFieldCount, PropertyFieldCountDefault)
	fieldCount, err := strconv.ParseInt(propStr, 0, 64)
	if err != nil {
		return err
	}
	fieldNames := make([]string, 0, fieldCount)
	for i := int64(0); i < fieldCount; i++ {
		fieldNames = append(fieldNames, fmt.Sprintf("field%d", i))
	}
	fieldLengthGenerator, err := self.getFieldLengthGenerator(p)
	if err != nil {
		return err
	}

	propStr = p.GetDefault(PropertyReadProportion, PropertyReadProportionDefault)
	readProportion, err := strconv.ParseFloat(propStr, 64)
	if err != nil {
		return err
	}
	propStr = p.GetDefault(PropertyUpdateProportion, PropertyUpdateProportionDefault)
	updateProportion, err := strconv.ParseFloat(propStr, 64)
	if err != nil {
		return err
	}
	propStr = p.GetDefault(PropertyInsertProportion, PropertyInsertProportionDefault)
	insertProportion, err := strconv.ParseFloat(propStr, 64)
	if err != nil {
		return err
	}
	propStr = p.GetDefault(PropertyScanProportion, PropertyScanProportionDefault)
	scanProportion, err := strconv.ParseFloat(propStr, 64)
	if err != nil {
		return err
	}
	propStr = p.GetDefault(PropertyReadModifyWriteProportion, PropertyReadModifyWriteProportionDefault)
	readModifyWriteProportion, err := strconv.ParseFloat(propStr, 64)
	if err != nil {
		return err
	}
	propStr = p.GetDefault(PropertyRecordCount, PropertyRecordCountDefault)
	recordCount, err := strconv.ParseInt(propStr, 0, 64)
	if err != nil {
		return err
	}
	if recordCount == 0 {
		recordCount = math.MaxInt32
	}
	requestDistrib := p.GetDefault(PropertyRequestDistribution, PropertyRequestDistributionDefault)
	propStr = p.GetDefault(PropertyMaxScanLength, PropertyMaxScanLengthDefault)
	maxScanLength, err := strconv.ParseInt(propStr, 0, 64)
	if err != nil {
		return err
	}
	scanLengthDistrib := p.GetDefault(PropertyScanLengthDistribution, PropertyScanLengthDistributionDefault)
	propStr = p.GetDefault(PropertyInsertStart, PropertyInsertStartDefault)
	insertStart, err := strconv.ParseInt(propStr, 0, 64)
	if err != nil {
		return err
	}
	propStr = p.GetDefault(PropertyReadAllFields, PropertyReadAllFieldsDefault)
	readAllFields, err := strconv.ParseBool(propStr)
	if err != nil {
		return err
	}
	propStr = p.GetDefault(PropertyWriteAllFields, PropertyWriteAllFieldsDefault)
	writeAllFields, err := strconv.ParseBool(propStr)
	if err != nil {
		return err
	}
	propStr = p.GetDefault(PropertyDataIntegrity, PropertyDataIntegrityDefault)
	dataIntegrity, err := strconv.ParseBool(propStr)
	if err != nil {
		return err
	}
	propStr = p.GetDefault(PropertyFieldLengthDistribution, PropertyFieldLengthDistributionDefault)
	isConstant := (propStr == "constant")
	if dataIntegrity && isConstant {
		return g.NewErrorf("must have constant field size to check data integrity")
	}
	propStr = p.GetDefault(PropertyInsertOrder, PropertyInsertOrderDefault)
	var orderedInserts bool
	var keyChooser g.IntegerGenerator
	if propStr == "hashed" {
		orderedInserts = false
	} else if requestDistrib == "exponential" {
		propStr = p.GetDefault(PropertyExponentialPercentile, PropertyExponentialPercentileDefault)
		percentile, err := strconv.ParseFloat(propStr, 64)
		if err != nil {
			return err
		}
		propStr = p.GetDefault(PropertyExponentialFraction, PropertyExponentialFractionDefault)
		fraction, err := strconv.ParseFloat(propStr, 64)
		if err != nil {
			return err
		}
		keyChooser = g.NewExponentialGenerator(percentile, float64(recordCount)*fraction)
	} else {
		orderedInserts = true
	}

	keySequence := g.NewCounterGenerator(insertStart)
	operationChooser := g.NewDiscreteGenerator()

	if readProportion > 0 {
		operationChooser.AddValue(readProportion, "READ")
	}
	if updateProportion > 0 {
		operationChooser.AddValue(updateProportion, "UPDATE")
	}
	if insertProportion > 0 {
		operationChooser.AddValue(insertProportion, "INSERT")
	}
	if scanProportion > 0 {
		operationChooser.AddValue(scanProportion, "SCAN")
	}
	if readModifyWriteProportion > 0 {
		operationChooser.AddValue(readModifyWriteProportion, "READMODIFYWRITE")
	}

	transactionInsertKeySequence := g.NewAcknowledgedCounterGenerator(recordCount)
	switch requestDistrib {
	case "uniform":
		keyChooser = g.NewUniformIntegerGenerator(0, recordCount-1)
	case "zipfian":
		// It does this by generating a random "next key" in part by
		// taking the modulus over the number of keys.
		// If the number of keys changes, this would shift the modulus,
		// and we don't want that to change which keys are popular
		// so we'll actually construct the scrambled zipfian generator
		// with a keyspace that is larger than exists at the beginning
		// of the test. That is, we'll predict the number of inserts, and
		// tell the scrambled zipfian generator the number of existing keys
		// plus the number of predicted keys as the total keyspace.
		// Then, if the generator picks a key that hasn't been inserted yet,
		// will just ignore it and pick another key. This way, the size of
		// the keyspace doesn't change from the prespective of the scrambled
		// zipfian generator.
		propStr = p.GetDefault(PropertyOperationCount, "")
		opCount, err := strconv.ParseInt(propStr, 0, 64)
		if err != nil {
			return err
		}
		// 2.0 is fudge factor
		expectedNewKeys := int64(float64(opCount) * insertProportion * 2.0)
		keyChooser = g.NewScrambledZipfianGeneratorByItems(recordCount + expectedNewKeys)
	case "latest":
		keyChooser = g.NewSkewedLatestGenerator(transactionInsertKeySequence.CounterGenerator)
	case "hotspot":
		propStr = p.GetDefault(HotspotDataFraction, HotspotDataFractionDefault)
		hotSetFraction, err := strconv.ParseFloat(propStr, 64)
		if err != nil {
			return err
		}
		propStr = p.GetDefault(HotspotOpnFraction, HotspotOpnFractionDefault)
		hotOpnFraction, err := strconv.ParseFloat(propStr, 64)
		if err != nil {
			return err
		}
		keyChooser = g.NewHotspotIntegerGenerator(0, recordCount-1, hotSetFraction, hotOpnFraction)
	default:
		return g.NewErrorf("unknown request distribution %s", requestDistrib)
	}

	fieldChooser := g.NewUniformIntegerGenerator(0, fieldCount-1)
	var scanLengthChooser g.IntegerGenerator
	switch scanLengthDistrib {
	case "uniform":
		scanLengthChooser = g.NewUniformIntegerGenerator(1, maxScanLength)
	case "zipfian":
		scanLengthChooser = g.NewZipfianGeneratorByInterval(1, maxScanLength)
	default:
		return g.NewErrorf("distribution %s not allowed for scan length", scanLengthDistrib)
	}

	propStr = p.GetDefault(InsertionRetryLimit, InsertionRetryLimitDefault)
	insertionRetryLimit, err := strconv.ParseInt(propStr, 0, 64)
	if err != nil {
		return err
	}
	propStr = p.GetDefault(InsertionRetryInterval, InsertionRetryIntervalDefault)
	insertionRetryInterval, err := strconv.ParseInt(propStr, 0, 64)
	if err != nil {
		return err
	}

	// set all fields
	self.table = table
	self.fieldCount = fieldCount
	self.fieldNames = fieldNames
	self.fieldLengthGenerator = fieldLengthGenerator
	self.readAllFields = readAllFields
	self.writeAllFields = writeAllFields
	self.dataIntegrity = dataIntegrity
	self.keySequence = keySequence
	self.operationChooser = operationChooser
	self.keyChooser = keyChooser
	self.fieldChooser = fieldChooser
	self.transactionInsertKeySequence = transactionInsertKeySequence
	self.scanLengthChooser = scanLengthChooser
	self.orderedInserts = orderedInserts
	self.recordCount = recordCount
	self.insertionRetryLimit = insertionRetryLimit
	self.insertionRetryInterval = insertionRetryInterval
	return nil
}

func (self *CoreWorkload) getFieldLengthGenerator(p Properties) (g.IntegerGenerator, error) {
	var fieldLengthGenerator g.IntegerGenerator
	fieldLengthDistribution := p.GetDefault(PropertyFieldLengthDistribution, PropertyFieldLengthDistributionDefault)
	propStr := p.GetDefault(PropertyFieldLength, PropertyFieldCountDefault)
	fieldLength, err := strconv.ParseInt(propStr, 0, 64)
	if err != nil {
		return nil, err
	}
	fieldLengthHistogramFile := p.GetDefault(PropertyFieldLengthHistogramFile, PropertyFieldLengthHistogramFileDefault)
	switch fieldLengthDistribution {
	case "constant":
		fieldLengthGenerator = g.NewConstantIntegerGenerator(fieldLength)
	case "uniform":
		fieldLengthGenerator = g.NewUniformIntegerGenerator(1, fieldLength)
	case "zipfian":
		fieldLengthGenerator = g.NewZipfianGeneratorByInterval(1, fieldLength-1)
	case "histogram":
		fieldLengthGenerator, err = g.NewHistogramGeneratorFromFile(fieldLengthHistogramFile)
		if err != nil {
			return nil, err
		}
	default:
		return nil, g.NewErrorf("unknown field length distribution %s", fieldLengthDistribution)
	}
	return fieldLengthGenerator, nil
}

func (self *CoreWorkload) InitRoutine(p Properties) (interface{}, error) {
	// nothing to do
	return nil, nil
}

func (self *CoreWorkload) Cleanup() error {
	// nothing to do
	return nil
}

func (self *CoreWorkload) buildKeyName(keyNumber int64) string {
	if !self.orderedInserts {
		keyNumber = int64(g.Hash(keyNumber))
	}
	return fmt.Sprintf("user%d", keyNumber)
}

func (self *CoreWorkload) buildSingleValue(key string) KVMap {
	fieldKey := self.fieldNames[self.fieldChooser.NextInt()]
	var data []byte
	if self.dataIntegrity {
		data = self.buildDeterministicValue(key, fieldKey)
	} else {
		// fill with random data
		data = RandomBytes(self.fieldLengthGenerator.NextInt())
	}
	return KVMap{
		fieldKey: data,
	}
}

func (self *CoreWorkload) buildValues(key string) KVMap {
	ret := make(KVMap)
	var data Binary
	for _, fieldKey := range self.fieldNames {
		if self.dataIntegrity {
			data = self.buildDeterministicValue(key, fieldKey)
		} else {
			// fill with random data
			data = RandomBytes(self.fieldLengthGenerator.NextInt())
		}
		ret[fieldKey] = data
	}
	return ret
}

func javaStringHashcode(b []byte) int64 {
	hash := int64(0)
	length := len(b)
	if length > 0 {
		for i := 0; i < length; i++ {
			hash = 31*hash + int64(b[i])
		}
	}
	return hash
}

func (self *CoreWorkload) buildDeterministicValue(key string, fieldKey string) []byte {
	size := self.fieldLengthGenerator.NextInt()
	buf := bytes.NewBuffer(make([]byte, 0, size))
	buf.WriteString(key)
	buf.WriteString(":")
	buf.WriteString(fieldKey)
	for int64(buf.Len()) < size {
		buf.WriteString(":")
		buf.WriteString(fmt.Sprintf("%d", javaStringHashcode(buf.Bytes())))
	}
	buf.Truncate(int(size))
	return buf.Bytes()
}

// Do one insert operation. Because it will be called concurrently from
// multiple client goroutines, this function must be routine safe.
// However, avoid synchronized, or the goroutines will block waiting
// for each other, and it will be difficult to reach the target thoughput.
// Ideally, this function would have no side effects other than DB operations.
func (self *CoreWorkload) DoInsert(db DB, object interface{}) bool {
	keyNumber := self.keySequence.NextInt()
	dbKey := self.buildKeyName(keyNumber)
	values := self.buildValues(dbKey)

	var status StatusType
	numberOfRetries := int64(0)
	var random *rand.Rand
	for {
		status = db.Insert(self.table, dbKey, values)
		if status == StatusOK {
			break
		}
		// Retry if configured. Without retrying, the load process will fail
		// even if one single insertion fails. User can optionally configure
		// an insertion retry limit(default is 0) to enable retry.
		numberOfRetries++
		if numberOfRetries < self.insertionRetryLimit {
			if random == nil {
				random = rand.New(rand.NewSource(time.Now().UnixNano()))
			}
			// sleep for a random number between
			// [0.8, 1.2) * InsertionRetryInterval
			sleepTime := int64(float64(1000*self.insertionRetryInterval) * (0.8 + 0.4*random.Float64()))
			time.Sleep(time.Duration(sleepTime))
		} else {
			// error inserting, not retrying any more
			break
		}
	}
	return (status == StatusOK)
}

// Do one transcation operation. Because it will be called concurrently from
// multiple client goroutines, this function must be routine safe.
// However, avoid synchronized, or the goroutines will block waiting
// for each other, and it will be difficult to reach the target thoughtput.
// Ideally, this function would have no side effects other than DB operations.
func (self *CoreWorkload) DoTransaction(db DB, object interface{}) bool {
	op := self.operationChooser.NextString()
	switch op {
	case "READ":
		self.DoTransactionRead(db)
	case "UPDATE":
		self.DoTransactionUpdate(db)
	case "INSERT":
		self.DoTransactionInsert(db)
	case "SCAN":
		self.DoTransactionScan(db)
	default:
		self.DoTransactionReadModifyWrite(db)
	}
	return true
}

func (self *CoreWorkload) nextKeyNumber() int64 {
	var ret int64
	c, ok := self.keyChooser.(*g.ExponentialGenerator)
	if ok {
		for {
			ret = self.transactionInsertKeySequence.LastInt() - c.NextInt()
			if ret >= 0 {
				break
			}
		}
	} else {
		for {
			ret = self.keyChooser.NextInt()
			if ret <= self.transactionInsertKeySequence.LastInt() {
				break
			}
		}
	}
	return ret
}

// Verify the dataset returned from transaction.
// Results are reported in the first three buckets of the histogram under
// the label "VERIFY".
// Bucket 0 means the expected data was returned.
// Bucket 1 means incorrect data was returned.
// Bucket 2 means null data was returned when some data was expected.
func (self *CoreWorkload) verifyRow(key string, cells KVMap) {
	status := StatusOK
	startTime := NowMS()
	if (cells == nil) || len(cells) == 0 {
		// This assumes that empty dataset is never valid
		status = StatusError
	} else {
		for k, v := range cells {
			if bytes.Compare(v, self.buildDeterministicValue(key, k)) != 0 {
				status = StatusUnexpectedState
				break
			}
		}
	}
	endTime := NowMS()
	self.measurements.Measure("VERIFY", endTime-startTime)
	self.measurements.ReportStatus("VERIFY", status)
}

func (self *CoreWorkload) DoTransactionRead(db DB) {
	// choose a random key
	keyNumber := self.nextKeyNumber()
	keyName := self.buildKeyName(keyNumber)
	var fields []string
	if !self.readAllFields {
		// read a random field
		fieldName := self.fieldNames[self.fieldChooser.NextInt()]
		fields = []string{fieldName}
	} else if self.dataIntegrity {
		// pass the full field list if dataIntegrity is on for verification
		fields = self.fieldNames
	}
	ret, _ := db.Read(self.table, keyName, fields)
	if self.dataIntegrity {
		self.verifyRow(keyName, ret)
	}
}

func (self *CoreWorkload) DoTransactionReadModifyWrite(db DB) {
	// choose a random key
	keyNumber := self.nextKeyNumber()
	keyName := self.buildKeyName(keyNumber)
	fields := make([]string, 0)
	if !self.readAllFields {
		// read a random field
		fieldName := self.fieldNames[self.fieldChooser.NextInt()]
		fields = []string{fieldName}
	}
	values := make(KVMap)
	if !self.writeAllFields {
		// new data for all the fields
		values = self.buildValues(keyName)
	} else {
		// update a random field
		values = self.buildSingleValue(keyName)
	}

	// do the transaction
	intendStartTime := self.measurements.GetIntendedStartTime()
	startTime := NowMS()
	ret, _ := db.Read(self.table, keyName, fields)
	db.Update(self.table, keyName, values)
	endTime := NowMS()
	if self.dataIntegrity {
		self.verifyRow(keyName, ret)
	}
	self.measurements.Measure("READ-MODIFY-WRITE", endTime-startTime)
	self.measurements.MeasureIntended("READ-MODIFY-WRITE", (endTime-intendStartTime)/1000)
}

func (self *CoreWorkload) DoTransactionScan(db DB) {
	// choose a random key
	keyNumber := self.nextKeyNumber()
	startKeyName := self.buildKeyName(keyNumber)
	fields := make([]string, 0)
	length := self.scanLengthChooser.NextInt()
	if !self.readAllFields {
		// read a random field
		fieldName := self.fieldNames[self.fieldChooser.NextInt()]
		fields = []string{fieldName}
	}
	db.Scan(self.table, startKeyName, length, fields)
}

func (self *CoreWorkload) DoTransactionUpdate(db DB) {
	// choose a random key
	keyNumber := self.nextKeyNumber()
	keyName := self.buildKeyName(keyNumber)
	values := make(KVMap)
	if !self.writeAllFields {
		// new data for all the fields
		values = self.buildValues(keyName)
	} else {
		// update a random field
		values = self.buildSingleValue(keyName)
	}
	db.Update(self.table, keyName, values)
}

func (self *CoreWorkload) DoTransactionInsert(db DB) {
	// choose the next key
	keyNumber := self.transactionInsertKeySequence.NextInt()
	keyName := self.buildKeyName(keyNumber)
	values := self.buildValues(keyName)
	db.Insert(self.table, keyName, values)
	self.transactionInsertKeySequence.Acknowledge(keyNumber)
}

// A disk-fragmenting workload.
// Properties to control the client:
// disksize: how many bytes of storage can the disk store? (default 100,000,000)
// occupancy: what fraction of the available storage should be used? (default 0.9)
// requestdistribution: what distribution should be used to select the records to operate on - uniform, zipfian or latest (default histogram)
//
// See also:
// Russell Sears, Catharine van Ingen.
// Fragmentation in Large Object Repositories(https://database.cs.wisc.edu/cidr/cidr2007/papers/cidr07p34.pdf)
// CIDR 2006. [Presentation(https://database.cs.wisc.edu/cidr/cidr2007/slides/p34-sears.ppt)]
type ConstantOccupancyWorkload struct {
	*CoreWorkload
	diskSize    int64
	storageAges int64
	objectSizes g.IntegerGenerator
	occupancy   float64
	objectCount int64
}

func NewConstantOccupancyWorkload() *ConstantOccupancyWorkload {
	return &ConstantOccupancyWorkload{
		CoreWorkload: NewCoreWorkload(),
	}
}

func (self *ConstantOccupancyWorkload) Init(p Properties) (err error) {
	catch(&err)
	propStr := p.GetDefault(PropertyDiskSize, PropertyDiskSizeDefault)
	diskSize, err := strconv.ParseInt(propStr, 0, 64)
	try(err)
	propStr = p.GetDefault(PropertyStorageAge, PropertyStorageAgeDefault)
	storageAges, err := strconv.ParseInt(propStr, 0, 64)
	try(err)
	propStr = p.GetDefault(PropertyOccupancy, PropertyOccupancyDefault)
	occupancy, err := strconv.ParseFloat(propStr, 64)
	try(err)
	_, ok1 := p[PropertyRecordCount]
	_, ok2 := p[PropertyInsertCount]
	_, ok3 := p[PropertyOperationCount]
	if ok1 || ok2 || ok3 {
		EPrintln("Warning: record, insert or operation count was set prior to initting ConstantOccupancyWorkload. Overriding old values.")
	}
	gen, err := self.CoreWorkload.getFieldLengthGenerator(p)
	try(err)
	fieldSize := gen.Mean()
	propStr = p.GetDefault(PropertyFieldCount, PropertyFieldCountDefault)
	fieldCount, err := strconv.ParseInt(propStr, 0, 64)
	try(err)
	objectCount := int64(occupancy * (float64(diskSize) / (fieldSize * float64(fieldCount))))
	if objectCount == 0 {
		try(g.NewErrorf("Object count was zero. Perhaps diskSize is too low?"))
	}
	p.Add(PropertyRecordCount, fmt.Sprintf("%d", objectCount))
	p.Add(PropertyOperationCount, fmt.Sprintf("%d", storageAges*objectCount))
	p.Add(PropertyInsertCount, fmt.Sprintf("%d", objectCount))
	try(self.CoreWorkload.Init(p))
	return
}
