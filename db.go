package yabf

import (
	"database/sql/driver"
	"errors"
	g "github.com/hhkbp2/yabf/generator"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	Error              = errors.New("The operation failed.")
	NotFound           = errors.New("The requested record was not found.")
	NotImplemented     = errors.New("The operation is not implemented for the current binding.")
	UnexpectedState    = errors.New("The operation reported success, but the result was not as expected.")
	BadRequest         = errors.New("The request was not valid.")
	Forbidden          = errors.New("The request was not valid.")
	ServiceUnavailable = errors.New("Dependant service for the current binding is not available.")
)

// Binary represents arbitrary binary value(byte array).
type Binary []byte

func (self Binary) Value() (driver.Value, error) {
	return []byte(self), nil
}

// Result represents the result type of db operations.
type KVMap map[string]Binary

// DB is A layer for accessing a database to be benchmarked.
// Each routine in the client will be given its own instance of
// whatever DB class is to be used in the test.
// This class should be constructed using a no-argument constructor, so we can
// load it dynamically. Any argument-based initialization should be
// done by Init().
//
// Note that YABF does not make any use of the return codes returned by this class.
// Instead, it keeps a count of the return values and presents them to the user.
//
// The semantics of methods such as Insert, Update and Delete vary from database
// to database.  In particular, operations may or may not be durable once these
// methods commit, and some systems may return 'success' regardless of whether
// or not a tuple with a matching key existed before the call.  Rather than dictate
// the exact semantics of these methods, we recommend you either implement them
// to match the database's default semantics, or the semantics of your
// target application.  For the sake of comparison between experiments we also
// recommend you explain the semantics you chose when presenting performance results.
type DB interface {
	// Set the properties for this DB.
	SetProperties(p Properties)

	// Get the properties for this DB.
	GetProperties() Properties

	// Initialize any state for this DB.
	// Called once per DB instance; there is one DB instance per client routine.
	Init() error

	// Cleanup any state for this DB.
	// Called once per DB instance; there is one DB instance per client routine.
	Cleanup() error

	// Read a record from the database.
	// Each field/value pair from the result will be returned.
	Read(table string, key string, fields []string) (KVMap, StatusType)

	// Perform a range scan for a set of records in the database.
	// Each field/value pair from the result will be returned.
	Scan(table string, startKey string, recordCount int64, fields []string) ([]KVMap, StatusType)

	// Update a record in the database.
	// Any field/value pairs in the specified values will be written into
	// the record with the specified record key, overwriting any existing
	// values with the same field name.
	Update(table string, key string, values KVMap) StatusType

	// Insert a record in the database. Any field/value pairs in the specified
	// values will be written into the record with the specified record key.
	Insert(table string, key string, values KVMap) StatusType

	// Delete a reord from the database.
	Delete(table string, key string) StatusType
}

type DBBase struct {
	p Properties
}

func NewDBBase() *DBBase {
	return &DBBase{}
}

func (self *DBBase) SetProperties(p Properties) {
	self.p = p
}

func (self *DBBase) GetProperties() Properties {
	return self.p
}

func NewDB(database string, props Properties) (DB, error) {
	f, ok := Databases[database]
	if !ok {
		return nil, g.NewErrorf("unsupported database: %s", database)
	}
	db := f()
	db.SetProperties(props)
	return NewDBWrapper(db), nil
}

// Wrapper around a "real" DB that measures latencies and counts return codes.
// Also reports latency separately between OK and false operations.
type DBWrapper struct {
	DB
	measurements Measurements

	reportLatencyForEachError bool
	latencyTrackedErrors      map[string]bool
}

func NewDBWrapper(db DB) *DBWrapper {
	return &DBWrapper{
		DB:           db,
		measurements: GetMeasurements(),
	}
}

func (self *DBWrapper) Init() (err error) {
	defer catch(&err)
	try(self.DB.Init())
	p := self.GetProperties()
	propStr := p.GetDefault(PropertyReportLatencyForEachError, PropertyReportLatencyForEachErrorDefault)
	reportLatencyForEachError, err := strconv.ParseBool(propStr)
	try(err)
	latencyTrackedErrors := make(map[string]bool)
	var ok bool
	if !self.reportLatencyForEachError {
		propStr, ok = p[PropertyLatencyTrackedErrors]
		if ok {
			parts := strings.Split(propStr, ",")
			for _, p := range parts {
				latencyTrackedErrors[p] = true
			}
		}
	}
	self.reportLatencyForEachError = reportLatencyForEachError
	self.latencyTrackedErrors = latencyTrackedErrors

	EPrintln("DBWrapper: report latency for each error is %t and specific error codes to track for latency are: %s",
		reportLatencyForEachError, propStr)
	return
}

// Cleanup any state for this DB.
func (self *DBWrapper) Cleanup() error {
	startTime := NowNS()
	err := self.DB.Cleanup()
	if err != nil {
		return err
	}
	endTime := NowNS()
	self.measure("CLEANUP", StatusOK, startTime, endTime)
	return err
}

// Read a record from the database.
func (self *DBWrapper) Read(table string, key string, fields []string) (KVMap, StatusType) {
	startTime := NowNS()
	ret, status := self.DB.Read(table, key, fields)
	endTime := NowNS()
	self.measure("READ", status, startTime, endTime)
	self.measurements.ReportStatus("READ", status)
	return ret, status
}

// Perform a range scan for a set of records in the databases.
func (self *DBWrapper) Scan(table string, startKey string, recordCount int64, fields []string) ([]KVMap, StatusType) {
	startTime := NowNS()
	ret, status := self.DB.Scan(table, startKey, recordCount, fields)
	endTime := NowNS()
	self.measure("SCAN", status, startTime, endTime)
	self.measurements.ReportStatus("SCAN", status)
	return ret, status
}

// Update a recerd in the database.
func (self *DBWrapper) Update(table string, key string, values KVMap) StatusType {
	startTime := NowNS()
	status := self.DB.Update(table, key, values)
	endTime := NowNS()
	self.measure("UPDATE", status, startTime, endTime)
	self.measurements.ReportStatus("UPDATE", status)
	return status
}

// Insert a record in the database.
func (self *DBWrapper) Insert(table string, key string, values KVMap) StatusType {
	startTime := NowNS()
	status := self.DB.Insert(table, key, values)
	endTime := NowNS()
	self.measure("INSERT", status, startTime, endTime)
	self.measurements.ReportStatus("INSERT", status)
	return status
}

// Delete a record from the database.
func (self *DBWrapper) Delete(table string, key string) StatusType {
	startTime := NowNS()
	status := self.DB.Delete(table, key)
	endTime := NowNS()
	self.measure("DELETE", status, startTime, endTime)
	self.measurements.ReportStatus("DELETE", status)
	return status
}

func (self *DBWrapper) measure(op string, status StatusType, startTime, endTime int64) {
	measurementName := op
	if status != StatusOK {
		statusStr := status.String()
		_, ok := self.latencyTrackedErrors[statusStr]
		if self.reportLatencyForEachError || ok {
			measurementName = op + "-" + statusStr
		} else {
			measurementName = op + "-FAILED"
		}
	}
	self.measurements.Measure(measurementName, NanosecondToMicrosecond(endTime-startTime))
}

// A simple DB implementation that just prints out the requested operations,
// instead of doing them against a database.
type GoodBadUglyDB struct {
	*DBBase
	delays []int64
	lock   *sync.RWMutex
}

func NewGoodBadUglyDB() *GoodBadUglyDB {
	return &GoodBadUglyDB{
		DBBase: NewDBBase(),
		delays: []int64{200, 1000, 10000, 50000, 100000},
		lock:   &sync.RWMutex{},
	}
}

func (self *GoodBadUglyDB) delay() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	p := r.Float64()
	var mod int64
	if p < 0.9 {
		mod = 0
	} else if p < 0.99 {
		mod = 1
	} else if p < 0.9999 {
		mod = 2
	} else {
		mod = 3
	}
	// this will make mod 3 pauses global
	if mod == 3 {
		EPrintln("OUCH")
	}
	// TODO
}

// Initialize any state for this DB.
func (self *GoodBadUglyDB) Init() error {
	propStr := self.GetProperties().GetDefault(SimulateDelay, SimulateDelayDefault)
	parts := strings.Split(propStr, ",")
	i := 0
	for _, p := range parts {
		d, err := strconv.ParseInt(p, 0, 64)
		if err != nil {
			return err
		}
		self.delays[i] = d
		i++
	}
	return nil
}

func (self *GoodBadUglyDB) Cleanup() error {
	// do nothing
	return nil
}

// Read a record from the database.
func (self *GoodBadUglyDB) Read(table string, key string, fields []string) (KVMap, StatusType) {
	self.delay()
	return nil, StatusOK
}

// Perform a range scan for a set of records in the database.
func (self *GoodBadUglyDB) Scan(table string, startKey string, recordCount int64, fields []string) ([]KVMap, StatusType) {
	self.delay()
	return nil, StatusOK
}

// Update a record in the database.
func (self *GoodBadUglyDB) Update(table string, key string, values KVMap) StatusType {
	self.delay()
	return StatusOK
}

// Insert a record in the database.
func (self *GoodBadUglyDB) Insert(table string, key string, value KVMap) StatusType {
	self.delay()
	return StatusOK
}

// Delete a record from the database.
func (self *GoodBadUglyDB) Delete(table string, key string) StatusType {
	self.delay()
	return StatusOK
}
