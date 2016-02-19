package yabf

import (
	g "github.com/hhkbp2/yabf/generator"
)

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
	InitRoutine(p Properties, id int64) (interface{}, error)

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

	// Allows scheduling a request to stop the Workload.
	RequestStop()

	// Check the status of the stop request flag.
	isStopRequested() bool
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
	fieldCount                   int
	fieldNames                   []string
	fieldLengthGenerator         g.IntegerGenerator
	readAllFields                bool
	writeAllFields               bool
	dataIntegrity                bool
	keySequence                  g.IntegerGenerator
	operationChooser             *g.DiscreteGenerator
	keyChooser                   g.IntegerGenerator
	fieldChooser                 g.Generator
	transactionInsertKeySequence *g.AcknowledgedCounterGenerator
	scanLength                   *g.IntegerGenerator
	orderedInserts               bool
	recordCount                  int
	insertionRetryLimit          int
	insertionRetryInterval       int
}
