package workload

import (
	"github.com/hhkbp2/yabf"
	g "github.com/hhkbp2/yabf/generator"
)

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
