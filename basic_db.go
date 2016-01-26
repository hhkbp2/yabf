package generator

import (
	"github.com/hhkbp2/yabf/basic_db/generator"
	"strconv"
)

func MillisecondToNanosecond(millis int64) int64 {
	return millis * 1000 * 1000
}

func ConcatFieldsStr(fields []Binary) string {
	var ret string
	if len(fields) > 0 {
		afterFirst := false
		for _, f := range fields {
			if afterFirst {
				ret += ", "
			} else {
				afterFirst = true
			}
			ret += string(f)
		}
	} else {
		ret = "<all fields>"
	}
	return ret
}

func ConcatKVStr(values KVMap) string {
	var ret string
	afterFirst := false
	for k, v := range values {
		if afterFirst {
			ret += ", "
		} else {
			afterFirst = true
		}
		ret += (k + "=" + v)
	}
	return ret
}

type BasicDB struct {
	*DBBase
	verbose        bool
	randomizeDelay bool
	toDelay        int64
}

func NewBasicDB() *BasicDB {
	return &BasicDB{
		DBBase: NewDBBase(),
	}
}

func (self *BasicDB) Delay() {
	if self.toDelay > 0 {
		var nanos int64
		if self.randomizeDelay {
			nanos = MillisecondToNanosecond(generator.NextInt64(self.toDelay))
			if nanos == 0 {
				return
			}
		} else {
			nanos = MillisecondToNanosecond(self.toDelay)
		}
		delay := time.Duration(nanos)
		time.Sleep(delay)
	}
}

// Initialize any state for this DB.
func (self *BasicDB) Init() error {
	p = self.GetProperties()
	var err error
	self.verbose, err = strconv.ParseBool(
		p.GetDefault(ConfigBasicDBVerbose, ConfigBasicDBVerboseDefault))
	if err != nil {
		return err
	}
	self.toDelay, err = strconv.ParseInt(
		p.GetDefault(ConfigSimulateDelay, ConfigSimulateDelayDefault))
	if err != nil {
		return err
	}
	self.randomizeDelay, err = strconv.ParseBool(
		p.GetDefault(ConfigRandomizeDelay, ConfigRandomizeDelayDefault))
	if err != nil {
		return err
	}
	if self.verbose {
		OutputProperties(p)
	}
}

// Read a record from the database.
func (self *BasicDB) Read(table string, key Binary, fields []Binary) (KVMap, error) {
	self.Delay()
	if self.verbose {
		Output("READ %s %s [%s]", table, string(key), ConcatFieldsStr(fields))
	}
	return nil, nil
}

// Perform a range scan for a set of records in the database.
func (self *BasicDB) Scan(table string, startKey Binary, recordCount int, fields []Binary) (KVMap, error) {
	self.Delay()
	if self.verbose {
		Output("SCAN %s %s %d [%s]",
			table, string(startKey), recordCount, ConcatFieldsStr(fields))
	}
	return nil, nil
}

// Update a record in the database.
func (self *BasicDB) Update(table string, key Binary, values KVMap) error {
	self.Delay()
	if self.verbose {
		Output("UPDATE %s %s [%s]", table, string(key), ConcatKVStr(values))
	}
	return nil
}

// Insert a record in the database.
func (self *BasicDB) Insert(table string, key Binary, values KVMap) error {
	self.Delay()
	if self.verbose {
		Output("INSERT %s %s [%s]", table, string(key), ConcatKVStr(values))
	}
	return nil
}

// Delete a record from the database.
func (self *BasicDB) Delete(table string, key string) error {
	self.Delay()
	if self.verbose {
		Output("DELETE %s %s", table, string(key))
	}
	return nil
}
