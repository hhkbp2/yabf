package yabf

import (
	"fmt"
	g "github.com/hhkbp2/yabf/generator"
	"strconv"
	"strings"
	"time"
)

func MillisecondToNanosecond(millis int64) int64 {
	return millis * 1000 * 1000
}

func ConcatFieldsStr(fields []string) string {
	if len(fields) > 0 {
		return strings.Join(fields, ", ")
	} else {
		return "<all fields>"
	}
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
		ret += fmt.Sprintf("%s=%s", k, v)
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
			nanos = MillisecondToNanosecond(g.NextInt64(self.toDelay))
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
	p := self.GetProperties()
	var err error
	self.verbose, err = strconv.ParseBool(
		p.GetDefault(ConfigBasicDBVerbose, ConfigBasicDBVerboseDefault))
	if err != nil {
		return err
	}
	self.toDelay, err = strconv.ParseInt(
		p.GetDefault(ConfigSimulateDelay, ConfigSimulateDelayDefault), 0, 64)
	if err != nil {
		return err
	}
	self.randomizeDelay, err = strconv.ParseBool(
		p.GetDefault(ConfigRandomizeDelay, ConfigRandomizeDelayDefault))
	if err != nil {
		return err
	}
	if self.verbose {
		PrintProperties(p)
	}
	return nil
}

func (self *BasicDB) Cleanup() error {
	// do nothing
	return nil
}

// Read a record from the database.
func (self *BasicDB) Read(table string, key string, fields []string) (KVMap, StatusType) {
	self.Delay()
	if self.verbose {
		Println("READ %s %s [%s]", table, key, ConcatFieldsStr(fields))
	}
	return nil, StatusOK
}

// Perform a range scan for a set of records in the database.
func (self *BasicDB) Scan(table string, startKey string, recordCount int64, fields []string) ([]KVMap, StatusType) {
	self.Delay()
	if self.verbose {
		Println("SCAN %s %s %d [%s]", table, string(startKey), recordCount, ConcatFieldsStr(fields))
	}
	return nil, StatusOK
}

// Update a record in the database.
func (self *BasicDB) Update(table string, key string, values KVMap) StatusType {
	self.Delay()
	if self.verbose {
		Println("UPDATE %s %s [%s]", table, key, ConcatKVStr(values))
	}
	return StatusOK
}

// Insert a record in the database.
func (self *BasicDB) Insert(table string, key string, values KVMap) StatusType {
	self.Delay()
	if self.verbose {
		Println("INSERT %s %s [%s]", table, key, ConcatKVStr(values))
	}
	return StatusOK
}

// Delete a record from the database.
func (self *BasicDB) Delete(table string, key string) StatusType {
	self.Delay()
	if self.verbose {
		Println("DELETE %s %s", table, key)
	}
	return StatusOK
}
