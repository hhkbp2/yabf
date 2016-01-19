package generator

import (
	"sync/atomic"
)

type CounterGenerator struct {
	*IntegerGeneratorBase
	count int64
}

func NewCounterGenerator(startCount int64) *CounterGenerator {
	object := &CounterGenerator{
		IntegerGeneratorBase: NewIntegerGeneratorBase(startCount - 1),
		count:                startCount - 1,
	}
	return object
}

func (self *CounterGenerator) NextInt() int64 {
	ret := atomic.AddInt64(&self.count, 1)
	self.SetLastInt(ret)
	return ret
}

func (self *CounterGenerator) NextString() string {
	return self.IntegerGeneratorBase.NextString(self)
}

func (self *CounterGenerator) Mean() float64 {
	panic("unsupported operation")
}
