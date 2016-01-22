package generator

import (
	"sync"
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

const (
	AcknowledgedWindowSize = int64(1 << 20)
	AcknowledgedWindowMask = AcknowledgedWindowSize - 1
)

type AcknowledgedCounterGenerator struct {
	*CounterGenerator
	mutex  *sync.Mutex
	window []bool
	limit  int64
}

func NewAcknowledgedCounterGenerator(startCount int64) *AcknowledgedCounterGenerator {
	return &AcknowledgedCounterGenerator{
		CounterGenerator: NewCounterGenerator(startCount),
		lock:             &sync.Mutex{},
		window:           make([]bool, AcknowledgedWindowSize),
		limit:            startCount - 1,
	}
}

func (self *AcknowledgedCounterGenerator) LastInt() int64 {
	return self.limit
}

func (self *AcknowledgedCounterGenerator) LastString() string {
	return self.lastStringFrom(self)
}

func (self *AcknowledgedCounterGenerator) Acknowledge(value int64) {
	currentSlot := value & AcknowledgedWindowMask
	self.window[currentSlot] = true
	self.mutex.Lock()
	defer self.mutex.Unlock()
	beforeFirstSlot = self.limit & AcknowledgedWindowMask
	var index int64
	for index = limit + 1; index != beforeFirstSlot; index++ {
		slot := index & AcknowledgedWindowMask
		if !self.window[slot] {
			break
		}
		window[slot] = false
	}
	limit = index - 1
}
