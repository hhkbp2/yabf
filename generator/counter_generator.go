package generator

import (
	"sync"
	"sync/atomic"
)

// Generates a sequence of integers 0, 1, ...
type CounterGenerator struct {
	*IntegerGeneratorBase
	count int64
}

// Create a counter that starts at startCount.
func NewCounterGenerator(startCount int64) *CounterGenerator {
	object := &CounterGenerator{
		IntegerGeneratorBase: NewIntegerGeneratorBase(startCount - 1),
		count:                startCount - 1,
	}
	return object
}

// If the generator returns numeric(integer) values, return the next value
// as an int. Default is to return -1, which is appropriate for generators
// that do not return numeric values.
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
	// The size of the window of pending id ack.
	AcknowledgedWindowSize = int64(1 << 20)
	// The mask to use to turn an id into a slot in window.
	AcknowledgedWindowMask = AcknowledgedWindowSize - 1
)

// A CounterGenerator that reports generated integers via LastInt()
// only after they have been acknowledged.
type AcknowledgedCounterGenerator struct {
	*CounterGenerator
	mutex  *sync.Mutex
	window []bool
	limit  int64
}

// Create a counter that starts at startCount.
func NewAcknowledgedCounterGenerator(startCount int64) *AcknowledgedCounterGenerator {
	return &AcknowledgedCounterGenerator{
		CounterGenerator: NewCounterGenerator(startCount),
		mutex:            &sync.Mutex{},
		window:           make([]bool, AcknowledgedWindowSize),
		limit:            startCount - 1,
	}
}

// In this generator, the highest acknowledged counter value
// (as opposed to the highest generated counter value).
func (self *AcknowledgedCounterGenerator) LastInt() int64 {
	return self.limit
}

func (self *AcknowledgedCounterGenerator) LastString() string {
	return self.lastStringFrom(self)
}

// Make a generated counter value available via LastInt().
func (self *AcknowledgedCounterGenerator) Acknowledge(value int64) {
	currentSlot := value & AcknowledgedWindowMask
	self.window[currentSlot] = true
	self.mutex.Lock()
	defer self.mutex.Unlock()
	beforeFirstSlot := self.limit & AcknowledgedWindowMask
	var index int64
	for index = self.limit + 1; index != beforeFirstSlot; index++ {
		slot := index & AcknowledgedWindowMask
		if !self.window[slot] {
			break
		}
		self.window[slot] = false
	}
	self.limit = index - 1
}
