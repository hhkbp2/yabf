package generator

import (
	"fmt"
)

// IntegerGenerator is a generator capable of generating integers and strings.
type IntegerGenerator interface {
	Generator
	// NextInt returns the next value as an int. When overriding this method,
	// be sure to call setLastString() properly, or the LastString() call
	// won't work.
	NextInt() int64
	LastInt() int64

	Mean() float64
}

// IntegerGeneratorBase is a parent class for all IntegerGenerator subclasses.
type IntegerGeneratorBase struct {
	lastInt int64
}

func NewIntegerGeneratorBase(last int64) *IntegerGeneratorBase {
	return &IntegerGeneratorBase{
		lastInt: last,
	}
}

// SetLastInt sets the last value to be generated.
// IntegerGenerator subclasses must use this call to properly set the last
// int value, or the LastString() and LastInt() calls won't work.
func (self *IntegerGeneratorBase) SetLastInt(value int64) {
	self.lastInt = value
}

// NextString generates the next string in the distribution.
func (self *IntegerGeneratorBase) NextString(g IntegerGenerator) string {
	return fmt.Sprintf("%d", g.NextInt())
}

func (self *IntegerGeneratorBase) LastInt() int64 {
	return self.lastInt
}

func (self *IntegerGeneratorBase) LastString() string {
	return fmt.Sprintf("%d", self.LastInt())
}

// ConstantIntegerGenerator is a trivial integer generator that always returns
// the same value.
type ConstantIntegerGenerator struct {
	*IntegerGeneratorBase
	value int64
}

func NewConstantIntegerGenerator(i int64) *ConstantIntegerGenerator {
	return &ConstantIntegerGenerator{
		IntegerGeneratorBase: NewIntegerGeneratorBase(i - 1),
		value:                i,
	}
}

func (self *ConstantIntegerGenerator) NextInt() int64 {
	return self.value
}

func (self *ConstantIntegerGenerator) NextString() string {
	return self.IntegerGeneratorBase.NextString(self)
}

func (self *ConstantIntegerGenerator) Mean() float64 {
	return float64(self.NextInt())
}
