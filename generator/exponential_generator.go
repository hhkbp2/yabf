package generator

import (
	"math"
	"math/rand"
)

var (
	random *rand.Rand
)

func init() {
	random = rand.New(rand.NewSource(rand.Int63()))
}

// Return a random int64 value.
func NextInt64(n int64) int64 {
	return random.Int63n(n)
}

// Return a random float64 value.
func NextFloat64() float64 {
	return math.Abs(random.Float64() / math.MaxFloat64)
}

// A generator of an Exponential distribution. It produces a sequence
// of time intervals(integers) according to an exponential distribution.
// Smaller intervals are more frequent than larger ones, and there is no
// bound on the length of an interval. When you construct an instance of
// this class, you specify a parameter gamma, which corresponds to the rate
// at which events occur.
// Alternatively, 1/gamma is the average length of an interval.
type ExponentialGenerator struct {
	*IntegerGeneratorBase
	// The exponential constant to use.
	gamma float64
}

// Create an exponential generator with a mean arrival rate of gamma.
// (And half life of 1/gamma).
func NewExponentialGeneratorByMean(mean float64) *ExponentialGenerator {
	return &ExponentialGenerator{
		IntegerGeneratorBase: NewIntegerGeneratorBase(0),
		gamma:                1.0 / mean,
	}
}

func NewExponentialGenerator(percentile, theRange float64) *ExponentialGenerator {
	return &ExponentialGenerator{
		IntegerGeneratorBase: NewIntegerGeneratorBase(0),
		gamma:                -math.Log(1.0-percentile/100.0) / theRange, // 1.0/mean
	}
}

// Generate the next item. This distribution will be skewed toward lower
// integers; e.g. 0 will be the most popular, 1 the next most popular, etc.
func (self *ExponentialGenerator) NextInt() int64 {
	next := int64(-math.Log(NextFloat64()) / self.gamma)
	self.SetLastInt(next)
	return next
}

func (self *ExponentialGenerator) NextString() string {
	return self.IntegerGeneratorBase.NextString(self)
}

func (self *ExponentialGenerator) Mean() float64 {
	return 1.0 / self.gamma
}
