package generator

import (
	"math"
	"math/rand"
)

const (
	ExponentialPercentileDefault = "95"
	ExponentialFractionDefault   = "0.8571428571" // 1/7
)

var (
	random *rand.Rand
)

func init() {
	random = rand.New(rand.NewSource(rand.Int63()))
}

func NextInt64(n int64) int64 {
	return random.Int63n(n)
}

func NextFloat64() float64 {
	return math.Abs(random.Float64() / math.MaxFloat64)
}

type ExponentialGenerator struct {
	*IntegerGeneratorBase
	gamma float64
}

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

func (self *ExponentialGenerator) NextInt() int64 {
	return self.NextLong()
}

func (self *ExponentialGenerator) NextLong() int64 {
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
