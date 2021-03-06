package generator

// Generate integers resembling a hotspot distribution where x% of operations
// access y% of data items. The parameters specify the bounds for the numbers,
// the percentage of the interval which comprises the hot set and
// the percentage of operations that access the hot set. Numbers of the host set
// are always smaller than any number in the cold set. Elements from the hot set
// and the cold set are chosen using a uniform distribution.
type HotspotIntegerGenerator struct {
	*IntegerGeneratorBase
	lowerBound     int64
	upperBound     int64
	hotInterval    int64
	coldInterval   int64
	hotsetFraction float64
	hotOpnFraction float64
}

// Check the validation of value in range [0.0, 1.0].
func checkFraction(value float64) float64 {
	if value < 0.0 || value > 1.0 {
		// Hotset fraction out of range
		value = 0.0
	}
	return value
}

// Create a generator for hotspot distribution.
func NewHotspotIntegerGenerator(
	lowerBound, upperBound int64,
	hotsetFraction, hotOpnFraction float64) *HotspotIntegerGenerator {
	// check whether hostset fraction is out of range
	hotsetFraction = checkFraction(hotsetFraction)
	// check whether hot operation fraction is out of range
	hotOpnFraction = checkFraction(hotOpnFraction)
	if lowerBound > upperBound {
		// upper bound of hotspot Generator smaller than the lower one
		// swap the values
		lowerBound, upperBound = upperBound, lowerBound
	}
	interval := upperBound - lowerBound + 1
	hotInterval := int64(float64(interval) * hotsetFraction)
	return &HotspotIntegerGenerator{
		IntegerGeneratorBase: NewIntegerGeneratorBase(0),
		lowerBound:           lowerBound,
		upperBound:           upperBound,
		hotInterval:          hotInterval,
		coldInterval:         interval - hotInterval,
		hotsetFraction:       hotsetFraction,
		hotOpnFraction:       hotOpnFraction,
	}
}

func (self *HotspotIntegerGenerator) NextInt() int64 {
	var value int64
	if NextFloat64() < self.hotOpnFraction {
		// Choose a value from the hot set.
		value = self.lowerBound + NextInt64(self.hotInterval)
	} else {
		// Choose a value from the cold set.
		value = self.lowerBound + self.hotInterval + NextInt64(self.coldInterval)
	}
	self.SetLastInt(value)
	return value
}

func (self *HotspotIntegerGenerator) NextString() string {
	return self.IntegerGeneratorBase.NextString(self)
}

func (self *HotspotIntegerGenerator) Mean() float64 {
	return self.hotOpnFraction*float64(self.lowerBound+self.hotInterval/2.0) +
		(1-self.hotOpnFraction)*float64(self.lowerBound+self.hotInterval+self.coldInterval/2.0)
}

func (self *HotspotIntegerGenerator) GetLowerBound() int64 {
	return self.lowerBound
}

func (self *HotspotIntegerGenerator) GetUpperBound() int64 {
	return self.upperBound
}

func (self *HotspotIntegerGenerator) GetHotsetFraction() float64 {
	return self.hotsetFraction
}

func (self *HotspotIntegerGenerator) GetHotOpnFraction() float64 {
	return self.hotOpnFraction
}
