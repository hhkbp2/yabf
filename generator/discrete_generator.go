package generator

type Pair struct {
	Weight float64
	Value  string
}

type DiscreteGenerator struct {
	values    []*Pair
	lastValue string
}

func NewDiscreteGenerator() *DiscreteGenerator {
	return &DiscreteGenerator{
		values:    make([]*Pair, 0),
		lastValue: "",
	}
}

func (self *DiscreteGenerator) NextString() string {
	var sum float64
	for _, p := range self.values {
		sum += p.Weight
	}

	value := NextFloat64()

	for _, p := range self.values {
		v := p.Weight / sum
		if value < v {
			return p.Value
		}
		value -= v
	}

	// should never get here.
	panic("oops. should not get here")
	return ""
}

func (self *DiscreteGenerator) LastString() string {
	if len(self.lastValue) == 0 {
		self.lastValue = self.NextString()
	}
	return self.lastValue
}

func (self *DiscreteGenerator) AddValue(weight float64, value string) {
	self.values = append(self.values, &Pair{
		Weight: weight,
		Value:  value,
	})
}
