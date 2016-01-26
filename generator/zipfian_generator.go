package generator

import (
	"math"
)

const (
	ZipfianConstant = float64(0.99)
)

func zeta(st, n int64, theta, initialSum float64) (int64, float64) {
	countForzata := n
	return countForzata, zetaStatic(st, n, theta, initialSum)
}

func zetaStatic(st, n int64, theta, initialSum float64) float64 {
	sum := initialSum
	for i := st; i < n; i++ {
		sum += 1 / math.Pow(float64(i+1), theta)
	}
	return sum
}

type ZipfianGenerator struct {
	*IntegerGeneratorBase
	items                                int64
	base                                 int64
	zipfianConstant                      float64
	alpha, zetan, eta, theta, zeta2theta float64
	countForzata                         int64
	allowItemCountDecrease               bool
}

func NewZipfianGenerator(
	min, max int64, zipfianConstant, zetan float64) *ZipfianGenerator {

	items := max - min + 1
	base := min
	theta := zipfianConstant
	countForzata, zeta2theta := zeta(0, 2, theta, 0)
	alpha := 1.0 / (1.0 - theta)
	countForzata = items
	eta := (1 - math.Pow(float64(2.0/items), 1-theta)) / (1 - zeta2theta/zetan)

	object := &ZipfianGenerator{
		IntegerGeneratorBase:   NewIntegerGeneratorBase(0),
		items:                  items,
		base:                   base,
		zipfianConstant:        zipfianConstant,
		alpha:                  alpha,
		zetan:                  zetan,
		eta:                    eta,
		theta:                  theta,
		zeta2theta:             zeta2theta,
		countForzata:           countForzata,
		allowItemCountDecrease: false,
	}
	object.NextInt()
	return object
}

func (self *ZipfianGenerator) NextInt() int64 {
	return self.Next(self.items)
}

func (self *ZipfianGenerator) Next(itemCount int64) int64 {
	if itemCount != self.countForzata {
		if itemCount > self.countForzata {
			self.countForzata, self.zetan = zeta(self.countForzata, itemCount, self.theta, self.zetan)
			self.eta = (1 - math.Pow(float64(2.0/self.items), 1-self.theta)) / (1 - self.zeta2theta/self.zetan)
		} else if (itemCount < self.countForzata) && (self.allowItemCountDecrease) {
			self.countForzata, self.zetan = zeta(0, itemCount, self.theta, 0)
			self.eta = (1 - math.Pow(float64(2.0/self.items), 1-self.theta)) / (1 - self.zeta2theta/self.zetan)
		}
	}

	u := NextFloat64()
	uz := u * self.zetan
	if uz < 1.0 {
		return self.base
	}
	if uz < 1.0+math.Pow(0.5, self.theta) {
		return self.base + 1
	}
	ret := self.base + int64(float64(itemCount)*math.Pow(self.eta*u-self.eta+1.0, self.alpha))
	self.IntegerGeneratorBase.SetLastInt(ret)
	return ret
}

func (self *ZipfianGenerator) Mean() float64 {
	panic("unsupported operation")
}
