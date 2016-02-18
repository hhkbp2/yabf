package generator

import (
	"math"
)

const (
	ZipfianConstant = float64(0.99)
)

// Compute the zeta constant needed for the distribution.
// Do this incrementally for a distribution that has n items now
// but used to have st items. Use the zipfian constant theta.
// Remember the new value of n so that if we change the itemCount,
// we'll know to recompute zeta.
func zeta(st, n int64, theta, initialSum float64) (int64, float64) {
	countForzata := n
	return countForzata, zetaStatic(st, n, theta, initialSum)
}

// Compute the zeta constant needed for the distribution. Do this incrementally
// for a distribution that has h items now but used to have st items.
// Use the zipfian constant theta. Remember the new value of n so that
// if we change itemCount, we'll know to recompute zeta.
func zetaStatic(st, n int64, theta, initialSum float64) float64 {
	sum := initialSum
	for i := st; i < n; i++ {
		sum += 1 / math.Pow(float64(i+1), theta)
	}
	return sum
}

// A generator of a zipfian distribution. It produces a sequence of items,
// such that some items are more popular than others, according to
// a zipfian distribution. When you construct an instance of this class,
// you specify the number of items in the set to draw from, either by
// specifying an itemcount (so that the sequence is of items from 0 to
// itemcount-1) or by specifying a min and a max (so that the sequence
// is of items from min to max inclusive). After you construct the instance,
// you can change the number of items by calling NextInt() or nextLong().
//
// Note that the popular items will be clustered together, e.g. item 0
// is the most popular, item 1 the second most popular, and so on (or min
// is the most popular, min+1 the next most popular, etc.)
// If you don't want this clustering, and instead want the popular items
// scattered throughout the item space, then use ScrambledZipfianGenerator
// instead.
//
// Be aware: initializing this generator may take a long time if there are
// lots of items to choose from (e.g. over a minute for 100 million objects).
// This is because certain mathematical values need to be computed to properly
// generate a zipfian skew, and one of those values (zeta) is a sum sequence
// from 1 to n, where n is the itemCount. Note that if you increase the number
// of items in the set, we can compute a new zeta incrementally, so it should
// be fast unless you have added millions of items. However, if you decrease
// the number of items, we recompute zeta from scratch, so this can take
// a long time.
//
// The algorithm used here is from
// "Quickly Generating Billion-Record Synthetic Databases",
// Jim Gray et al, SIGMOD 1994.
//
type ZipfianGenerator struct {
	*IntegerGeneratorBase
	// Number of items.
	items int64
	// Min item to generate.
	base int64
	// The zipfian constant to use.
	zipfianConstant float64
	// Computed parameters for generating the distribution.
	alpha, zetan, eta, theta, zeta2theta float64
	// The number of items used to compute zetan the last time.
	countForzata int64

	// Flag to prevent problems. If you increase the number of items which
	// the zipfian generator is allowed to choose from, this code will
	// incrementally compute a new zeta value for the larger itemcount.
	// However, if you decrease the number of items, the code computes
	// zeta from scratch; this is expensive for large itemsets.
	// Usually this is not intentional; e.g. one goroutine thinks
	// the number of items is 1001 and calls "NextLong()" with that item count;
	// then another goroutine who thinks the number of items is 1000 calls
	// NextLong() with itemCount=1000 triggering the expensive recomputation.
	// (It is expensive for 100 million items, not really for 1000 items.)
	// Why did the second goroutine think there were only 1000 items?
	// maybe it read the item count before the first goroutine incremented it.
	// So this flag allows you to say if you really do want that recomputation.
	// If true, then the code will recompute zeta if the itemcount goes down.
	// If false, the code will assume itemcount only goes up, and never
	// recompute.
	allowItemCountDecrease bool
}

// Create a zipfian generator for items between min and max(inclusive) for
// the specified zipfian constant, using the precomputed value of zeta.
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

// Generate the next item. this distribution will be skewed toward
// lower itegers; e.g. 0 will be the most popular, 1 the next most popular, etc.
func (self *ZipfianGenerator) NextInt() int64 {
	return self.Next(self.items)
}

// Generate the next item as a int64.
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
