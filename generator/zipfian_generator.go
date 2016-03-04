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

// Create a zipfian generator for items between min and max(inclusive).
func NewZipfianGeneratorByInterval(min, max int64) *ZipfianGenerator {
	zeta := zetaStatic(min, max-min+1, ZipfianConstant, 0)
	return NewZipfianGenerator(min, max, ZipfianConstant, zeta)
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

// Return the next value, skewed by the zipfian distribution. The 0th item will
// be the most popular, followed by the 1st, followed by the 2nd, etc.
// (or, if min != 0, the min-th item is the most popular, the min+1th item
// the next most popular, etc.) If you want the popular items
// scattered throughout the item space, use ScrambledZipfianGenerator instead.
func (self *ZipfianGenerator) NextInt() int64 {
	return self.Next(self.items)
}

// Return the next value, skewed by the zipfian distribution. The 0th item will
// be the most popular, followed by the 1st, followed by the 2nd, etc.
// (same as NextInt())
func (self *ZipfianGenerator) NextLong() int64 {
	return self.Next(self.items)
}

// Generate the next item. this distribution will be skewed toward
// lower itegers; e.g. 0 will be the most popular, 1 the next most popular, etc.
func (self *ZipfianGenerator) Next(itemCount int64) int64 {
	var ret int64
	defer func(r *int64) {
		self.IntegerGeneratorBase.SetLastInt(*r)
	}(&ret)
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
		ret = self.base
		return ret
	}
	if uz < 1.0+math.Pow(0.5, self.theta) {
		ret = self.base + 1
		return ret
	}
	ret = self.base + int64(float64(itemCount)*math.Pow(self.eta*u-self.eta+1.0, self.alpha))
	return ret
}

func (self *ZipfianGenerator) NextString() string {
	return self.IntegerGeneratorBase.NextString(self)
}

func (self *ZipfianGenerator) Mean() float64 {
	panic("unsupported operation")
}

var (
	Zetan               = float64(26.46902820178302)
	UsedZipfianConstant = float64(0.99)
	ItemCount           = float64(10000000000)
)

// A generator of a zipfian distribution. It produces a sequence of items,
// such that some items are more popular than others, according to a zipfian
// distribution. When you construct an instance of this class, you specify
// the number of items in the set to draw from, either by specifying
// an itemCount(so that the sequence is of items from 0 to itemCount-1) or
// by specifying a min and a max (so that the sequence is of items from min
// to max inclusive). After you construct the instance, you can change
// the number of items by calling NextInt(itemCount) or Next(itemCount).
// Unlike ZipfianGenerator, this class scatters the "popular" items across
// the item space. Use this, instead of ZipfianGenerator, if you don't want
// the head of the distribution(the popular items) clustered together.
type ScrambledZipfianGenerator struct {
	*IntegerGeneratorBase
	gen       *ZipfianGenerator
	min       int64
	max       int64
	itemCount int64
}

// Create a zipfian generator for the specified number of items.
func NewScrambledZipfianGeneratorByItems(items int64) *ScrambledZipfianGenerator {
	return NewScrambledZipfianGenerator(0, items-1)
}

// Create a zipfian generator for items between min and max (inclusive) for
// the specified zipfian constant. If you use a zipfian constant other than
// 0.99, this will take a long time complete because we need to recompute
// zeta.
func NewScrambledZipfianGeneratorConstant(min, max int64, constant float64) *ScrambledZipfianGenerator {
	var gen *ZipfianGenerator
	itemCount := max - min + 1
	if constant == UsedZipfianConstant {
		gen = NewZipfianGenerator(0, itemCount, constant, Zetan)
	} else {
		zeta := zetaStatic(0, itemCount, constant, 0)
		gen = NewZipfianGenerator(0, itemCount, constant, zeta)
	}
	return &ScrambledZipfianGenerator{
		IntegerGeneratorBase: NewIntegerGeneratorBase(min),
		gen:                  gen,
		min:                  min,
		max:                  max,
		itemCount:            max - min + 1,
	}
}

// Create a zipfian generator for items between min and max(inclusive).
func NewScrambledZipfianGenerator(min, max int64) *ScrambledZipfianGenerator {
	return NewScrambledZipfianGeneratorConstant(min, max, ZipfianConstant)
}

// Return the next int in the sequence.
func (self *ScrambledZipfianGenerator) NextInt() int64 {
	return self.Next()
}

// return the next item in the sequence.
func (self *ScrambledZipfianGenerator) Next() int64 {
	ret := self.gen.NextLong()
	ret = self.min + int64(FNVHash64(uint64(ret))%uint64(self.itemCount))
	self.SetLastInt(ret)
	return ret
}

func (self *ScrambledZipfianGenerator) NextString() string {
	return self.IntegerGeneratorBase.NextString(self)
}

// Since the values are scrambed (hopefully uniformly), the mean is simply
// the middle of the range.
func (self *ScrambledZipfianGenerator) Mean() float64 {
	return float64(self.min+self.max) / 2.0
}

// Hash a integer value.
func Hash(value int64) uint64 {
	return FNVHash64(uint64(value))
}

const (
	FNVOffsetBasis32 = uint32(0x811c9dc5)
	FNVPrime32       = uint32(16777619)
)

// 32 bit FNV hash.
// Refer to http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
func FNVHash32(value uint32) uint32 {
	hash := FNVOffsetBasis32
	for i := 0; i < 4; i++ {
		octet := value & 0x00FF
		value >>= 8

		hash ^= octet
		hash *= FNVPrime32
	}
	return hash
}

const (
	FNVOffsetBasis64 = uint64(0xCBF29CE484222325)
	FNVPrime64       = uint64(1099511628211)
)

// 64 bit FNV hash.
// Refer to http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
func FNVHash64(value uint64) uint64 {
	hash := FNVOffsetBasis64
	for i := 0; i < 8; i++ {
		octet := value & 0x00FF
		value >>= 8

		hash ^= octet
		hash *= FNVPrime64
	}
	return hash
}
