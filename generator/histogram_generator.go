package generator

import (
	"bufio"
	"container/list"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func NewErrorf(format string, args ...interface{}) error {
	return errors.New(fmt.Sprintf(format, args...))
}

// Generate integers according to a histogram distribution. The histogram
// buckets are of width one, but the values are multiplied by a block size.
// Therefore, instead of drawing sizes uniformly at random within each bucket,
// we always draw the largest value in the current bucket, so the value drawn
// is always a multiple of blockSize.
// The minimum value this distribution returns is blockSize(not zero).
type HistogramGenerator struct {
	*IntegerGeneratorBase
	blockSize    int64
	buckets      []int64
	area         int64
	weightedArea int64
	meanSize     float64
}

type bucket struct {
	Index int64
	Area  int64
}

func NewHistogramGeneratorFromFile(file string) (*HistogramGenerator, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	l := list.New()
	scanner := bufio.NewScanner(f)
	lineCount := 0
	var size int64
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, "\t")
		if len(parts) != 2 {
			return nil, NewErrorf("invalid format for histogram file: %s", file)
		}
		if lineCount == 0 {
			if parts[0] != "BlockSize" {
				return nil, NewErrorf(
					"First line of histogram file is not the BlockSize")
			}
			s, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, err
			}
			size = int64(s)
		} else {
			k, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, err
			}
			v, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, err
			}
			l.PushBack(&bucket{
				Index: int64(k),
				Area:  int64(v),
			})
		}
		lineCount++
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	buckets := make([]int64, l.Len())
	for e := l.Front(); e != nil; e = e.Next() {
		b := e.Value.(*bucket)
		buckets[b.Index] = buckets[b.Area]
	}
	return NewHistogramGenerator(buckets, size), nil
}

func NewHistogramGenerator(buckets []int64, blockSize int64) *HistogramGenerator {
	object := &HistogramGenerator{
		IntegerGeneratorBase: NewIntegerGeneratorBase(0),
		blockSize:            blockSize,
		buckets:              buckets,
	}
	object.init()
	return object
}

func (self *HistogramGenerator) init() {
	var area, weightedArea int64
	for i := 0; i < len(self.buckets); i++ {
		area += self.buckets[i]
		weightedArea = int64(i) * self.buckets[i]
	}
	self.area = area
	self.weightedArea = weightedArea
	self.meanSize = float64(self.blockSize) * float64(self.weightedArea)
}

func (self *HistogramGenerator) NextInt() int64 {
	number := NextInt64(self.area)
	var i int
	for i = 0; i < len(self.buckets)-1; i++ {
		number -= self.buckets[i]
		if number <= 0 {
			return int64(i+1) * self.blockSize
		}
	}
	next := int64(i) * self.blockSize
	self.SetLastInt(next)
	return next
}

func (self *HistogramGenerator) NextString() string {
	return self.IntegerGeneratorBase.NextString(self)
}

func (self *HistogramGenerator) Mean() float64 {
	return self.meanSize
}
