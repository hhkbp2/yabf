package generator

import (
	"github.com/hhkbp2/testify/require"
	"strconv"
	"testing"
)

func TestHistogramGenerator(t *testing.T) {
	buckets := []int64{1, 2, 3, 4}
	blockSize := int64(1)
	var area int64
	for i := 0; i < len(buckets); i++ {
		area += buckets[i] * blockSize
	}
	hg := NewHistogramGenerator(buckets, blockSize)
	times := 10
	runTestHistogramGenerator(t, hg, times, area)
	filename := "histogram_generator.data"
	hg, err := NewHistogramGeneratorFromFile(filename)
	require.Nil(t, err)
	runTestHistogramGenerator(t, hg, times, area)
}

func runTestHistogramGenerator(t *testing.T, g IntegerGenerator, times int, area int64) {
	for i := 0; i < times; i++ {
		last := g.NextInt()
		require.True(t, last <= area)
		require.Equal(t, g.LastInt(), last)
		str := g.NextString()
		v, err := strconv.ParseInt(str, 0, 64)
		require.Nil(t, err)
		require.True(t, v <= area)
		require.Equal(t, str, g.LastString())
	}
}
