package generator

import (
	"github.com/hhkbp2/testify/require"
	"strconv"
	"testing"
)

func TestZipfianGenerator(t *testing.T) {
	runTestZipfianGenerator(t, func(min, max int64) IntegerGenerator {
		return NewZipfianGeneratorByInterval(min, max)
	})
}

func TestScrambledZipfianGenerator(t *testing.T) {
	runTestZipfianGenerator(t, func(min, max int64) IntegerGenerator {
		return NewScrambledZipfianGenerator(min, max)
	})
}

func runTestZipfianGenerator(t *testing.T, f func(min, max int64) IntegerGenerator) {
	min := int64(1000)
	max := int64(2000)
	g := f(min, max)
	total := 10
	for i := 0; i < total; i++ {
		last := g.NextInt()
		require.True(t, last >= min && last <= max)
		require.Equal(t, last, g.LastInt())
		str := g.NextString()
		v, err := strconv.ParseInt(str, 0, 64)
		require.Nil(t, err)
		require.True(t, v >= min && v <= max)
	}
}
