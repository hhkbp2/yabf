package generator

import (
	"github.com/hhkbp2/testify/require"
	"strconv"
	"testing"
)

func TestUniformIntegerGenerator(t *testing.T) {
	lowerBound := int64(1000)
	upperBound := int64(2000)
	var g IntegerGenerator
	uig := NewUniformIntegerGenerator(lowerBound, upperBound)
	g = uig
	total := 10
	for i := 0; i < total; i++ {
		last := g.NextInt()
		require.True(t, last >= lowerBound && last <= upperBound)
		require.Equal(t, last, g.LastInt())
		str := g.NextString()
		v, err := strconv.ParseInt(str, 0, 64)
		require.Nil(t, err)
		require.True(t, v >= lowerBound && v <= upperBound)
		require.Equal(t, float64((lowerBound+upperBound)/2.0), g.Mean())
	}
}
