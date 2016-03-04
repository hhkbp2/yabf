package generator

import (
	"github.com/hhkbp2/testify/require"
	"strconv"
	"testing"
)

func TestHotspotIntegerGenerator(t *testing.T) {
	lowerBound := int64(1000)
	upperBound := int64(2000)
	hotsetFraction := float64(0.2)
	hotOpnFraction := float64(0.99)
	var g IntegerGenerator
	hig := NewHotspotIntegerGenerator(lowerBound, upperBound, hotsetFraction, hotOpnFraction)
	g = hig
	last := g.NextInt()
	interval := upperBound - lowerBound
	hotsetHigh := lowerBound + int64(float64(interval)*hotsetFraction)
	require.True(t, last <= hotsetHigh)
	require.Equal(t, last, g.LastInt())
	str := g.NextString()
	last, err := strconv.ParseInt(str, 0, 64)
	require.Nil(t, err)
	require.True(t, last <= hotsetHigh)
	require.Equal(t, str, g.LastString())
}
