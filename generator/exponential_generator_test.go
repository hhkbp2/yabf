package generator

import (
	"github.com/hhkbp2/testify/require"
	"strconv"
	"testing"
)

func TestExponentGenerator(t *testing.T) {
	total := 100
	recordCount := int64(10000)
	percentile := float64(95)
	fraction := float64(0.8571428571)
	var g IntegerGenerator
	eg := NewExponentialGenerator(percentile, float64(recordCount)*fraction)
	g = eg
	for i := 0; i < total; i++ {
		last := g.NextInt()
		require.True(t, last >= recordCount)
		require.Equal(t, g.LastInt(), last)
		str := g.NextString()
		v, err := strconv.ParseInt(str, 0, 64)
		require.Nil(t, err)
		require.True(t, v >= recordCount)
		require.Equal(t, g.LastString(), str)
	}
}
