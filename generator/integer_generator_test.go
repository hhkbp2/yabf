package generator

import (
	"fmt"
	"github.com/hhkbp2/testify/require"
	"testing"
)

func TestConstantIntegerGenerator(t *testing.T) {
	value := int64(100)
	var g IntegerGenerator
	g = NewConstantIntegerGenerator(value)
	require.Equal(t, value-1, g.LastInt())
	for i := 0; i < 10; i++ {
		require.Equal(t, value, g.NextInt())
		require.Equal(t, value-1, g.LastInt())
		require.Equal(t, fmt.Sprintf("%d", value), g.NextString())
		require.Equal(t, fmt.Sprintf("%d", value-1), g.LastString())
		require.Equal(t, float64(value), g.Mean())
	}
}

func TestSkewedLatestGenerator(_ *testing.T) {
	// TODO add impl
}
