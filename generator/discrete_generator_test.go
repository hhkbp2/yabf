package generator

import (
	"fmt"
	"github.com/hhkbp2/testify/require"
	"strconv"
	"testing"
)

func TestDiscreteGenerator(t *testing.T) {
	var g Generator
	dg := NewDiscreteGenerator()
	g = dg
	startWeight := float64(1.0)
	total := 4
	for i := 0; i < total; i++ {
		dg.AddValue(startWeight, fmt.Sprintf("%g", startWeight+float64(i)))
	}
	for i := 0; i < total; i++ {
		n := g.NextString()
		v, err := strconv.ParseFloat(n, 64)
		require.Nil(t, err)
		require.True(t, v < startWeight+float64(total))
		require.Equal(t, n, g.LastString())
	}
}
