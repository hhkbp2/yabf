package generator

import (
	"fmt"
	"github.com/hhkbp2/testify/require"
	"testing"
)

func TestCounterGenerator(t *testing.T) {
	value := int64(100)
	var g IntegerGenerator
	g = NewCounterGenerator(value)
	require.Equal(t, value-1, g.LastInt())
	for i := int64(0); i < 5; i++ {
		require.Equal(t, value+i, g.NextInt())
		require.Equal(t, value+i, g.LastInt())
	}
	for i := int64(5); i < 5; i++ {
		require.Equal(t, fmt.Sprintf("%d", value+i), g.NextString())
		require.Equal(t, fmt.Sprintf("%d", value+i), g.LastString())
	}
	require.Panics(t, func() { g.Mean() })
}

func TestAcknowledgedCounterGenerator(t *testing.T) {
	value := int64(100)
	total := int64(10)
	var g IntegerGenerator
	acg := NewAcknowledgedCounterGenerator(value)
	g = acg
	require.Equal(t, value-1, g.LastInt())
	for i := int64(0); i < total/2; i++ {
		require.Equal(t, value+i, g.NextInt())
		require.Equal(t, value-1, g.LastInt())
	}
	for i := total / 2; i < total; i++ {
		require.Equal(t, fmt.Sprintf("%d", value+i), g.NextString())
		require.Equal(t, fmt.Sprintf("%d", value-1), g.LastString())
	}
	for i := int64(0); i < total; i++ {
		acg.Acknowledge(value + i)
		require.Equal(t, value+i, g.LastInt())
		require.Equal(t, fmt.Sprintf("%d", value+i), g.LastString())
	}
	require.Equal(t, value+total, acg.NextInt())
	require.Panics(t, func() { g.Mean() })
}
