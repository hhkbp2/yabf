package yabf

import (
	"github.com/hhkbp2/testify/require"
	"testing"
)

func TestProperties(t *testing.T) {
	k := "key"
	v := "value"
	p := NewProperties()
	p.Add(k, v)
	x := p.Get(k)
	require.Equal(t, v, x)
	x = p.GetDefault(k, "other")
	require.Equal(t, v, x)
	k1 := "a"
	v1 := "b"
	p2 := map[string]string{k1: v1}
	p.Merge(p2)
	z := p.Get(k1)
	require.Equal(t, v1, z)
}

func TestToTime(t *testing.T) {
	millisecond := int64(12345)
	nanosecond := MillisecondToNanosecond(millisecond)
	require.Equal(t, millisecond*1000*1000, nanosecond)
	second := MillisecondToSecond(millisecond)
	require.Equal(t, millisecond/1000, second)
	v := SecondToNanosecond(second)
	require.Equal(t, second*1000*1000*1000, v)
	v = NanosecondToMicrosecond(nanosecond)
	require.Equal(t, nanosecond/1000, v)
	v = NanosecondToMillisecond(nanosecond)
	require.Equal(t, nanosecond/1000/1000, v)
}

func TestRandomBytes(t *testing.T) {
	length := int64(100)
	b1 := RandomBytes(length)
	require.Equal(t, length, int64(len(b1)))
	b2 := RandomBytes(length)
	require.Equal(t, length, int64(len(b2)))
	require.NotEqual(t, b1, b2)
}
