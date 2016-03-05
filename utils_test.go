package yabf

import (
	"github.com/hhkbp2/testify/require"
	"testing"
)

func TestToTime(t *testing.T) {
	millisecond := int64(12345)
	nanosecond := MillisecondToNanosecond(millisecond)
	require.Equal(t, millisecond*1000*1000, nanosecond)
	second := MillisecondToSecond(millisecond)
	require.Equal(t, millisecond/1000, second)
}
