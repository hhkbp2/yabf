package generator

import (
	"fmt"
	"github.com/hhkbp2/testify/require"
	"testing"
)

func TestFileGenerator(t *testing.T) {
	filename := "file_generator.data"
	var g Generator
	fg, err := NewFileGenerator(filename)
	require.Nil(t, err)
	g = fg
	total := 5
	for i := 1; i < total; i++ {
		last := g.NextString()
		require.Equal(t, last, fmt.Sprintf("%d", i))
		require.Equal(t, last, g.LastString())
	}
	err = fg.ReloadFile()
	require.Nil(t, err)
	defer fg.Close()
	for i := 1; i < total; i++ {
		last := g.NextString()
		require.Equal(t, last, fmt.Sprintf("%d", i))
		require.Equal(t, last, g.LastString())
	}
}
