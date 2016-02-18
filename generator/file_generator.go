package generator

import (
	"bufio"
	"os"
)

// A generator, whose sequence is the lines of a file.
type FileGenerator struct {
	filename string
	current  string
	file     *os.File
	scanner  *bufio.Scanner
}

// Create a FileGenerator with the given file.
func NewFileGenerator(filename string) (*FileGenerator, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	object := &FileGenerator{
		filename: filename,
		current:  "",
		file:     f,
		scanner:  bufio.NewScanner(f),
	}
	return object, nil
}

// Return the next string of the sequence, which is the next line of the file.
func (self *FileGenerator) NextString() string {
	if self.scanner.Scan() {
		self.current = self.scanner.Text()
		return self.current
	}
	return ""
}

// Return the previous read line.
func (self *FileGenerator) LastString() string {
	return self.current
}

// Reopen the file to reuse values.
func (self *FileGenerator) ReloadFile() error {
	self.file.Close()
	f, err := os.Open(self.filename)
	if err != nil {
		return err
	}
	self.file = f
	self.scanner = bufio.NewScanner(f)
	return nil
}
