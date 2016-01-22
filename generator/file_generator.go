package generator

import (
	"os"
)

type FileGenerator struct {
	filename string
	current  string
	file     *os.File
	scanner  *bufio.Scanner
}

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

func (self *FileGenerator) NextString() string {
	if self.scanner.Scan() {
		self.current = self.scanner.Text()
		return self.current
	}
	return ""
}

func (self *FileGenerator) LastString() string {
	return self.current
}

func (self *FileGenerator) ReloadFile() error {
	self.file.Close()
	f, err = os.Open(self.filename)
	if err != nil {
		return err
	}
	self.file = f
	self.scanner = bufio.NewScanner(f)
	return nil
}
