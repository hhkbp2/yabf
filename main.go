package yabf

import (
	"flag"
	"fmt"
	"os"
)

var (
	filename = flag.String("P", "", "Specify a property file")
)

func Usage() {
	fmt.Fprint(os.Stderr, "YABF Command Line Client\n")
	fmt.Fprint(os.Stderr, "Usage: ", os.Args[0], " [options]\n")
	fmt.Fprint(os.Stderr, "Options:\n")
	flag.PrintDefaults()
	fmt.Fprint(os.Stderr, "\n")
}

func Main() {
	flag.Usage = Usage
	flag.Parse()
}

func main() {
	Main()
}
