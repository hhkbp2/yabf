package yabf

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type MakeDBFunc func() DB

var (
	Commands = map[string]bool{
		"load":  true,
		"run":   true,
		"shell": true,
	}
	Databases = map[string]MakeDBFunc{
		"cloudtable": func() DB {
			// TODO impl this
			return nil
		},
		"basic": func() DB {
			return NewBasicDB()
		},
	}
	OptionPrefixes = []string{"--", "-"}
	OptionList     = []*Option{
		&Option{
			Name:            "P",
			HasArgument:     true,
			HasDefaultValue: false,
			Doc:             "specify workload file",
		},
		&Option{
			Name:            "p",
			HasArgument:     true,
			HasDefaultValue: false,
			Doc:             "specify a property value",
		},
		&Option{
			Name:            "s",
			HasArgument:     false,
			HasDefaultValue: true,
			DefaultValue:    false,
			Doc:             "Print status to stderr",
		},
		&Option{
			Name:            "db",
			HasArgument:     true,
			HasDefaultValue: false,
			Doc:             "use a specified DB class(can also set the \"db\" property)",
		},
		&Option{
			Name:            "table",
			HasArgument:     true,
			HasDefaultValue: true,
			DefaultValue:    PropertyTableNameDefault,
			Doc:             "use the table name instead of the default %s",
		},
		&Option{
			Name:            "h",
			HasArgument:     false,
			HasDefaultValue: false,
			Doc:             "show this help message and exit",
		},
		&Option{
			Name:            "help",
			HasArgument:     false,
			HasDefaultValue: false,
			Doc:             "show this help message and exit",
		},
	}
	Options = make(map[string]*Option)

	ProgramName = ""
	Args        *Arguemnts
	OutputDest  *os.File
)

type Option struct {
	Name            string
	HasArgument     bool
	HasDefaultValue bool
	DefaultValue    interface{}
	Doc             string
}

type Arguemnts struct {
	Command  string
	Database string
	Options  map[string]interface{}
	Properties
}

func Usage() {
	usageFormat := `usage: %s command database [options]

Commands:
  load               Execute the load phase
  run                Execute the transaction phase
  shell              Interactive mode

Databases:
  basic              A demo database that does nothing but echo the operations
  cloudtable         A distributed KV store

Options:
  -P filename      : specify workload file
  -p name=value    : specify a property value
  -s               : Print status to stderr
  -db classname    : use a specified DB class(can also set the "db" property)
  -table tablename : use the table name instead of the default %s

Workload Files:
  There are various predefined workloads under workloads/ directory.

positional arguments:
  {load,run,shell}   Command to run.
  {cloudtable}       Database to test.

optional arguments:
  -h, --help         show this help message and exit
`
	fmt.Fprintf(os.Stderr, usageFormat, ProgramName, PropertyTableNameDefault)
}

func init() {
	ProgramName = filepath.Base(os.Args[0])

	// init options
	for i := 0; i < len(OptionList); i++ {
		o := OptionList[i]
		Options[o.Name] = o
	}
}

func ExitOnError(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}

func ParseArgs() *Arguemnts {
	if len(os.Args) <= 1 {
		ExitOnError("no enough argument")
	}

	index := 1
	firstArg := os.Args[index]
	if firstArg == "-h" || firstArg == "--help" {
		Usage()
		os.Exit(0)
	}
	index++

	_, ok := Commands[firstArg]
	if !ok {
		ExitOnError("unsupported command: %s", os.Args[1])
	}
	command := firstArg

	if len(os.Args) < 3 {
		ExitOnError("no enough argument")
	}

	_, ok = Databases[os.Args[index]]
	if !ok {
		ExitOnError("unsupported database: %s", os.Args[2])
	}
	database := os.Args[index]
	index++

	// init options to be returned with default values
	opts := make(map[string]interface{})
	for name, opt := range Options {
		if opt.HasDefaultValue {
			opts[name] = opt.DefaultValue
		}
	}
	// init property to be returned
	props := NewProperties()

	for i := index; i < len(os.Args); i++ {
		a := os.Args[i]
		for _, p := range OptionPrefixes {
			if strings.HasPrefix(a, p) {
				a = strings.TrimPrefix(a, p)
				break
			}
		}
		option, ok := Options[a]
		if !ok {
			ExitOnError("unknown option: %s", os.Args[i])
		}
		if option.HasArgument {
			i++
			if !(i < len(os.Args)) {
				ExitOnError("missing argument for option: %s", option.Name)
			}
			arg := os.Args[i]
			switch option.Name {
			case "p":
				// it's a property, should be in `k=v` form
				parts := strings.Split(arg, "=")
				if len(parts) != 2 {
					ExitOnError("invalid property: %s", arg)
				}
				props.Add(parts[0], parts[1])
			case "h", "help":
				Usage()
				os.Exit(0)
			default:
				opts[option.Name] = arg
			}
		} else {
			opts[option.Name] = true
		}
	}
	return &Arguemnts{
		Command:    command,
		Database:   database,
		Options:    opts,
		Properties: props,
	}
}
