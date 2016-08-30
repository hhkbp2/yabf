package yabf

import (
	"os"
	"path/filepath"
	"runtime"
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
		"basic": func() DB {
			return NewBasicDB()
		},
		"simple": func() DB {
			return NewGoodBadUglyDB()
		},
	}
	OptionPrefixes = []string{"--", "-"}
	OptionList     = []*Option{
		&Option{
			Name:            "P",
			HasArgument:     true,
			HasDefaultValue: false,
			Doc:             "specify workload file",
			Operation: func(context interface{}, value string) {
				props, _ := context.(Properties)
				propsFromFile, err := LoadProperties(value)
				if err != nil {
					ExitOnError(err.Error())
				}
				props.Merge(propsFromFile)
			},
		},
		&Option{
			Name:            "p",
			HasArgument:     true,
			HasDefaultValue: false,
			Doc:             "specify a property value",
			Operation: func(context interface{}, value string) {
				props, _ := context.(Properties)
				// it's a property, should be in `k=v` form
				parts := strings.Split(value, "=")
				if len(parts) != 2 {
					ExitOnError("invalid property: %s", value)
				}
				props.Add(parts[0], parts[1])
			},
		},
		&Option{
			Name:            "s",
			HasArgument:     false,
			HasDefaultValue: false,
			Doc:             "show status (default: no status)",
		},
		&Option{
			Name:            "l",
			HasArgument:     true,
			HasDefaultValue: false,
			Doc:             "use label for status (e.g. to label one experiment out of a whole batch)",
		},
		&Option{
			Name:            "db",
			HasArgument:     true,
			HasDefaultValue: false,
			Doc:             "use a specified DB class(can also set the \"db\" property)",
			Operation: func(context interface{}, value string) {
				props, _ := context.(Properties)
				props.Add(PropertyDB, value)
			},
		},
		&Option{
			Name:            "table",
			HasArgument:     true,
			HasDefaultValue: true,
			DefaultValue:    PropertyTableNameDefault,
			Doc:             "use the table name instead of the default %s",
			Operation: func(context interface{}, value string) {
				props, _ := context.(Properties)
				props.Add(PropertyTableName, value)
			},
		},
		&Option{
			Name:            "x",
			HasArgument:     true,
			HasDefaultValue: true,
			DefaultValue:    "info",
			Doc:             "specify the level name of log output (dafault: info)",
			Operation: func(context interface{}, value string) {
				levelName := strings.ToLower(value)
				level, ok := nameToLevels[levelName]
				if !ok {
					ExitOnError("invalid log level name: %s", value)
				}
				logLevel = level
			},
		},
		&Option{
			Name:            "h",
			HasArgument:     false,
			HasDefaultValue: false,
			Doc:             "show this help message and exit",
			Operation: func(context interface{}, value string) {
				Usage()
				os.Exit(0)
			},
		},
		&Option{
			Name:            "help",
			HasArgument:     false,
			HasDefaultValue: false,
			Doc:             "show this help message and exit",
			Operation: func(context interface{}, value string) {
				Usage()
				os.Exit(0)
			},
		},
		&Option{
			Name:            "v",
			HasArgument:     false,
			HasDefaultValue: false,
			Doc:             "show the version number and exit",
			Operation: func(context interface{}, value string) {
				Version()
				os.Exit(0)
			},
		},
		&Option{
			Name:            "version",
			HasArgument:     false,
			HasDefaultValue: false,
			Doc:             "show the version number and exit",
			Operation: func(context interface{}, value string) {
				Version()
				os.Exit(0)
			},
		},
	}
	Options = make(map[string]*Option)

	ProgramName = ""
	MainVersion = "0.1.0"
)

type OptionOperationFunc func(context interface{}, value string)

type Option struct {
	Name            string
	HasArgument     bool
	HasDefaultValue bool
	DefaultValue    string
	Doc             string
	Operation       OptionOperationFunc
}

type Arguemnts struct {
	Command  string
	Database string
	Options  map[string]string
	Properties
}

func Usage() {
	usageFormat := `usage: %s command database [options]

Commands:
  load               Execute the load phase
  run                Execute the transaction phase
  shell              Interactive mode

Databases:
  simple             A demo database that does nothing
  basic              A demo database that does nothing but echo the operations
  mysql              Mysql server

Options:
  -db classname      use a specified DB class(can also set the "db" property)
  -l label           use label for status (e.g. to label one experiment out of a whole batch)
  -P filename        specify workload file
  -p name=value      specify a property value
  -s                 show status (default: no status)
  -table tablename   use the table name instead of the default %s
  -x levelname       specify the level name of log output (dafault: info)

Workload Files:
  There are various predefined workloads under workloads/ directory.

positional arguments:
  {load,run,shell}   Command to run.
  {mysql}            Database to test.

optional arguments:
  -h, --help         show this help message and exit
  -v, --version      show the version number and exit`
	Printf(usageFormat, ProgramName, PropertyTableNameDefault)
}

func Version() {
	versionFormat := `%s %s (git rev: %s)`
	Printf(versionFormat, ProgramName, MainVersion, GitVersion)
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
	EPrintf(format, args...)
	os.Exit(1)
}

func ParseArgs() *Arguemnts {
	if len(os.Args) <= 1 {
		ExitOnError("no enough argument")
	}

	index := 1
	firstArg := os.Args[index]
	switch firstArg {
	case "-h", "--help":
		Usage()
		os.Exit(0)
	case "-v", "--version":
		Version()
		os.Exit(0)
	}
	index++

	command := firstArg
	_, ok := Commands[command]
	if !ok {
		ExitOnError("unsupported command: %s", command)
	}

	if len(os.Args) < 3 {
		ExitOnError("no enough argument")
	}

	database := os.Args[index]
	_, ok = Databases[database]
	if !ok {
		ExitOnError("unsupported database: %s", database)
	}
	index++

	// init property to be returned
	props := NewProperties()
	props[PropertyDB] = database

	// init options to be returned with default values
	options := make(map[string]string)
	for name, opt := range Options {
		if opt.HasDefaultValue {
			options[name] = opt.DefaultValue
			if opt.Operation != nil {
				opt.Operation(props, opt.DefaultValue)
			}
		}
	}
	for i := index; i < len(os.Args); i++ {
		a := os.Args[i]
		for _, p := range OptionPrefixes {
			if strings.HasPrefix(a, p) {
				a = strings.TrimPrefix(a, p)
				break
			}
		}
		opt, ok := Options[a]
		if !ok {
			ExitOnError("unknown option: %s", os.Args[i])
		}
		if opt.HasArgument {
			i++
			if !(i < len(os.Args)) {
				ExitOnError("missing argument for option: %s", opt.Name)
			}
			arg := os.Args[i]
			if opt.Operation != nil {
				// invoke option specified operation
				opt.Operation(props, arg)
			} else {
				// default operation is to add it into option list for further process
				options[opt.Name] = arg
			}
		} else {
			// default value for options without argeumnt is "true"
			value := "true"
			options[opt.Name] = value
			if opt.Operation != nil {
				opt.Operation(props, value)
			}
		}
	}
	return &Arguemnts{
		Command:    command,
		Database:   database,
		Options:    options,
		Properties: props,
	}
}

func Main() {
	// enable MAXPROCS option
	runtime.GOMAXPROCS(runtime.NumCPU())
	args := ParseArgs()
	var client Client
	switch args.Command {
	case "shell":
		client = NewShell(args)
	case "load":
		client = NewLoader(args)
	case "run":
		client = NewRunner(args)
	default:
		ExitOnError("invalid command: %s", args.Command)
	}
	client.Main()
}
