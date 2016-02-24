package yabf

import (
	"bufio"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Client interface {
	Main()
}

type Loader struct {
	args *Arguemnts
}

func NewLoader(args *Arguemnts) *Loader {
	return &Loader{
		args: args,
	}
}

func (self *Loader) Main() {
	// TODO to impl
}

type Runner struct {
	args *Arguemnts
}

func NewRunner(args *Arguemnts) *Runner {
	return &Runner{
		args: args,
	}
}

func (self *Runner) Main() {
	// TODO to impl
}

type Shell struct {
	args *Arguemnts
}

func NewShell(args *Arguemnts) *Shell {
	return &Shell{
		args: args,
	}
}

var (
	regexCmd *regexp.Regexp
)

func init() {
	regexCmd = regexp.MustCompile(`\s+`)
}

func (self *Shell) Main() {
	Println("YABF Command Line Client")
	Println(`Type "help" for command line help`)

	db, err := NewDB(self.args.Database, self.args.Properties)
	if err != nil {
		ExitOnError("fail to create specified db, error: %s", err)
	}
	db.SetProperties(self.args.Properties)
	err = db.Init()
	if err != nil {
		ExitOnError("fail to init db, error: %s", err)
	}

	Println("Connected.")
	scanner := bufio.NewScanner(os.Stdin)
	tableName := PropertyTableNameDefault
	for {
		Printf("> ")
		if !scanner.Scan() {
			break
		}
		startTime := time.Now().UnixNano()
		line := scanner.Text()
	READLINE:
		switch line {
		case "":
		case "help":
			self.help()
			continue
		case "quit":
			return
		default:
			parts := regexCmd.Split(line, -1)
			length := len(parts)
			switch parts[0] {
			case "table":
				switch length {
				case 1:
					Println(`Using table "%s"`, tableName)
				case 2:
					tableName = parts[1]
					Println(`Using table "%s"`, tableName)
				default:
					Println(`Error: syntax is "table tablename"`)
				}
			case "read":
				switch length {
				case 1:
					Println(`Error: syntax is "read keyname [field1 field2 ...]"`)
				default:
					key := parts[1]
					fields := make([]string, 0, length-2)
					for i := 2; i < length; i++ {
						fields = append(fields, parts[i])
					}
					ret, status := db.Read(tableName, key, fields)
					Println("Return code: %s", status)
					for k, v := range ret {
						Println("%s=%s", k, v)
					}
				}
			case "scan":
				if length < 3 {
					Println(`Error: syntax is "scan keyname scanlength [field1 field2 ...]"`)
				} else {
					key := parts[1]
					scanLength, err := strconv.ParseInt(parts[2], 0, 64)
					if err != nil {
						Println("invalid scanlength: %s", parts[2])
						break
					}
					fields := make([]string, 0, length-3)
					for i := 3; i < length; i++ {
						fields = append(fields, parts[i])
					}
					ret, status := db.Scan(tableName, key, scanLength, fields)
					Println("Return code: %s", status)
					if len(ret) == 0 {
						Println("0 records")
					} else {
						Println("--------------------------------")
						count := 0
						for _, kv := range ret {
							Println("Record %d", count)
							count++
							for k, v := range kv {
								Println("%s=%s", k, v)
							}
							Println("--------------------------------")
						}
					}
				}
			case "update":
				if length < 3 {
					Println(`Error: syntax is "update keyname name1=value1 [name2=value2 ...]"`)
				} else {
					key := parts[1]
					values := make(map[string]Binary)
					for i := 2; i < length; i++ {
						nv := strings.Split(parts[i], "=")
						if len(nv) != 2 {
							Println(`Error: invalid name=value %s`, parts[i])
							break READLINE
						}
						values[nv[0]] = []byte(nv[1])
					}
					status := db.Update(tableName, key, values)
					Println("Result: %s", status)
				}
			case "insert":
				if length < 3 {
					Println(`Error: syntax is "insert keyname name1=value1 [name2=value2 ...]"`)
				} else {
					key := parts[1]
					values := make(map[string]Binary)
					for i := 2; i < length; i++ {
						nv := strings.Split(parts[i], "=")
						if len(nv) != 2 {
							Println(`Error: invalid name=value %s`, parts[i])
							break READLINE
						}
						values[nv[0]] = []byte(nv[1])
					}
					status := db.Insert(tableName, key, values)
					Println("Result: %s", status)
				}
			case "delete":
				if length != 2 {
					Println(`Error: syntax is "delete keyname"`)
				} else {
					status := db.Delete(tableName, parts[1])
					Println("Result: %s", status)
				}
			default:
				Println(`Error: unknown command "%s"`, parts[0])
			}
		}
		Println("%d ms", (time.Now().UnixNano()-startTime)/1000)
	}
}

func (self *Shell) help() {
	helpFormat := `Commands
  read key [field1 field2 ...] - Read a record
  scan key recordcount [field1 field2 ...] - Scan starting at key
  insert key name1=value1 [name2=value2 ...] - Insert a new record
  update key name1=value1 [name2=value2 ...] - Update a record
  delete key - Delete a record
  table [tablename] - Get or [set] the name of the table
  quit - Quit`
	Println(helpFormat)
}
