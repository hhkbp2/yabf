package yabf

import (
	"fmt"
)

type Properties map[string]string

func (self Properties) Get(key string) string {
	v, _ := self[key]
	return v
}

func (self Properties) GetDefault(key string, defaultValue string) string {
	if v, ok := self[key]; ok {
		return v
	}
	return defaultValue
}

func Output(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println("")
}

func OutputProperties(p Properties) {
	Output("***************** properties *****************")
	if p != nil {
		for k, v := range p {
			Output("\"%s\"=\"%s\"", k, v)
		}
	}
	Output("**********************************************")
}
