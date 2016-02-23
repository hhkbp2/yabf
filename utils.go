package yabf

import (
	"fmt"
	"math/rand"
	"time"
)

type Properties map[string]string

func NewProperties() Properties {
	return make(Properties)
}

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

func (self Properties) Add(key, value string) {
	self[key] = value
}

const (
	RandomBytesLength = 6
)

func makeRandomBytes() []byte {
	buf := make([]byte, RandomBytesLength)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	v := int8(r.Int63())
	buf[0] = byte(' ' + (v & 31))
	buf[1] = byte(' ' + ((v >> 5) & 63))
	buf[2] = byte(' ' + ((v >> 10) & 95))
	buf[3] = byte(' ' + ((v >> 15) & 31))
	buf[4] = byte(' ' + ((v >> 20) & 63))
	buf[5] = byte(' ' + ((v >> 25) & 95))
	return buf
}

func RandomBytes(length int64) []byte {
	ret := make([]byte, length)
	addSize := int64(0)
	for i := int64(0); i < length; i += addSize {
		b := makeRandomBytes()
		addSize = int64(len(b))
		for j := int64(0); (j < addSize) && (i+j < length); j++ {
			ret[i+j] = b[j]
		}
	}
	return ret
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
