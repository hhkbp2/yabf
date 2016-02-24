package yabf

import (
	"bufio"
	"fmt"
	g "github.com/hhkbp2/yabf/generator"
	"math/rand"
	"os"
	"regexp"
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

func (self Properties) Merge(other Properties) Properties {
	for k, v := range other {
		self[k] = v
	}
	return self
}

var (
	regexIgnorable *regexp.Regexp
	regexProperty  *regexp.Regexp
)

func init() {
	regexIgnorable = regexp.MustCompile(`\s*(#.*)?`)
	regexProperty = regexp.MustCompile(`\s*([\d.]+)\s*=\s*([\d.]+)\s*`)
}

func LoadProperties(fileName string) (Properties, error) {
	ret := NewProperties()
	f, err := os.Open(fileName)
	if err != nil {
		return ret, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if regexIgnorable.MatchString(line) {
			continue
		}
		parts := regexIgnorable.FindAllString(line, -1)
		if parts == nil {
			return ret, g.NewErrorf("invalid workload file: %s, line: %s", fileName, line)
		}
		ret.Add(parts[0], parts[1])
	}
	return ret, scanner.Err()
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

func Printf(format string, args ...interface{}) {
	fmt.Fprintf(OutputDest, format, args...)
}

func Println(format string, args ...interface{}) {
	Printf(format, args...)
	fmt.Fprintln(OutputDest, "")
}

func PrintProperties(p Properties) {
	Println("***************** properties *****************")
	if p != nil {
		for k, v := range p {
			Println("\"%s\"=\"%s\"", k, v)
		}
	}
	Println("**********************************************")
}
