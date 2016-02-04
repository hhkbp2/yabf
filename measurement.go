package yabf

import (
	"bufio"
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/codahale/hdrhistogram"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"time"
)

type MeasurementType uint8

const (
	MeasurementHistogram MeasurementType = 1 + iota
	MeasurementHDRHistogram
	MeasurementHDRHistogramAndHistogram
	MeasurementHDRHistogramAndRaw
	MeasurementTimeSeries
	MeasurementRaw
)

// Used to export the collected measuremrnts into a usefull format, for example
// human readable text or machine readable JSON.
type MeasurementExporter interface {
	// Write a measurement to the exported format. v should be int64 or float64
	Write(metric string, measurement string, v interface{}) error
	io.Closer
}

// A single measured metric (such as READ LATENCY)
type OneMeasurement interface {
	Measure(latency int64)
	GetName() string
	GetSummary() string
	// Export the current measurements to a suitable format.
	Export(exporter MeasurementExporter)
}

type OneMeasurementBase struct {
	Name string
}

func NewOneMeasurementBase(name string) *OneMeasurementBase {
	return &OneMeasurementBase{
		Name: name,
	}
}

func (self *OneMeasurementBase) GetName() string {
	return self.Name
}

// Collects latency measurements, and reports them when requested.
type Measurements interface {
	// Report a single value of a single metric. E.g. for read latency,
	// operation="READ" and latency is the measured value.
	Measure(operation string, latency int64)
	// Report a single value of a single metric. E.g. for read latency,
	// operation="READ" and latency is the measured value.
	MeasureIntended(operation string, latency int64)
}

// Write human readable text. Tries to emulate the previous print report method.
type TextMeasurementExporter struct {
	io.WriteCloser
	buf *bufio.Writer
}

func NewTextMeasurementExporter(w io.WriteCloser) *TextMeasurementExporter {
	return &TextMeasurementExporter{
		WriteCloser: w,
		buf:         bufio.NewWriter(w),
	}
}

func (self *TextMeasurementExporter) Write(metric string, measurement string, v interface{}) error {
	_, err := self.buf.WriteString(fmt.Sprintf("[%s], %s, %v\n", metric, measurement))
	return err
}

func (self *TextMeasurementExporter) Close() error {
	err := self.buf.Flush()
	err2 := self.Close()
	if err != nil {
		return err
	}
	return err2
}

type innerJSONMeasurement struct {
	Metric      string      `json:"metric"`
	Measurement string      `json:"measurement"`
	Value       interface{} `json:"value"`
}

type JSONMeasurementExporter struct {
	io.WriteCloser
	buf *bufio.Writer
}

func NewJSONMeasurementExporter(w io.WriteCloser) *JSONMeasurementExporter {
	return &JSONMeasurementExporter{
		WriteCloser: w,
		buf:         bufio.NewWriter(w),
	}
}

func (self *JSONMeasurementExporter) Write(metric string, measurement string, v interface{}) error {
	b, err := json.Marshal(&innerJSONMeasurement{
		Metric:      metric,
		Measurement: measurement,
		Value:       v,
	})
	if err != nil {
		return err
	}
	_, err = self.buf.Write(b)
	return err
}

func (self *JSONMeasurementExporter) Close() error {
	err := self.buf.Flush()
	err2 := self.Close()
	if err != nil {
		return err
	}
	return err2
}

// Export measurements into a machine readable JSON Array of measurement objects.
type JSONArrayMeasurementExporter struct {
	io.WriteCloser
	buf        *bufio.Writer
	afterFirst bool
}

func NewJSONArrayMeasurementExporter(w io.WriteCloser) *JSONArrayMeasurementExporter {
	object := &JSONArrayMeasurementExporter{
		WriteCloser: w,
		buf:         bufio.NewWriter(w),
		afterFirst:  false,
	}
	object.buf.WriteString("[")
	return object
}

func (self *JSONArrayMeasurementExporter) Write(metric string, measurement string, v interface{}) error {
	b, err := json.Marshal(&innerJSONMeasurement{
		Metric:      metric,
		Measurement: measurement,
		Value:       v,
	})
	if err != nil {
		return err
	}
	if self.afterFirst {
		_, err = self.buf.WriteString(",")
		if err != nil {
			return err
		}
	} else {
		self.afterFirst = true
	}
	_, err = self.buf.Write(b)
	return err
}

func (self *JSONArrayMeasurementExporter) Close() error {
	_, err := self.buf.WriteString("]")
	if err != nil {
		return err
	}
	err = self.buf.Flush()
	err2 := self.Close()
	if err != nil {
		return err
	}
	return err2
}

type RawDataPoint struct {
	timestamp time.Time
	value     int64
}

func NewRawDataPoint(value int64) {
	return &RawDataPoint{
		timestamp: time.Now(),
		value:     value,
	}
}

type RawDataPointSlice []*RawDataPoint

func (self RawDataPointSlice) Len() int {
	return len(self)
}

func (self RawDataPointSlice) Less(i, j int) bool {
	return self[i].value < self[j].value
}

func (self RawDataPointSlice) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

type OneMeasurementRaw struct {
	*OneMeasurementBase
	filePath       string
	file           *os.File
	noSummaryStats bool
	measurements   *list.List
	totalLatency   int64
	// A window of stats to print summary for at the next GetSummary() call.
	// It's suppose to be a one line summary, so we will just print count and
	// average.
	windowOperations   int64
	windowTotalLatency int64
}

func NewOneMeasurementRaw(name string, props Properties) (*OneMeasurementRaw, error) {
	filePath := props.GetDefault(OutputFilePath, OutputFilePathDefault)
	var output *os.File
	if len(filePath) == 0 {
		outpu = os.Stdout
	} else {
		f, err := os.Open(filePath)
		if err != nil {
			return nil, err
		}
		output = f
	}
	noSummaryStats, err := strconv.ParseBool(props.GetDefault(NoSummaryStats, NoSummaryStatsDefault))
	if err != nil {
		writer.Close()
		return nil, err
	}
	return &OneMeasurementRaw{
		OneMeasurementBase: NewOneMeasurementBase(name),
		filePath:           filePath,
		file:               output,
		noSummaryStats:     noSummaryStats,
		measurements:       list.New(),
	}
}

func (self *OneMeasurementRaw) Measure(latency int64) {
	self.totalLatency += latency
	self.windowTotalLatency += latency
	self.windowOperations++
	self.measurements.PushBack(NewDataPoint(latency))
}

func (self *OneMeasurementRaw) GetSummary() string {
	if self.windowOperations == 0 {
		return ""
	}
	ret := fmt.Sprintf("%s count: %d, average latency(us): %.2g",
		self.GetName(), self.windowOperations, float64(self.windowTotalLatency)/float64(self.windowOperations))
	self.windowOperations = 0
	self.windowTotalLatency = 0
	return ret
}

func (self *OneMeasurementRaw) Export(exporter MeasurementExporter) {
	self.file.WriteString(fmt.Sprintf(
		"%s latency raw data: op, timestamp(ms), latency(us)\n",
		self.GetName()))
	for e := self.measurements.First(); e != nil; e = e.Next() {
		p := e.Value.(*RawDataPoint)
		self.file.WriteString(fmt.Sprintf("%s,%d,%d",
			self.GetName(), p.timestamp, p.value))
	}
	if len(self.filePath) != 0 {
		self.file.Close()
	}
	total := self.measurements.Len()
	exporter.Write(self.GetName(), "Total Operations", total)
	if total > 0 && !self.noSummaryStats {
		s := make(RawDataPointSlice, 0, total)
		i := 0
		for e := self.measurements.First(); e != nil && i < total; e = e.Next() {
			p := e.Value.(*RawDataPoint)
			s[i] = p
			i++
		}
		exporter.Write(self.GetName(),
			"Below is a summary of latency in miscroseconds:", -1)
		exporter.Write(self.GetName(),
			"Average", float64(self.totalLatency)/float64(total))
		sort.Sort(s)
		name := self.GetName()
		exporter.Write(name, "Min", s[0].value)
		exporter.Write(name, "Max", s[total-1].value)
		exporter.Write(name, "p1", s[int(float64(total)*0.01)].value)
		exporter.Write(name, "p5", s[int(float64(total)*0.05)].value)
		exporter.Write(name, "p50", s[int(float64(total)*0.5)].value)
		exporter.Write(name, "p90", s[int(float64(total)*0.9)].value)
		exporter.Write(name, "p95", s[int(float64(total)*0.95)].value)
		exporter.Write(name, "p99", s[int(float64(total)*0.99)].value)
		exporter.Write(name, "p99.9", s[int(float64(total)*0.999)].value)
		exporter.Write(name, "p99.99", s[int(float64(total)*0.9999)].value)
	}
}

// Take measurements and maintain a histogram of a given metric, such as
// READ LATENCY.
type OneMeasurementHistogram struct {
	*OneMeasurementBase
	// Specify the range of latencies to track in the histogram.
	buckets int64
	// Groups operations in discrete blocks of 1m width.
	histogram []int64
	// Counts all operations outside the histogram's range.
	histogramOverflow int64
	// The total number of reported operations.
	operations int64
	// The sum of each latency measurement over all operations.
	// Calculated in ms.
	totalLatency int64
	// The sum of each latency Measurement squared over all operations.
	// Used to calculate variance of latency. Calculated in ms.
	totalSquaredLatency float64
	// Keep a windowed version of these stats for printing status
	windowOperations   int64
	windowTotalLatency int64
	min                int64
	max                int64
}

func NewOneMeasurementHistogram(name string, props Properties) (*OneMeasurementHistogram, error) {
	buckets, err := strconv.ParseInt(props.GetDefault(Buckets, BucketsDefault))
	if err != nil {
		return nil, err
	}
	return &OneMeasurementHistogram{
		OneMeasurementBase: NewOneMeasurementBase(name),
		buckets:            int64(buckets),
		histogram:          make(int64, buckets),
		histogramOverflow:  0,
		min:                -1,
		max:                -1,
	}
}

func (self *OneMeasurementHistogram) Measure(latency int64) {
	// latency reported in us and collected in buckets by ms.
	if (latency / 1000) >= self.buckets {
		self.histogramOverflow++
	} else {
		self.histogram[latency/1000]++
	}
	self.operations++
	self.totalLatency += latency
	self.totalSquaredLatency += math.Pow(float64(latency), 2.0)
	self.windowOperations++
	self.windowTotalLatency += latency

	if (self.min < 0) || (latency < self.min) {
		self.min = latency
	}
	if (self.max < 0) || (latency > self.max) {
		self.max = latency
	}
}

func (self *OneMeasurementHistogram) GetSummary() string {
	if self.windowOperations == 0 {
		return ""
	}
	report := float64(self.windowTotalLatency) / float64(self.windowOperations)
	self.windowOperations = 0
	self.windowTotalLatency = 0
	return fmt.Sprintf("[%s AverageLatency(us)=%.2d]", self.GetName(), report)
}

func (self *OneMeasurementHistogram) Export(exporter MeasurementExporter) {
	mean := self.totalLatency / float64(self.operations)
	variance := self.totalSquaredLatency/float64(self.operations) - math.Power(mean, 2.0)
	name := self.GetName()
	exporter.Write(name, "Operations", self.operations)
	exporter.Write(name, "AverageLatency(us)", mean)
	exporter.Write(name, "LatencyVariance(us)", variance)
	exporter.Write(name, "MinLatency(us)", self.min)
	exporter.Write(name, "MaxLatency(us)", self.max)
	opCounter := 0
	done95th := false
	for i := 0; i < self.buckets; i++ {
		opCounter += self.histogram[i]
		percentage := float64(opCounter) / float64(self.operations)
		if (!done95th) && (percentage >= 0.95) {
			exporter.Write(name, "95thPercentileLatency(us)", i*1000)
			done95th = true
		}
		if percentage >= 0.99 {
			exporter.Write(name, "99thPercentileLatency(us)", i*1000)
			break
		}
	}

	for i := 0; i < self.buckets; i++ {
		exporter.Write(name, fmt.Sprintf("%d", i), self.histogram[i])
	}
	exporter.Write(name, ">"+self.buckets, self.histogramOverflow)
}

type OneMeasurementHdrHistogram struct {
	*OneMeasurementBase
	histogram   *hdrhistogram.Histogram
	filePath    string
	file        *os.File
	percentiles *list.List
}

func NewOneMeasurementHistogram(name string) *OneMeasurementHdrHistogram {
	// TODO
}

// Delegates to 2 measurement instances.
type TwoInOneMeasurement struct {
	*OneMeasurementBase
	thing1 OneMeasurement
	thing2 OneMeasurement
}

func NewTwoInOneMeasurement(name string, thing1, thing2 OneMeasurement) *TwoInOneMeasurement {
	return &TwoInOneMeasurement{
		OneMeasurementBase: NewOneMeasurementBase(name),
		thing1:             thing1,
		thing2:             thing2,
	}
}

// It appears latency is reported in microseconds.
func (self *TwoInOneMeasurement) Measure(latency int) {
	self.thing1.Measure(latency)
	self.thing2.Measure(latency)
}

// This is called periodically from the status goroutine.
// There's a single status goroutine per client process.
// We optionally serialize the interval to log on this opportunity.
func (self *TwoInOneMeasurement) GetSummary() string {
	return self.thing1.GetSummary() + "\n" + self.thing2.GetSummary()
}

// This is called from a main goroutine, on orderly termination.
func (self *TwoInOneMeasurement) Export(exporter MeasurementExporter) {
	self.thing1.Export(exporter)
	self.thing2.Export(exporter)
}
