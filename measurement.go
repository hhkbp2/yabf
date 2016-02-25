package yabf

import (
	"bufio"
	"container/list"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/codahale/hdrhistogram"
	g "github.com/hhkbp2/yabf/generator"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
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

type StatusType uint8

const (
	StatusOK StatusType = 1 + iota
	StatusError
	StatusNotFound
	StatusNotImplemented
	StatusUnexpectedState
	StatusBadRequest
	StatusForbidden
	StatusServiceUnavailable
)

func (self StatusType) String() string {
	switch self {
	case StatusOK:
		return "OK"
	case StatusError:
		return "ERROR"
	case StatusNotFound:
		return "NOT_FOUND"
	case StatusNotImplemented:
		return "NOT_IMPLEMENTED"
	case StatusUnexpectedState:
		return "UNEXPECTED_STATE"
	case StatusBadRequest:
		return "BAD_REQUEST"
	case StatusForbidden:
		return "FORBIDDEN"
	case StatusServiceUnavailable:
		return "SERVICE_UNAVAILABLE"
	default:
		return "UNKNOW_STATUS"
	}
}

// Used to export the collected measuremrnts into a usefull format, for example
// human readable text or machine readable JSON.
type MeasurementExporter interface {
	// Write a measurement to the exported format. v should be int64 or float64
	Write(metric string, measurement string, v interface{}) error
	io.Closer
}

type MakeMeasurementExporterFunc func(w io.WriteCloser) MeasurementExporter

var (
	MeasurementExporters map[string]MakeMeasurementExporterFunc
)

func init() {
	MeasurementExporters = map[string]MakeMeasurementExporterFunc{
		"TextMeasurementExporter": func(w io.WriteCloser) MeasurementExporter {
			return NewTextMeasurementExporter(w)
		},
		"JSONMeasurementExporter": func(w io.WriteCloser) MeasurementExporter {
			return NewJSONMeasurementExporter(w)
		},
		"JSONArrayMeasurementExporter": func(w io.WriteCloser) MeasurementExporter {
			return NewJSONArrayMeasurementExporter(w)
		},
	}
}

func NewMeasurementExporter(className string, w io.WriteCloser) (MeasurementExporter, error) {
	f, ok := MeasurementExporters[className]
	if !ok {
		return nil, g.NewErrorf("unsupported measurement exporter: %s", className)
	}
	e := f(w)
	return e, nil
}

// A single measured metric (such as READ LATENCY)
type OneMeasurement interface {
	Measure(latency int64)
	GetName() string
	GetSummary() string
	// Report a return code.
	ReportStatus(status StatusType)
	// Exports the current measurements to a suitable format.
	ExportMeasurements(exporter MeasurementExporter) error
}

type OneMeasurementBase struct {
	Name        string
	ReturnCodes map[StatusType]uint32
}

func NewOneMeasurementBase(name string) *OneMeasurementBase {
	return &OneMeasurementBase{
		Name:        name,
		ReturnCodes: make(map[StatusType]uint32),
	}
}

func (self *OneMeasurementBase) GetName() string {
	return self.Name
}

func (self *OneMeasurementBase) ReportStatus(status StatusType) {
	count, _ := self.ReturnCodes[status]
	self.ReturnCodes[status] = count + 1
}

func (self *OneMeasurementBase) ExportStatusCounts(exporter MeasurementExporter) error {
	var err error
	for status, count := range self.ReturnCodes {
		err = exporter.Write(self.GetName(), fmt.Sprintf("Return=%s", status), count)
		if err != nil {
			return err
		}
	}
	return nil
}

// Collects latency measurements, and reports them when requested.
type Measurements interface {
	// in nano seconds
	SetIntendedStartTime(t int64)
	GetIntendedStartTime() int64

	// Report a single value of a single metric. E.g. for read latency,
	// operation="READ" and latency is the measured value.
	Measure(operation string, latency int64)
	// Report a single value of a single metric. E.g. for read latency,
	// operation="READ" and latency is the measured value.
	MeasureIntended(operation string, latency int64)

	// Return a one line summary of the measurements.
	GetSummary() string

	// Report a return code for a single DB operation.
	ReportStatus(operation string, status StatusType)

	// Export the current measurements to a suitable format.
	ExportMeasurements(exporter MeasurementExporter) error
}

type DefaultMeasurements struct {
	props                     Properties
	measurementType           MeasurementType
	measurementInterval       int
	opToMeasurementMap        map[string]OneMeasurement
	opToIntendedMesurementMap map[string]OneMeasurement
	intendedStartTime         int64
}

func NewDefaultMeasurement(props Properties) *DefaultMeasurements {
	opToMeasurementMap := make(map[string]OneMeasurement)
	opToIntendedMesurementMap := make(map[string]OneMeasurement)
	var measurementType MeasurementType
	propStr := props.GetDefault(PropertyMeasurementType, PropertyMeasurementTypeDefault)
	switch propStr {
	case "histogram":
		measurementType = MeasurementHistogram
	case "hdrhistogram":
		measurementType = MeasurementHDRHistogram
	case "hdrhistogram+histogram":
		measurementType = MeasurementHDRHistogramAndHistogram
	case "hdrhistogram+raw":
		measurementType = MeasurementHDRHistogramAndRaw
	case "timeseries":
		measurementType = MeasurementTimeSeries
	case "raw":
		measurementType = MeasurementRaw
	default:
		panic(fmt.Sprintf("unknown %s=%s", PropertyMeasurementType, propStr))
	}

	var measurementInterval int
	propStr = props.GetDefault(PropertyMeasurementInterval, PropertyMeasurementIntervalDefault)
	switch propStr {
	case "op":
		measurementInterval = 0
	case "intended":
		measurementInterval = 1
	case "both":
		measurementInterval = 2
	default:
		panic(fmt.Sprintf("unknown %s=%s", PropertyMeasurementInterval, propStr))
	}
	return &DefaultMeasurements{
		props:                     props,
		measurementType:           measurementType,
		measurementInterval:       measurementInterval,
		opToMeasurementMap:        opToMeasurementMap,
		opToIntendedMesurementMap: opToIntendedMesurementMap,
	}
}

func MustNewMeasurement(m OneMeasurement, err error) OneMeasurement {
	if err != nil {
		panic(fmt.Sprintf("unexpected error: %s", err))
	}
	return m
}

func (self *DefaultMeasurements) constructOneMeasurement(name string) OneMeasurement {
	switch self.measurementType {
	case MeasurementHistogram:
		return MustNewMeasurement(NewOneMeasurementHistogram(name, self.props))
	case MeasurementHDRHistogram:
		return MustNewMeasurement(NewOneMeasurementHdrHistogram(name, self.props))
	case MeasurementHDRHistogramAndHistogram:
		return NewTwoInOneMeasurement(name,
			MustNewMeasurement(NewOneMeasurementHdrHistogram("Hdr"+name, self.props)),
			MustNewMeasurement(NewOneMeasurementHistogram("Bucket"+name, self.props)))
	case MeasurementHDRHistogramAndRaw:
		return NewTwoInOneMeasurement(name,
			MustNewMeasurement(NewOneMeasurementHdrHistogram("Hdr"+name, self.props)),
			MustNewMeasurement(NewOneMeasurementRaw("Raw"+name, self.props)))
	case MeasurementTimeSeries:
		return MustNewMeasurement(NewOneMeasurementTimeSeries(name, self.props))
	default:
		panic("impossible to be here. Dead code reached. Bugs?")
	}
}

func (self *DefaultMeasurements) SetIntendedStartTime(t int64) {
	if self.measurementInterval == 0 {
		return
	}
	self.intendedStartTime = t
}

func (self *DefaultMeasurements) GetIntendedStartTime() int64 {
	if self.measurementInterval == 0 {
		return 0
	}
	if self.intendedStartTime == 0 {
		return time.Now().UnixNano()
	} else {
		return self.intendedStartTime
	}
}

// Report a single value of a single metric. E.g. for read latency,
// operation="READ" and latency is the measured value.
func (self *DefaultMeasurements) Measure(operation string, latency int64) {
	if self.measurementInterval == 0 {
		return
	}
	m := self.getOpMeasurement(operation)
	m.Measure(latency)
}

// Report a single value of a single metric. E.g. for read latency,
// operation="READ" and latency is the measured value.
func (self *DefaultMeasurements) MeasureIntended(operation string, latency int64) {
	if self.measurementInterval == 0 {
		return
	}
	m := self.getOpIntendedMeasurement(operation)
	m.Measure(latency)
}

func (self *DefaultMeasurements) GetSummary() string {
	var ret string
	for _, m := range self.opToMeasurementMap {
		ret += m.GetSummary()
	}
	for _, m := range self.opToIntendedMesurementMap {
		ret += m.GetSummary()
	}
	return ret
}

func (self *DefaultMeasurements) ReportStatus(operation string, status StatusType) {
	var m OneMeasurement
	if self.measurementInterval == 1 {
		m = self.getOpIntendedMeasurement(operation)
	} else {
		m = self.getOpMeasurement(operation)
	}
	m.ReportStatus(status)
}

func (self *DefaultMeasurements) ExportMeasurements(exporter MeasurementExporter) (err error) {
	defer catch(&err)
	for _, m := range self.opToMeasurementMap {
		try(m.ExportMeasurements(exporter))
	}
	for _, m := range self.opToIntendedMesurementMap {
		try(m.ExportMeasurements(exporter))
	}
	return
}

func (self *DefaultMeasurements) getOpMeasurement(operation string) OneMeasurement {
	m, ok := self.opToMeasurementMap[operation]
	if !ok {
		m = self.constructOneMeasurement(operation)
		self.opToMeasurementMap[operation] = m
	}
	return m
}

func (self *DefaultMeasurements) getOpIntendedMeasurement(operation string) OneMeasurement {
	m, ok := self.opToIntendedMesurementMap[operation]
	if !ok {
		m = self.constructOneMeasurement(operation)
		self.opToIntendedMesurementMap[operation] = m
	}
	return m
}

var (
	measurementProperties Properties = NewProperties()
	singleton             Measurements
)

func SetMeasurementProperties(props Properties) {
	measurementProperties = props
}

func GetMeasurementProperties() Properties {
	return measurementProperties
}

func GetMeasurements() Measurements {
	if singleton == nil {
		singleton = NewDefaultMeasurement(measurementProperties)
	}
	return singleton
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

// Export measurements into a machine readable JSON file.
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

// One raw point, has two fields:
// timestamp(ms) when the datapoint is inserted, and the value.
type RawDataPoint struct {
	timestamp time.Time
	value     int64
}

func NewRawDataPoint(value int64) *RawDataPoint {
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

// Record a series of measurements as raw data points without down sampling,
// optionally write to an output file when configured.
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
		output = os.Stdout
	} else {
		f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		output = f
	}
	noSummaryStats, err := strconv.ParseBool(props.GetDefault(NoSummaryStats, NoSummaryStatsDefault))
	if err != nil {
		output.Close()
		return nil, err
	}
	object := &OneMeasurementRaw{
		OneMeasurementBase: NewOneMeasurementBase(name),
		filePath:           filePath,
		file:               output,
		noSummaryStats:     noSummaryStats,
		measurements:       list.New(),
	}
	return object, nil
}

func (self *OneMeasurementRaw) Measure(latency int64) {
	self.totalLatency += latency
	self.windowTotalLatency += latency
	self.windowOperations++
	self.measurements.PushBack(NewRawDataPoint(latency))
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

func try(err error) {
	if err != nil {
		panic(fmt.Errorf("Error: %s", err.Error()))
	}
}

func tryn(n int, err error) {
	try(err)
}

func catch(err *error) {
	if p := recover(); p != nil {
		*err = p.(error)
	}
}

func (self *OneMeasurementRaw) ExportMeasurements(exporter MeasurementExporter) (err error) {
	defer catch(&err)
	// Output raw data points first then print out a summary of percentiles.
	tryn(self.file.WriteString(fmt.Sprintf(
		"%s latency raw data: op, timestamp(ms), latency(us)\n",
		self.GetName())))
	for e := self.measurements.Front(); e != nil; e = e.Next() {
		p := e.Value.(*RawDataPoint)
		tryn(self.file.WriteString(fmt.Sprintf("%s,%d,%d",
			self.GetName(), p.timestamp, p.value)))
	}
	if len(self.filePath) != 0 {
		self.file.Close()
	}
	total := self.measurements.Len()
	try(exporter.Write(self.GetName(), "Total Operations", total))
	if total > 0 && !self.noSummaryStats {
		s := make(RawDataPointSlice, 0, total)
		i := 0
		for e := self.measurements.Front(); e != nil && i < total; e = e.Next() {
			p := e.Value.(*RawDataPoint)
			s[i] = p
			i++
		}
		try(exporter.Write(self.GetName(),
			"Below is a summary of latency in miscroseconds:", -1))
		try(exporter.Write(self.GetName(),
			"Average", float64(self.totalLatency)/float64(total)))
		sort.Sort(s)
		name := self.GetName()
		try(exporter.Write(name, "Min", s[0].value))
		try(exporter.Write(name, "Max", s[total-1].value))
		try(exporter.Write(name, "p1", s[int(float64(total)*0.01)].value))
		try(exporter.Write(name, "p5", s[int(float64(total)*0.05)].value))
		try(exporter.Write(name, "p50", s[int(float64(total)*0.5)].value))
		try(exporter.Write(name, "p90", s[int(float64(total)*0.9)].value))
		try(exporter.Write(name, "p95", s[int(float64(total)*0.95)].value))
		try(exporter.Write(name, "p99", s[int(float64(total)*0.99)].value))
		try(exporter.Write(name, "p99.9", s[int(float64(total)*0.999)].value))
		try(exporter.Write(name, "p99.99", s[int(float64(total)*0.9999)].value))
	}
	try(self.ExportStatusCounts(exporter))
	return
}

type SeriesUnit struct {
	Time    int64
	Average float64
}

func NewSeriesUnit(t int64, average float64) *SeriesUnit {
	return &SeriesUnit{
		Time:    t,
		Average: average,
	}
}

// A time series measurement of a metric, such as READ LATENCY.
type OneMeasurementTimeSeries struct {
	*OneMeasurementBase
	granularity          int64
	measurements         []*SeriesUnit
	start                int64
	currentUnit          int64
	count                int
	sum                  int64
	operations           int
	totalLatency         int64
	windowOperations     int
	windowToTotalLatency int64
	min                  int64
	max                  int64
}

func NewOneMeasurementTimeSeries(name string, props Properties) (*OneMeasurementTimeSeries, error) {
	propStr := props.GetDefault(PropertyGranularity, PropertyGranularityDefault)
	granularity, err := strconv.ParseInt(propStr, 0, 64)
	if err != nil {
		return nil, err
	}
	object := &OneMeasurementTimeSeries{
		OneMeasurementBase: NewOneMeasurementBase(name),
		granularity:        int64(granularity),
		measurements:       make([]*SeriesUnit, 0),
		start:              -1,
		currentUnit:        -1,
		min:                -1,
		max:                -1,
	}
	return object, nil
}

func NanoToMillis(nano int64) int64 {
	return nano / 1000000
}

func (self *OneMeasurementTimeSeries) CheckEndOfUnit(forceEnd bool) {
	now := NanoToMillis(time.Now().UnixNano())
	if self.start < 0 {
		self.currentUnit = 0
		self.start = now
	}

	unit := (now - self.start) / self.granularity * self.granularity
	if (unit > self.currentUnit) || forceEnd {
		average := float64(self.sum) / float64(self.count)
		self.measurements = append(self.measurements, NewSeriesUnit(self.currentUnit, average))
		self.currentUnit = unit
		self.count = 0
		self.sum = 0
	}
}

func (self *OneMeasurementTimeSeries) Measure(latency int64) {
	self.CheckEndOfUnit(false)
	self.count++
	self.sum += latency
	self.totalLatency += latency
	self.operations++
	self.windowOperations++
	self.windowToTotalLatency += latency

	if latency > self.max {
		self.max = latency
	}
	if (latency < self.min) || (self.min < 0) {
		self.min = latency
	}
}

func (self *OneMeasurementTimeSeries) GetSummary() string {
	if self.windowOperations == 0 {
		return ""
	}
	report := float64(self.windowToTotalLatency) / float64(self.windowOperations)
	self.windowToTotalLatency = 0
	self.windowOperations = 0
	return fmt.Sprintf("[%s AverageLatency(us)=%.2g]", self.GetName(), report)
}

func (self *OneMeasurementTimeSeries) ExportMeasurements(exporter MeasurementExporter) (err error) {
	defer catch(&err)
	self.CheckEndOfUnit(true)
	name := self.GetName()
	try(exporter.Write(name, "Operations", self.operations))
	try(exporter.Write(name, "AverageLatency(us)", float64(self.totalLatency)/float64(self.operations)))
	try(exporter.Write(name, "MinLatency(us)", self.min))
	try(exporter.Write(name, "MaxLatency(us)", self.max))

	try(self.ExportStatusCounts(exporter))
	for _, unit := range self.measurements {
		try(exporter.Write(name, fmt.Sprintf("%d", unit.Time), unit.Average))
	}
	return
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
	buckets, err := strconv.ParseInt(props.GetDefault(Buckets, BucketsDefault), 0, 64)
	if err != nil {
		return nil, err
	}
	object := &OneMeasurementHistogram{
		OneMeasurementBase: NewOneMeasurementBase(name),
		buckets:            int64(buckets),
		histogram:          make([]int64, buckets),
		histogramOverflow:  0,
		min:                -1,
		max:                -1,
	}
	return object, nil
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

func (self *OneMeasurementHistogram) ExportMeasurements(exporter MeasurementExporter) (err error) {
	defer catch(&err)
	mean := float64(self.totalLatency) / float64(self.operations)
	variance := self.totalSquaredLatency/float64(self.operations) - math.Pow(mean, 2.0)
	name := self.GetName()
	try(exporter.Write(name, "Operations", self.operations))
	try(exporter.Write(name, "AverageLatency(us)", mean))
	try(exporter.Write(name, "LatencyVariance(us)", variance))
	try(exporter.Write(name, "MinLatency(us)", self.min))
	try(exporter.Write(name, "MaxLatency(us)", self.max))
	opCounter := int64(0)
	done95th := false
	for i := int64(0); i < self.buckets; i++ {
		opCounter += self.histogram[i]
		percentage := float64(opCounter) / float64(self.operations)
		if (!done95th) && (percentage >= 0.95) {
			exporter.Write(name, "95thPercentileLatency(us)", i*1000)
			done95th = true
		}
		if percentage >= 0.99 {
			try(exporter.Write(name, "99thPercentileLatency(us)", i*1000))
			break
		}
	}

	try(self.ExportStatusCounts(exporter))

	for i := int64(0); i < self.buckets; i++ {
		try(exporter.Write(name, fmt.Sprintf("%d", i), self.histogram[i]))
	}
	try(exporter.Write(name, fmt.Sprintf(">%d", self.buckets), self.histogramOverflow))
	return
}

type HdrHistogramLogReader struct {
	r io.Reader
}

func NewHdrHistogramLogReader(r io.Reader) *HdrHistogramLogReader {
	return &HdrHistogramLogReader{
		r: r,
	}
}

func (self *HdrHistogramLogReader) NextHistogram() (*hdrhistogram.Histogram, error) {
	var snapshot *hdrhistogram.Snapshot
	err := binary.Read(self.r, binary.LittleEndian, snapshot)
	if err != nil {
		return nil, err
	}
	h := hdrhistogram.Import(snapshot)
	return h, nil
}

type HdrHistogramLogWriter struct {
	w io.Writer
}

func NewHdrHistogramLogWriter(w io.Writer) *HdrHistogramLogWriter {
	return &HdrHistogramLogWriter{
		w: w,
	}
}

func (self *HdrHistogramLogWriter) OutputHistogram(h *hdrhistogram.Histogram) error {
	return binary.Write(self.w, binary.LittleEndian, h.Export())
}

type OneMeasurementHdrHistogram struct {
	*OneMeasurementBase
	histogram   *hdrhistogram.Histogram
	filePath    string
	file        *os.File
	writer      *HdrHistogramLogWriter
	percentiles []int64
}

// Helper function to parse the given percentile value string.
func parsePercentileValues(prop, defaultValue string) []int64 {
	parts := strings.Split(prop, ",")
	ret := make([]int64, 0, len(parts))
	for _, p := range parts {
		i, err := strconv.ParseInt(p, 0, 64)
		if err != nil {
			return parsePercentileValues(defaultValue, defaultValue)
		}
		ret = append(ret, int64(i))
	}
	return ret
}

func NewOneMeasurementHdrHistogram(name string, props Properties) (*OneMeasurementHdrHistogram, error) {
	prop := props.GetDefault(PropertyPercentiles, PropertyPercentilesDefault)
	percentiles := parsePercentileValues(prop, PropertyPercentilesDefault)
	prop = props.GetDefault(PropertyHdrHistogramOutput, PropertyHdrHistogramOutputDefault)
	shouldLog, err := strconv.ParseBool(prop)
	if err != nil {
		return nil, err
	}
	var filePath string
	var f *os.File
	var writer *HdrHistogramLogWriter
	if !shouldLog {
		filePath = ""
		f = nil
		writer = nil
	} else {
		filePath = props.GetDefault(PropertyHdrHistogramOutputPath, PropertyHdrHistogramOutputPathDefault)
		f, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		writer = NewHdrHistogramLogWriter(f)
	}
	object := &OneMeasurementHdrHistogram{
		OneMeasurementBase: NewOneMeasurementBase(name),
		histogram:          hdrhistogram.New(0, math.MaxInt64, 10),
		filePath:           filePath,
		file:               f,
		writer:             writer,
		percentiles:        percentiles,
	}
	return object, nil
}

// It appears latency is reported in micros.
func (self *OneMeasurementHdrHistogram) Measure(latency int64) {
	self.histogram.RecordValue(latency)
}

// This is called periodically from the status goroutine. There's a single
// status goroutine per client process. We optionally serialize the interval to
// log on this oppertunity.
func (self *OneMeasurementHdrHistogram) GetSummary() string {
	if self.writer != nil {
		self.writer.OutputHistogram(self.histogram)
	}
	format := "[%s: Count=%d, Max=%d, Min=%d, Avg=%d, 90=%d, 99=%d, 99.9=%g, 99.99=%g]"
	return fmt.Sprintf(format,
		self.GetName(),
		self.histogram.TotalCount(),
		self.histogram.Max(),
		self.histogram.Min(),
		self.histogram.Mean(),
		self.histogram.ValueAtQuantile(90),
		self.histogram.ValueAtQuantile(99),
		self.histogram.ValueAtQuantile(99.9),
		self.histogram.ValueAtQuantile(99.99))
}

var (
	Suffixes = []string{"th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th"}
)

func ordinal(p int64) string {
	switch p % 100 {
	case 11, 12, 13:
		return fmt.Sprintf("%dth", p)
	default:
		return fmt.Sprintf("%d%s", p, Suffixes[p%10])
	}
}

// This is called from a main thread, on orderly termination.
func (self *OneMeasurementHdrHistogram) ExportMeasurements(exporter MeasurementExporter) (err error) {
	defer catch(&err)

	if self.writer != nil {
		self.writer.OutputHistogram(self.histogram)
		self.file.Close()
	}
	name := self.GetName()
	try(exporter.Write(name, "Operations", self.histogram.TotalCount()))
	try(exporter.Write(name, "AverageLatency(us)", self.histogram.Mean()))
	try(exporter.Write(name, "MinLatency(us)", self.histogram.Min()))
	try(exporter.Write(name, "MaxLatency(us)", self.histogram.Max()))

	for _, p := range self.percentiles {
		try(exporter.Write(name, ordinal(p)+"PercentileLatency(us)", self.histogram.ValueAtQuantile(float64(p))))
	}
	try(self.ExportStatusCounts(exporter))
	return
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
func (self *TwoInOneMeasurement) Measure(latency int64) {
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
func (self *TwoInOneMeasurement) ExportMeasurements(exporter MeasurementExporter) (err error) {
	defer catch(&err)

	try(self.thing1.ExportMeasurements(exporter))
	try(self.thing2.ExportMeasurements(exporter))
	return
}
