package yabf

const (
	// DBWrapper
	PropertyReportLatencyForEachError        = "reportlatencyforeacherror"
	PropertyReportLatencyForEachErrorDefault = "false"
	PropertyLatencyTrackedErrors             = "latencytrackederrors"

	// BasicDB
	ConfigBasicDBVerbose        = "basicdb.verbose"
	ConfigBasicDBVerboseDefault = "true"
	ConfigSimulateDelay         = "basicdb.simulatedelay"
	ConfigSimulateDelayDefault  = "0"
	ConfigRandomizeDelay        = "basicdb.randomizedelay"
	ConfigRandomizeDelayDefault = "true"

	// Client
	// The number of records to load into the database initially.
	PropertyRecordCount = "recordcount"
	// The default value of `PropertyRecordCount`
	PropertyRecordCountDefault = "0"
	// The target number of operations to perform.
	PropertyOperationCount        = "operationcount"
	PropertyOperationCountDefault = "0"
	// The workload class to be loaded.
	PropertyWorkload = "workload"
	// The database class to be used.
	PropertyDB        = "db"
	PropertyDBDefault = "basic"
	// The exporter class to be used. The default is TestMeasurementExporter.
	PropertyExporter        = "exporter"
	PropertyExporterDefault = "TextMeasurementExporter"
	// If set to the path of a file, this file will be written instead of stdout.
	PropertyExportFile = "exportfile"
	// The number of client goroutines to run.
	PropertyThreadCount        = "threadcount"
	PropertyThreadCountDefault = "1"
	// Indicates how many inserts to do, if less than `recordcount`.
	// Useful for partitioning the load among multiple servers, if the Client
	// is the bottleneck. Additionally, workloads should support the
	// "insertstart" property, which tells them which record to start at.
	PropertyInsertCount = "insertcount"
	// Target number of opertions per second
	PropertyTarget        = "target"
	PropertyTargetDefault = "0"
	// The maximum amount of time (in seconds) for which the benchmark will be run.
	PropertyMaxExecutionTime        = "maxexecutiontime"
	PropertyMaxExecutionTimeDefault = "0"
	// Whether or not this is the transaction phase (run) or not (load).
	PropertyTransactions          = "dotransactions"
	PropertyStatusInterval        = "status.interval"
	PropertyStatusIntervalDefault = "10"

	// workload
	PropertyInsertStart        = "insertstart"
	PropertyInsertStartDefault = "0"

	// The name of the database table to run queries against.
	PropertyTableName = "table"
	// The default value of `PropertyTableName`
	PropertyTableNameDefault = "usertable"
	// The name of property for the number of fields in a record
	PropertyFieldCount = "fieldcount"
	// The default value of `PropertyFieldCount`.
	PropertyFieldCountDefault = "10"
	// The name of the property for the field length distribution.
	// Options are "uniform", "zipfian"(favoring short records), "constant",
	// and "histogram".
	// If "uniform", "zipfian" or "constant", the maximum field length will
	// be that specified by the fieldlength property. If "histogram", then
	// the histogram will be read from the filename specified in the
	// "fieldlengthhistogram" property.
	PropertyFieldLengthDistribution = "fieldlengthdistribution"
	// The default value of `PropertyFieldLengthDistribution`
	PropertyFieldLengthDistributionDefault = "constant"
	// The name of the property for the length of a field in bytes.
	PropertyFieldLength = "fieldlength"
	// The default value of `PropertyFieldLength`
	PropertyFieldLengthDefault = "100"
	// The name of a property that specifies the filename containing the field
	// length histogram (only used if fieldlengthdistribution is "histogram").
	PropertyFieldLengthHistogramFile = "fieldlengthhistogram"
	// The default value of `PropertyFieldLengthHistogramFile`
	PropertyFieldLengthHistogramFileDefault = "hist.txt"
	// The name of the property for deciding whether to read one field (false)
	// or all fields (true) of a record.
	PropertyReadAllFields = "readallfields"
	// The default value of `PropertyReadAllFields`
	PropertyReadAllFieldsDefault = "true"
	// The name of the property for deciding whether to write one field (false)
	// or all fields (true) of a record.
	PropertyWriteAllFields = "writeallfields"
	// The default value of `PropertyWriteAllFields`
	PropertyWriteAllFieldsDefault = "false"
	// The name of the property for deciding whether to check all returned
	// data against the formation template to ensure data integrity.
	PropertyDataIntegrity = "dataintegrity"
	// The default value of `PropertyDataIntegrity`
	PropertyDataIntegrityDefault = "false"
	// The name of the property for the proportion of transactions
	// that are reads.
	PropertyReadProportion = "readproportion"
	// The default value of `PropertyReadProportion`
	PropertyReadProportionDefault = "0.95"
	// The name of the property for proportion of transactions
	// that are updates.
	PropertyUpdateProportion = "updateproportion"
	// The default value of `PropertyUpdateProportion`
	PropertyUpdateProportionDefault = "0.05"
	// The name of the property for proportion of transactions
	// that are inserts.
	PropertyInsertProportion = "insertproportion"
	// The default value of `PropertyInsertProportion`
	PropertyInsertProportionDefault = "0.0"
	// The name of the property for proportion of transactions
	// that are scans.
	PropertyScanProportion = "scanproportion"
	// The default value of `PropertyScanProportion`
	PropertyScanProportionDefault = "0.0"
	// The name of the property for porportion of transcations
	// that are read-modify-write.
	PropertyReadModifyWriteProportion = "readmodifywriteproportion"
	// The default value of `PropertyReadModifyWriteProportion`
	PropertyReadModifyWriteProportionDefault = "0.0"
	// The name of the property for the distribution of requests
	// across the keyspace. Options are "uniform", "zipfian" and "latest"
	PropertyRequestDistribution = "requestdistribution"
	// The default value of `PropertyRequestDistribution`
	PropertyRequestDistributionDefault = "uniform"
	// The name of the property for the max scan length (number of records)
	PropertyMaxScanLength = "maxscanlength"
	// The default max scan length
	PropertyMaxScanLengthDefault = "1000"
	// The name of the property for the scan length distribution.
	// Options are "uniform" and "zipfian" (favoring short scans)
	PropertyScanLengthDistribution = "scanlengthdistribution"
	// The default value of `PropertyScanLengthDistribution`
	PropertyScanLengthDistributionDefault = "uniform"
	// The name of the property for the order to insert records.
	// Options are "ordered" or "hashed"
	PropertyInsertOrder = "insertorder"
	// The default value of `PropertyInsertOrder`
	PropertyInsertOrderDefault = "hashed"
	// Percentage data items that constitute the hot set.
	HotspotDataFraction = "hotspotdatafraction"
	// The default value of `HotspotDataFraction`
	HotspotDataFractionDefault = "0.2"
	// Percentage opertions that access the hot set.
	HotspotOpnFraction = "hotspotopnfraction"
	// The default value of `HotspotOpnFraction`
	HotspotOpnFractionDefault = "0.8"
	// How many times to retry when insertion of a single item to a DB fails.
	InsertionRetryLimit = "core_workload_insertion_retry_limit"
	// The default value of `InsertionRetryLimit`
	InsertionRetryLimitDefault = "0"
	// On average, how long to wait between the retries, in seconds.
	InsertionRetryInterval = "core_workload_insertion_retry_interval"
	// The default value of `InsertionRetryInterval`
	InsertionRetryIntervalDefault = "3"

	PropertyStorageAge        = "storageages"
	PropertyStorageAgeDefault = "10"
	PropertyDiskSize          = "disksize"
	PropertyDiskSizeDefault   = "100000000"
	PropertyOccupancy         = "occupancy"
	PropertyOccupancyDefault  = "0.9"

	// measurement
	PropertyMeasurementType            = "measurementtype"
	PropertyMeasurementTypeDefault     = "hdrhistogram"
	PropertyMeasurementInterval        = "measurement.interval"
	PropertyMeasurementIntervalDefault = "op"

	// Granularity for time series; measurements will be averaged in chunks of
	// this granularity. Units are milliseconds.
	PropertyGranularity        = "timeseries.granularity"
	PropertyGranularityDefault = "1000"

	// Optionally, user can configure an output file to save the raw
	// data points. Default is none, raw results will be written to stdout.
	OutputFilePath        = "measurement.raw.output_file"
	OutputFilePathDefault = ""
	// Optionally, user can request to not output summary stats. This is
	// useful if the user chains the raw measurement type behind the
	// HdrHistogram type which already outputs summary stats. But even in
	// that case, the user may still want this class to compute summary stats
	// for them, especially if they want accurate computation of percentiles
	// (because percentils computed by histogram classes are still
	// approximations).
	NoSummaryStats        = "measurement.raw.no_summary"
	NoSummaryStatsDefault = "false"

	Buckets        = "histogram.buckets"
	BucketsDefault = "1000"

	// The name of the property for deciding what percentile values to output.
	PropertyPercentiles = "hdrhistogram.percentiles"
	// The default value of `PropertyPercentiles`
	PropertyPercentilesDefault = "95,99"
	// The name of the property that specifies the output filename of hdrhistogram
	PropertyHdrHistogramOutput = "hdrhistogram.fileoutput"
	// The default value of `PropertyHdrHistogramOutput`
	PropertyHdrHistogramOutputDefault = "false"
	PropertyHdrHistogramOutputPath    = "hdrhistogram.output.path"
	// The default value of `PropertyHdrHistogramOutputPath`
	PropertyHdrHistogramOutputPathDefault = ""

	// generator
	// What percentage of the readings should be within the most recent
	// exponential.fracportion of the dataset?
	PropertyExponentialPercentile        = "exponential.percentile"
	PropertyExponentialPercentileDefault = "95"
	// What fraction of the dataset should be accessed exponential.percentile
	// of the time?
	PropertyExponentialFraction        = "exponential.frac"
	PropertyExponentialFractionDefault = "0.8571428571" // 1/7
)
