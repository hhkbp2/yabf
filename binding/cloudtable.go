package binding

import (
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/hhkbp2/yabf"
	"github.com/hhkbp2/yabf/binding/cloudtable-gen/cloudtable"
	"net"
)

const (
	PropertyCloudtableHost                        = "cloudtable.host"
	PropertyCloudtableHostDefault                 = "localhost"
	PropertyCloudtablePort                        = "cloudtable.port"
	PropertyCloudtablePortDefault                 = "2000"
	PropertyCloudtableColumnFamily                = "cloudtable.columnfamily"
	PropertyCloudtableAuthenticateUser            = "cloudtable.authuser"
	PropertyCloudtableAuthenticateUserDefault     = "authuser"
	PropertyCloudtableAuthenticatePassword        = "cloudtable.authpassword"
	PropertyCloudtableAuthenticatePasswordDefault = "authpassword"
)

type CloudTableDB struct {
	*yabf.DBBase
	host         string
	port         string
	columnFamily string
	authUser     string
	authPassword string
	transport    thrift.TTransport
	client       *cloudtable.TCloudTableServiceClient
}

func NewCloudTableDB() *CloudTableDB {
	return &CloudTableDB{
		DBBase: yabf.NewDBBase(),
	}
}

func (self *CloudTableDB) Init() error {
	props := self.GetProperties()
	host := props.GetDefault(PropertyCloudtableHost, PropertyCloudtableHostDefault)
	port := props.GetDefault(PropertyCloudtablePort, PropertyCloudtablePortDefault)
	authUser := props.GetDefault(PropertyCloudtableAuthenticateUser, PropertyCloudtableAuthenticateUserDefault)
	authPassword := props.GetDefault(PropertyCloudtableAuthenticatePassword, PropertyCloudtableAuthenticatePasswordDefault)
	columnFamily, ok := props[PropertyCloudtableColumnFamily]
	if !ok {
		return errors.New("no columnfamily specified")
	}
	self.host = host
	self.port = port
	self.columnFamily = columnFamily
	self.authUser = authUser
	self.authPassword = authPassword
	transport, err := thrift.NewTSocket(net.JoinHostPort(host, port))
	if err != nil {
		return err
	}
	if err = transport.Open(); err != nil {
		return err
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	client := cloudtable.NewTCloudTableServiceClientFactory(transport, protocolFactory)
	_, err = client.Authenticate(authUser, authPassword)
	if err != nil {
		return err
	}
	self.transport = transport
	self.client = client
	return nil
}

func (self *CloudTableDB) Cleanup() error {
	if self.transport != nil {
		self.transport.Close()
		self.transport = nil
	}
	return nil
}

func fieldsToColumns(family string, fields []string) []*cloudtable.TColumn {
	ret := make([]*cloudtable.TColumn, 0, len(fields))
	for _, f := range fields {
		c := cloudtable.NewTColumn()
		c.Family = family
		c.Qualifier = []byte(f)
		ret = append(ret, c)
	}
	return ret
}

func resultToKVMap(result *cloudtable.TResult_) yabf.KVMap {
	ret := make(yabf.KVMap)
	for _, cv := range result.ColumnValues {
		ret[string(cv.Qualifier)] = cv.Value
	}
	return ret
}

func (self *CloudTableDB) Read(table string, key string, fields []string) (yabf.KVMap, yabf.StatusType) {
	get := cloudtable.NewTGet()
	get.Row = []byte(key)
	get.Columns = fieldsToColumns(self.columnFamily, fields)
	result, err := self.client.Get(table, get)
	if err != nil {
		return nil, yabf.StatusError
	}
	return resultToKVMap(result), yabf.StatusOK
}

func (self *CloudTableDB) Scan(table string, startKey string, recordCount int64, fields []string) ([]yabf.KVMap, yabf.StatusType) {
	scan := cloudtable.NewTScan()
	scan.StartRow = []byte(startKey)
	scan.Columns = fieldsToColumns(self.columnFamily, fields)
	results, err := self.client.GetScannerResults(table, scan, int32(recordCount))
	if err != nil {
		return nil, yabf.StatusError
	}
	ret := make([]yabf.KVMap, 0, len(results))
	for _, r := range results {
		ret = append(ret, resultToKVMap(r))
	}
	return ret, yabf.StatusOK
}

func (self *CloudTableDB) Update(table string, key string, values yabf.KVMap) yabf.StatusType {
	put := cloudtable.NewTPut()
	put.Row = []byte(key)
	cvs := make([]*cloudtable.TColumnValue, 0, len(values))
	for k, v := range values {
		cv := cloudtable.NewTColumnValue()
		cv.Family = self.columnFamily
		cv.Qualifier = []byte(k)
		cv.Value = v
		cvs = append(cvs, cv)
	}
	put.ColumnValues = cvs
	err := self.client.Put(table, put)
	if err != nil {
		yabf.EPrintln("DEBUG error on insert: %s", err)
		return yabf.StatusError
	}
	return yabf.StatusOK
}

func (self *CloudTableDB) Insert(table string, key string, values yabf.KVMap) yabf.StatusType {
	return self.Update(table, key, values)
}

func (self *CloudTableDB) Delete(table string, key string) yabf.StatusType {
	delete := cloudtable.NewTDelete()
	delete.Row = []byte(key)
	err := self.client.DeleteSingle(table, delete)
	if err != nil {
		return yabf.StatusError
	}
	return yabf.StatusOK
}
