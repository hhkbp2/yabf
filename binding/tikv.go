package binding

import (
	"github.com/hhkbp2/yabf"
	"github.com/pingcap/tidb/store/tikv"
	"strings"
)

const (
	PropertyTikvPDAddress        = "tikv.pdaddress"
	PropertyTikvPDAddressDefault = "127.0.0.1:2379"
)

type TikvDB struct {
	*yabf.DBBase
	pdAddress string
	client    *tikv.RawKVClient
}

func NewTikvDB() *TikvDB {
	return &TikvDB{
		DBBase: yabf.NewDBBase(),
	}
}

func (self *TikvDB) Init() error {
	props := self.GetProperties()
	pdAddress := props.GetDefault(PropertyTikvPDAddress, PropertyTikvPDAddressDefault)
	client, err := tikv.NewRawKVClient(strings.Split(pdAddress, ","))
	if err != nil {
		return err
	}
	self.pdAddress = pdAddress
	self.client = client
	return nil
}

func (self *TikvDB) Cleanup() error {
	// Nothing to do here
	return nil
}

func (self *TikvDB) Read(table string, key string, fields []string) (yabf.KVMap, yabf.StatusType) {
	if len(fields) != 1 {
		return nil, yabf.StatusBadRequest
	}

	v, err := self.client.Get([]byte(key))
	if err != nil {
		return nil, yabf.StatusError
	}
	ret := make(yabf.KVMap)
	ret[fields[0]] = v
	return ret, yabf.StatusOK
}

func (self *TikvDB) Scan(table string, startKey string, recordCount int64, fields []string) ([]yabf.KVMap, yabf.StatusType) {
	// NOTICE not support right row
	return nil, yabf.StatusBadRequest
}

func (self *TikvDB) Update(table string, key string, values yabf.KVMap) yabf.StatusType {
	return self.Insert(table, key, values)
}

func (self *TikvDB) Insert(table string, key string, values yabf.KVMap) yabf.StatusType {
	if len(values) != 1 {
		return yabf.StatusBadRequest
	}
	var value []byte
	for _, v := range values {
		value = v
	}
	if err := self.client.Put([]byte(key), value); err != nil {
		return yabf.StatusError
	}
	return yabf.StatusOK
}

func (self *TikvDB) Delete(table string, key string) yabf.StatusType {
	if err := self.client.Delete([]byte(key)); err != nil {
		return yabf.StatusError
	}
	return yabf.StatusOK
}
