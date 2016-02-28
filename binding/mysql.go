package binding

import (
	"github.com/hhkbp2/yabf"
)

type MysqlDB struct {
	*yabf.DBBase
}

func NewMysqlDB() *MysqlDB {
	return &MysqlDB{
		DBBase: yabf.NewDBBase(),
	}
}

func (self *MysqlDB) Init() error {
}

func (self *MysqlDB) Cleanup() error {
}

func (self *MysqlDB) Read(table string, key string, fields []string) (yabf.KVMap, yabf.StatusType) {
}

func (self *MysqlDB) Scan(table string, startKey string, recordCount int64, fields []string) ([]yabf.KVMap, yabf.StatusType) {
}

func (self *MysqlDB) Update(table string, key string, values yabf.KVMap) yabf.StatusType {
}

func (self *MysqlDB) Insert(table string, key string, values yabf.KVMap) yabf.StatusType {
}

func (self *MysqlDB) Delete(table string, key string) yabf.StatusType {
}
