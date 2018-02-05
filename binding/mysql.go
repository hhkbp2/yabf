package binding

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/hhkbp2/yabf"
	"strconv"
	"strings"
)

const (
	PropertyMysqlHost              = "mysql.host"
	PropertyMysqlHostDefault       = "127.0.0.1"
	PropertyMysqlPort              = "mysql.port"
	PropertyMysqlPortDefault       = "3306"
	PropertyMysqlDatabase          = "mysql.db"
	PropertyMysqlDatabaseDefault   = "db"
	PropertyMysqlUser              = "mysql.user"
	PropertyMysqlUserDefault       = "user"
	PropertyMysqlPassword          = "mysql.password"
	PropertyMysqlPasswordDefault   = "password"
	PropertyMysqlOptions           = "mysql.options"
	PropertyMysqlOptionsDefault    = "charset=utf8"
	PropertyMysqlPrimaryKey        = "mysql.primarykey"
	PropertyMysqlPrimaryKeyDefault = "yabf_key"
)

type MysqlDB struct {
	*yabf.DBBase
	host       string
	port       int
	database   string
	primaryKey string
	user       string
	password   string
	options    string
	db         *sql.DB
}

func NewMysqlDB() *MysqlDB {
	return &MysqlDB{
		DBBase: yabf.NewDBBase(),
	}
}

func (self *MysqlDB) Init() error {
	props := self.GetProperties()
	host := props.GetDefault(PropertyMysqlHost, PropertyMysqlHostDefault)
	propStr := props.GetDefault(PropertyMysqlPort, PropertyMysqlPortDefault)
	port, err := strconv.ParseInt(propStr, 0, 32)
	if err != nil {
		return err
	}
	database := props.GetDefault(PropertyMysqlDatabase, PropertyMysqlDatabaseDefault)
	primaryKey := props.GetDefault(PropertyMysqlPrimaryKey, PropertyMysqlPrimaryKeyDefault)
	user := props.GetDefault(PropertyMysqlUser, PropertyMysqlUserDefault)
	password := props.GetDefault(PropertyMysqlPassword, PropertyMysqlPasswordDefault)
	options := props.GetDefault(PropertyMysqlOptions, PropertyMysqlOptionsDefault)
	self.host = host
	self.port = int(port)
	self.database = database
	self.primaryKey = primaryKey
	self.user = user
	self.password = password
	self.options = options
	sourceName := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", user, password, host, port, database, options)
	db, err := sql.Open("mysql", sourceName)
	if err != nil {
		return err
	}
	self.db = db
	return nil
}

func (self *MysqlDB) Cleanup() error {
	if self.db != nil {
		return self.db.Close()
	}
	return nil
}

func (self *MysqlDB) createReadStat(table string, fields []string, recordCount int64) string {
	var fieldStr string
	if len(fields) == 0 {
		fieldStr = "*"
	} else {
		fieldStr = strings.Join(fields, ", ")
	}
	if recordCount == 0 {
		return fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", fieldStr, table, self.primaryKey)
	} else {
		return fmt.Sprintf("SELECT %s FROM %s WHERE %s >= ? ORDER BY %s LIMIT ?", fieldStr, table, self.primaryKey, self.primaryKey)
	}
}

func (self *MysqlDB) Read(table string, key string, fields []string) (yabf.KVMap, yabf.StatusType) {
	statement := self.createReadStat(table, fields, 0)
	stmt, err := self.db.Prepare(statement)
	defer stmt.Close()
	if err != nil {
		return nil, yabf.StatusBadRequest
	}
	rows, err := stmt.Query(key)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, yabf.StatusNotFound
		}
		yabf.Errorf("fail to read table: %s, key: %s, error: %s", table, key, err)
		return nil, yabf.StatusError
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, yabf.StatusError
	}
	length := len(columns)
	if (len(fields) != 0) && (length != len(fields)) {
		return nil, yabf.StatusUnexpectedState
	}
	if !rows.Next() {
		if rows.Err() != nil {
			return nil, yabf.StatusError
		}
		return nil, yabf.StatusNotFound
	}
	results := make([][]byte, length)
	toScan := make([]interface{}, length)
	for i, _ := range results {
		toScan[i] = &results[i]
	}
	err = rows.Scan(toScan...)
	if err != nil {
		return nil, yabf.StatusError
	}
	ret := make(yabf.KVMap)
	for i := 0; i < length; i++ {
		ret[columns[i]] = results[i]
	}
	return ret, yabf.StatusOK
}

func (self *MysqlDB) Scan(table string, startKey string, recordCount int64, fields []string) ([]yabf.KVMap, yabf.StatusType) {
	statement := self.createReadStat(table, fields, recordCount)
	stmt, err := self.db.Prepare(statement)
	defer stmt.Close()
	if err != nil {
		return nil, yabf.StatusBadRequest
	}
	rows, err := stmt.Query(startKey, recordCount)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, yabf.StatusNotFound
		}
		yabf.Errorf("fail to scan table: %s, start key: %s, record count: %d, error: %s", table, startKey, recordCount, err)
		return nil, yabf.StatusError
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, yabf.StatusError
	}
	length := len(columns)
	if (len(fields) != 0) && (length != len(fields)) {
		return nil, yabf.StatusUnexpectedState
	}
	ret := make([]yabf.KVMap, 0, recordCount)
	for rows.Next() {
		results := make([][]byte, length)
		toScan := make([]interface{}, length)
		for i, _ := range results {
			toScan[i] = &results[i]
		}
		err = rows.Scan(toScan...)
		if err != nil {
			return nil, yabf.StatusError
		}
		m := make(yabf.KVMap)
		for i := 0; i < length; i++ {
			m[columns[i]] = results[i]
		}
	}
	return ret, yabf.StatusOK
}

func (self *MysqlDB) createUpdateStat(table string, values yabf.KVMap) (string, []interface{}) {
	var buf bytes.Buffer
	afterFirst := false
	args := make([]interface{}, 0, len(values))
	for k, v := range values {
		if afterFirst {
			buf.WriteString(", ")
		} else {
			afterFirst = true
		}
		buf.WriteString(k)
		buf.WriteString(" = ?")
		args = append(args, v)
	}
	setStr := buf.String()
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?", table, setStr, self.primaryKey), args
}

func (self *MysqlDB) Update(table string, key string, values yabf.KVMap) yabf.StatusType {
	statement, args := self.createUpdateStat(table, values)
	stmt, err := self.db.Prepare(statement)
	defer stmt.Close()
	if err != nil {
		return yabf.StatusBadRequest
	}
	_, err = stmt.Exec(args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return yabf.StatusNotFound
		}
		yabf.Errorf("fail to update table: %s, key: %s, error: %s", table, key, err)
		return yabf.StatusError
	}
	return yabf.StatusOK
}

func (self *MysqlDB) createInsertStat(table string, key string, values yabf.KVMap) (string, []interface{}) {
	var buf1, buf2 bytes.Buffer
	args := make([]interface{}, 0, len(values)+1)
	buf1.WriteString(self.primaryKey)
	buf2.WriteString("?")
	args = append(args, []byte(key))
	for k, v := range values {
		buf1.WriteString(", ")
		buf2.WriteString(", ")
		buf1.WriteString(k)
		buf2.WriteString("?")
		args = append(args, v)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", table, buf1.String(), buf2.String()), args
}

func (self *MysqlDB) Insert(table string, key string, values yabf.KVMap) yabf.StatusType {
	statement, args := self.createInsertStat(table, key, values)
	stmt, err := self.db.Prepare(statement)
	defer stmt.Close()
	if err != nil {
		return yabf.StatusBadRequest
	}
	_, err = stmt.Exec(args...)
	if err != nil {
		yabf.Errorf("fail to insert table: %s, key: %s, error: %s", table, key, err)
		return yabf.StatusError
	}
	return yabf.StatusOK
}

func (self *MysqlDB) Delete(table string, key string) yabf.StatusType {
	statement := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", table, self.primaryKey)
	stmt, err := self.db.Prepare(statement)
	defer stmt.Close()
	if err != nil {
		return yabf.StatusBadRequest
	}
	_, err = stmt.Exec(key)
	if err != nil {
		if err == sql.ErrNoRows {
			return yabf.StatusNotFound
		}
		yabf.Errorf("fail to delete table: %s, key: %s, error: %s", table, key, err)
		return yabf.StatusError
	}
	return yabf.StatusOK
}
