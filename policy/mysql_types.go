package policy

// import (
// 	"fmt"
// )

// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType

/*

 */
const (
	msFieldTypeDecimal   string = "decimal"
	msFieldTypeTiny      string = "tinyint"
	msFieldTypeShort     string = "smallint"
	msFieldTypeLong      string = "int"
	msFieldTypeFloat     string = "float"
	msFieldTypeDouble    string = "double"
	msFieldTypeNULL      string = "NULL"
	msFieldTypeTimestamp string = "timestamp"
	msFieldTypeLongLong  string = "bigint"
	msFieldTypeInt24     string = "mediumint"
	msFieldTypeDate      string = "date"
	msFieldTypeTime      string = "time"
	msFieldTypeDateTime  string = "datetime"
	msFieldTypeYear      string = "year"
	msFieldTypeVarBinary string = "varbinary"
	msFieldTypeBinary    string = "binary"
	msFieldTypeBit       string = "bit"
)

const (
	msFieldTypeJSON       string = "json"
	msFieldTypeEnum       string = "enum"
	msFieldTypeSet        string = "set"
	msFieldTypeTinyBLOB   string = "tinyblob"
	msFieldTypeMediumBLOB string = "mediumblob"
	msFieldTypeLongBLOB   string = "longblob"
	msFieldTypeBLOB       string = "blob"
	msFieldTypeTinyTEXT   string = "tinytext"
	msFieldTypeMediumTEXT string = "mediumtext"
	msFieldTypeLongTEXT   string = "longtext"
	msFieldTypeTEXT       string = "text"
	msFieldTypeVarString  string = "varchar"
	msFieldTypeString     string = "char"
	msFieldTypeGeometry   string = "geometry"
)
