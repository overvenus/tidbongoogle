package sheet

const (
	mDBPrefix      = "DB"
	mTablePrefix   = "Table"
	mTableIDPrefix = "TID"
)

type SchemaState byte

const (
	// StateNone means this schema element is absent and can't be used.
	StateNone SchemaState = iota
	// StateDeleteOnly means we can only delete items for this schema element.
	StateDeleteOnly
	// StateWriteOnly means we can use any write operation on this schema element,
	// but outer can't read the changed data.
	StateWriteOnly
	// StateWriteReorganization means we are re-organizing whole data after write only state.
	StateWriteReorganization
	// StateDeleteReorganization means we are re-organizing whole data after delete only state.
	StateDeleteReorganization
	// StatePublic means this schema element is ok for all write and read operations.
	StatePublic
)

type CIStr struct {
	O string `json:"O"` // Original string.
	L string `json:"L"` // Lower case string.
}

type DBInfo struct {
	ID      int64       `json:"id"`      // Database ID
	Name    CIStr       `json:"db_name"` // DB name.
	Charset string      `json:"charset"`
	Collate string      `json:"collate"`
	State   SchemaState `json:"state"`
}
