package sheet

import (
	"fmt"
	"os"
	"strings"

	"github.com/pingcap/parser/model"
	log "github.com/sirupsen/logrus"
)

func (d *decoder) Dump() map[string]map[string][][]string {
	// stop the world!!!
	d.mutex.Lock()
	defer d.mutex.Unlock()
	ret := make(map[string]map[string][][]string)
	for _, db := range d.schema {
		ret[db.Name.O] = d.dumpDB(db)
	}
	return ret
}

func (d *decoder) dumpDB(db *model.DBInfo) map[string][][]string {
	ret := make(map[string][][]string)
	for _, tb := range db.Tables {
		func(db *model.DBInfo, tb *model.TableInfo) {
			fi, err := os.OpenFile(fmt.Sprintf("%s.%s.csv", db.Name.O, tb.Name.O), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
			if err != nil {
				log.Errorf("Failed to open file to write: %v", err)
				return
			}
			defer fi.Close()
			t := d.dumpTableString(tb)
			ret[tb.Name.O] = t
			dumpCSV(t, fi)
		}(db, tb)
	}
	return ret
}

func (d *decoder) dumpTableFile(tb *model.TableInfo, fp *os.File) {
	dumpCSV(d.dumpTableString(tb), fp)
}

func dumpCSV(d [][]string, fp *os.File) {
	for _, r := range d {
		stringdata := make([]string, len(r))
		for i, cell := range r {
			t := strings.Replace(cell, "\\", "\\\\", -1)
			t = strings.Replace(t, "\"", "\\\"", -1)
			stringdata[i] = t
		}
		fp.WriteString(strings.Join(stringdata, ",") + "\n")
	}
}

func (d *decoder) dumpTableString(tb *model.TableInfo) [][]string {
	ret := make([][]string, 0)

	findRow := make(map[int64]int)
	header := make([]string, len(tb.Columns))
	for _, col := range tb.Columns {
		findRow[col.ID] = col.Offset
		title := col.Name.O
		header[col.Offset] = title
	}
	ret = append(ret, header)
	for _, v := range d.row[tb.ID] {
		onerow := make([]string, len(tb.Columns))
		for id, val := range v {
			if id%2 == 1 {
				continue
			}
			rowid, ok := findRow[val.GetInt64()]
			if !ok {
				continue
			}
			rowdata := v[int64(id+1)]
			tmpval, err := rowdata.ToString()
			if err != nil {
				log.Errorf("Failed to stringify %v: %v", val, err)
			}
			onerow[rowid] = tmpval
		}
		ret = append(ret, onerow)
	}
	return ret
}
