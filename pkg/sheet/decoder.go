package sheet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/enginepb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	log "github.com/sirupsen/logrus"
)

type Decoder = decoder

type decoder struct {
	ch        chan *raft_cmdpb.Request
	schema    map[int64]*model.DBInfo
	table     map[int64]*model.TableInfo
	row       map[int64]rowdata
	closechan chan struct{}
	mutex     sync.Mutex
}

type rowdata map[int64][]types.Datum

func NewDecoder() *decoder {
	return &decoder{
		ch:        make(chan *raft_cmdpb.Request, 128),
		schema:    make(map[int64]*model.DBInfo),
		table:     make(map[int64]*model.TableInfo),
		row:       make(map[int64]rowdata),
		mutex:     sync.Mutex{},
		closechan: make(chan struct{}),
	}
}

func decode(b []byte) ([]byte, error) {
	_, data, err := codec.DecodeBytes(b, nil)
	return data, err
}

func (d *decoder) decodeCmd(cmd *raft_cmdpb.Request) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	switch cmd.CmdType {
	case raft_cmdpb.CmdType_Put:
		//log.Debugf("Receive log type: %s", cmd.CmdType.String())
		if cmd.Put == nil {
			log.Errorf("Fail to decode, cmd.Put is nil")
			break
		}
		k, err := decode(cmd.Put.Key)
		if err != nil {
			log.Errorf("Failed to decode key: %v", err)
			break
		}
		v := cmd.Put.Value
		//log.Infof("%s / %x", string(k), k)

		if bytes.HasPrefix(k, []byte("mDB")) {
			key, _, err := decodeHashDataKey(k)
			if err != nil {
				log.Warnf("Failed to decode key: %v, key = %s, len(key) = %d", err, k, len(k))
				break
			}
			k = key
		} else if bytes.HasPrefix(k, []byte("t")) {
			// this is a table
		} else {
			break
		}
		//log.Infof("cf = %s, %s = %s", cmd.Put.Cf, k, v)
		strKey := string(k)
		if cmd.Put.Cf == "write" {
			// deal with short value
			return
		} else if cmd.Put.Cf != "" {
			return
		}
		if strKey == "DBs" {
			// metadata of a database
			log.Infof("Find DB metadata: %s", k)
			log.Debugf("Value = %s", string(v))
			dbmeta := &model.DBInfo{}
			if err := json.Unmarshal(v, dbmeta); err != nil {
				log.Errorf("Failed to decode value: %v", err)
			}
			log.Infof("DB name: %s", dbmeta.Name.O)
			d.schema[dbmeta.ID] = dbmeta
		} else if strings.HasPrefix(strKey, "DB:") {
			// metadata of a table
			keyList := strings.Split(strKey, ":")
			var dbId int64
			fmt.Sscanf(keyList[1], "%d", &dbId)
			tablemeta := &model.TableInfo{}
			if err := json.Unmarshal(v, tablemeta); err != nil {
				log.Errorf("Failed to decode value: %v", err)
			}
			log.Debugf("Value = %s", string(v))
			log.Infof("Table name: %s belongs to db #%d", tablemeta.Name.O, dbId)
			d.schema[dbId].Tables = append(d.schema[dbId].Tables, tablemeta)
			d.table[tablemeta.ID] = tablemeta   // table in db
			d.row[tablemeta.ID] = make(rowdata) // row in table
		} else if strings.HasPrefix(strKey, "t") {
			k, tableID, err := codec.DecodeInt(k[1:])
			if err != nil {
				log.Warnf("Failed to decode key: %v", err)
				break
			}
			if !bytes.HasPrefix(k, []byte("_r")) {
				break
			}
			k, rowID, err := codec.DecodeInt(k[1:])
			if err != nil {
				log.Warnf("Failed to decode key: %v", err)
				break
			}
			//kb, _ := json.Marshal(keyList)
			log.Infof("%x", k)
			log.Infof("T/R = %d / %d", tableID, rowID)
			log.Infof("%x", v)
			datas, err := codec.Decode(v, len(v))
			if err != nil {
				log.Warnf("Failed to decode key: %v", err)
				break
			}
			d.row[tableID][rowID] = datas
			log.Infof("%v", datas)
			// data
		}
	default:
		//log.Debugf("Ignore type: %s", cmd.CmdType.String())
	}
}

func (d *decoder) Do() {
	go func() {
		for {
			select {
			case <-d.closechan:
				break
			case cmd, ok := <-d.ch:
				if !ok {
					break
				}
				d.decodeCmd(cmd)
			}
		}
	}()
}

func (d *decoder) Finish() {
	close(d.closechan)
}

func (d *decoder) Decode(data []byte) {
	dec := &enginepb.CommandRequest{}
	proto.Unmarshal(data, dec)
	for _, it2 := range dec.Requests {
		d.ch <- it2
	}
}
