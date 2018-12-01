package googleutil

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"

	log "github.com/sirupsen/logrus"
	"golang.org/x/oauth2/google"
	sheets "google.golang.org/api/sheets/v4"
)

// CreateSheetClient creats a sheet service.
func CreateSheetClient(cfg *Config) *sheets.Service {
	b, err := ioutil.ReadFile(cfg.Credentials)
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	// If modifying these scopes, delete your previously saved token.json.
	config, err := google.ConfigFromJSON(b,
		sheets.DriveScope, sheets.DriveFileScope, sheets.SpreadsheetsScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client := getClient(config, cfg.SheetTokenFile)

	srv, err := sheets.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve Sheets client: %v", err)
	}
	return srv
}

var srv *sheets.Service
var db2id = make(map[string]string)
var gclient *DriveClient

func Initsheet(config *Config) {
	srv = CreateSheetClient(config)
}

func InitGClient(client *DriveClient) {
	gclient = client
}

func getSheetID(name string) string {
	id, ok := db2id[name]
	if !ok || id == "" {
		did, err := gclient.FindOrCreateFile("", name, "application/vnd.google-apps.spreadsheet")
		if err != nil {
			return ""
		}
		db2id[name] = did
		return did
	}
	return id
}

func CreateSheet(db string, data map[string][][]string) error {
	sheetid := getSheetID(db)
	if sheetid == "" {
		return errors.New("Cannot get sheetid")
	}

	currentSheet, err := srv.Spreadsheets.Get(sheetid).Do()
	if err != nil {
		return err
	}

	currentTables := make(map[string]int64, 0)
	for _, v := range currentSheet.Sheets {
		currentTables[v.Properties.Title] = v.Properties.SheetId
	}

	newTables := make(map[string]int64, 0)
	for k, _ := range data {
		oldId, ok := currentTables[k]
		if !ok {
			newTables[k] = -1
		} else {
			newTables[k] = oldId
		}
	}

	req := &sheets.BatchUpdateSpreadsheetRequest{
		Requests: make([]*sheets.Request, 0),
	}

	for k, v := range data {
		newid := newTables[k]
		tsheets := toSheetData(k, v)
		if newid != -1 {
			req.Requests = append(req.Requests, &sheets.Request{
				DeleteRange: &sheets.DeleteRangeRequest{
					Range: &sheets.GridRange{
						SheetId: newid,
					},
					ShiftDimension: "ROWS",
				},
			})
		} else {
			newid = int64(rand.Int31())
			req.Requests = append(req.Requests, &sheets.Request{
				AddSheet: &sheets.AddSheetRequest{
					Properties: &sheets.SheetProperties{
						SheetId: newid,
						Title:   k,
					},
				},
			})
		}
		req.Requests = append(req.Requests, &sheets.Request{
			UpdateCells: &sheets.UpdateCellsRequest{
				Rows:   tsheets.Data[0].RowData,
				Fields: "*",
				Start: &sheets.GridCoordinate{
					SheetId:     newid,
					RowIndex:    0,
					ColumnIndex: 0,
				},
			},
		})
	}

	tcount := len(currentSheet.Sheets)

	for k, v := range currentTables {
		_, ok := newTables[k]
		if !ok && tcount > 1 {
			req.Requests = append(req.Requests, &sheets.Request{
				DeleteSheet: &sheets.DeleteSheetRequest{
					SheetId: v,
				},
			})
			tcount--
		}
	}

	if len(req.Requests) == 0 {
		log.Infof("Sheet: nothing to do with sheet %s", db)
		return nil
	}

	resp, err := srv.Spreadsheets.BatchUpdate(sheetid, req).Do()
	if err != nil {
		log.Errorf("Failed to do BatchUpdate request: %v", err)
	}
	jsondata, _ := json.Marshal(resp)
	log.Debugf("%s", string(jsondata))
	return nil
}

func toSheetData(name string, data [][]string) *sheets.Sheet {
	ret := &sheets.GridData{}
	ret.StartRow = 0
	ret.StartColumn = 0
	ret.RowData = make([]*sheets.RowData, 0)
	for _, col := range data {
		cell := make([]*sheets.CellData, 0)
		for _, cel := range col {
			cell = append(cell, &sheets.CellData{
				UserEnteredValue: &sheets.ExtendedValue{
					StringValue: cel,
				},
			})
		}
		ret.RowData = append(ret.RowData, &sheets.RowData{
			Values: cell,
		})
	}
	return &sheets.Sheet{
		Properties: &sheets.SheetProperties{
			Title: name,
		},
		Data: []*sheets.GridData{ret},
	}
}
