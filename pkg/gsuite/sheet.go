package gsuite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"

	"github.com/overvenus/tidbongoogle/pkg/googleutil"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	sheets "google.golang.org/api/sheets/v4"
)

// Retrieve a token, saves the token, then returns the generated client.
func getClient(config *oauth2.Config) *http.Client {
	// The file token.json stores the user's access and refresh tokens, and is
	// created automatically when the authorization flow completes for the first
	// time.
	tokFile := "var/token.json"
	tok, err := tokenFromFile(tokFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(tokFile, tok)
	}
	return config.Client(context.Background(), tok)
}

// Request a token from the web, then returns the retrieved token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatalf("Unable to read authorization code: %v", err)
	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web: %v", err)
	}
	return tok
}

// Retrieves a token from a local file.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

// Saves a token to a file path.
func saveToken(path string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}

var Srv *sheets.Service
var db2id map[string]string
var gclient *googleutil.DriveClient

func init() {
	b, err := ioutil.ReadFile("var/credentials.json")
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	// If modifying these scopes, delete your previously saved token.json.
	config, err := google.ConfigFromJSON(b,
		sheets.DriveScope, sheets.DriveFileScope, sheets.SpreadsheetsScope)

	// config, err := google.ConfigFromJSON(b, "https://www.googleapis.com/auth/spreadsheets.readonly")
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client := getClient(config)

	Srv, err = sheets.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve Sheets client: %v", err)
	}
}

func InitGClient(client *googleutil.DriveClient) {
	gclient = client
}

func GetSheetID(name string) string {
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

func Create(db string, data map[string][][]string) error {
	sheetid := GetSheetID(db)
	if sheetid == "" {
		return errors.New("Cannot get sheetid")
	}

	currentSheet, err := Srv.Spreadsheets.Get(sheetid).Do()
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
				},
			})
		} else {
			req.Requests = append(req.Requests, &sheets.Request{
				AddSheet: &sheets.AddSheetRequest{
					Properties: &sheets.SheetProperties{
						SheetId: rand.Int63(),
						Title:   k,
					},
				},
			})
		}
		req.Requests = append(req.Requests, &sheets.Request{
			UpdateCells: &sheets.UpdateCellsRequest{
				Rows: tsheets.Data[0].RowData,
				Start: &sheets.GridCoordinate{
					SheetId:     newid,
					RowIndex:    0,
					ColumnIndex: 0,
				},
				Range: &sheets.GridRange{
					SheetId: newid,
				},
			},
		})
	}

	for k, v := range currentTables {
		_, ok := newTables[k]
		if !ok {
			req.Requests = append(req.Requests, &sheets.Request{
				DeleteSheet: &sheets.DeleteSheetRequest{
					SheetId: v,
				},
			})
		}
	}
	Srv.Spreadsheets.BatchUpdate(sheetid, req).Do()
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
