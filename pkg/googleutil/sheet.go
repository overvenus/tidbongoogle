package googleutil

import (
	"io/ioutil"
	"log"

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
