package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
)

// Retrieve a token, saves the token, then returns the generated client.
func getClient(config *oauth2.Config) *http.Client {
	// The file token.json stores the user's access and refresh tokens, and is
	// created automatically when the authorization flow completes for the first
	// time.
	tokFile := "var/driverwtoken.json"
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
		log.Fatalf("Unable to read authorization code %v", err)
	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web %v", err)
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

func main() {
	b, err := ioutil.ReadFile("var/credentials.json")
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	// If modifying these scopes, delete your previously saved token.json.
	config, err := google.ConfigFromJSON(b, drive.DriveScope,
		drive.DriveAppdataScope, drive.DriveFileScope, drive.DriveMetadataScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client := getClient(config)

	srv, err := drive.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}

	// List folders in the '10gAmy_B6qZvz9ZgUvYOguk5eQlzU0Gj1' and name contains 'region01'
	r, err := srv.Files.List().
		Q("mimeType='application/vnd.google-apps.folder' AND '10gAmy_B6qZvz9ZgUvYOguk5eQlzU0Gj1' in parents AND name contains 'region01'").
		Spaces("drive").
		Fields("nextPageToken, files(id, name, mimeType)").
		OrderBy("name desc").
		Do()
	if err != nil {
		log.Fatalf("Unable to retrieve files: %v", err)
	}
	fmt.Println("Files:")
	if len(r.Files) == 0 {
		fmt.Println("No files found.")
	} else {
		for _, i := range r.Files {
			fmt.Printf("%s (%s) [%s]\n", i.Name, i.Id, i.MimeType)
		}
	}
}

func createFolderAndFile(srv *drive.Service) {
	// Create a folder.
	f := drive.File{
		Name:     "region1",
		Parents:  []string{"10gAmy_B6qZvz9ZgUvYOguk5eQlzU0Gj1"},
		MimeType: "application/vnd.google-apps.folder",
	}
	folder, err := srv.Files.Create(&f).Do()
	if err != nil {
		log.Fatalf("Unable to create files: %v", err)
	}
	log.Printf("folder %#v", *folder)

	// Create a file.
	lg := drive.File{
		Name:     "1_1", // term 1, index 1
		Parents:  []string{folder.Id},
		MimeType: "application/octet-stream",
	}
	logF, err := srv.Files.Create(&lg).
		Media(strings.NewReader("this is a raft log")).Do()
	if err != nil {
		log.Fatalf("Unable to create files: %v", err)
	}
	log.Printf("file %#v", *logF)
}
