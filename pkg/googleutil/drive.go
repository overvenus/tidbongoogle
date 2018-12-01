package googleutil

import (
	"fmt"
	"io"
	"io/ioutil"

	log "github.com/sirupsen/logrus"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
)

// DriveClient is a wraper of dirve service.
type DriveClient struct {
	Drive *drive.Service
	Root  string
}

// NewDriveClient creats a drive client.
func NewDriveClient(cfg *Config, root string) *DriveClient {
	b, err := ioutil.ReadFile(cfg.Credentials)
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	// If modifying these scopes, delete your previously saved token.json.
	config, err := google.ConfigFromJSON(b, drive.DriveScope,
		drive.DriveAppdataScope, drive.DriveFileScope, drive.DriveMetadataScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client := getClient(config, cfg.DriveTokenFile)

	srv, err := drive.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}
	return &DriveClient{
		Drive: srv,
		Root:  root,
	}
}

// CreateFolder creates a folder named `name` in the folder `parent`.
func (cli *DriveClient) CreateFolder(
	name, parent string,
) (*drive.File, error) {
	// Use root as default, if parent id is empty.
	if parent == "" {
		parent = cli.Root
	}
	f := drive.File{
		Name:     name,
		Parents:  []string{parent},
		MimeType: "application/vnd.google-apps.folder",
	}
	folder, err := cli.Drive.Files.Create(&f).Do()
	if err != nil {
		return nil, err
	}
	return folder, nil
}

// MaybeCreateFolder tries its best to avoid to create a redundant folder.
func (cli *DriveClient) MaybeCreateFolder(
	name, parent string,
) (*drive.File, bool, error) {
	if parent == "" {
		parent = cli.Root
	}
	fl, err := cli.ListFolderByName(name, parent, 1)
	if err != nil {
		log.Errorf("fail to list folder %s", name)
		return nil, false, err
	}
	if len(fl.Files) == 0 {
		// No folder found.
		// Create a new folder.
		fd, err := cli.CreateFolder(name, parent)
		if err != nil {
			log.Errorf("fail to create folder %s", name)
			return nil, false, err
		}
		return fd, false, nil
	}
	log.Infof("found a folder named %s, skip create", name)
	return fl.Files[0], true, nil
}

// CreateFile creates a file named `name` in the folder `parent`.
func (cli *DriveClient) CreateFile(
	name, parent string, body io.Reader,
) (*drive.File, error) {
	// Use root as default, if parent id is empty.
	if parent == "" {
		parent = cli.Root
	}
	f := drive.File{
		Name:     name,
		Parents:  []string{parent},
		MimeType: "application/octet-stream",
	}
	file, err := cli.Drive.Files.Create(&f).Media(body).Do()
	if err != nil {
		return nil, err
	}
	return file, nil
}

// q: A query for filtering the file results.
func (cli *DriveClient) list(q string, pagesize int64, orderby string) (*drive.FileList, error) {
	call := cli.Drive.Files.List().
		Q(q).
		Spaces("drive").
		Fields("nextPageToken, files(id, name, mimeType)")
	if pagesize != 0 {
		call.PageSize(pagesize)
	}
	if orderby != "" {
		call.OrderBy(orderby)
	}
	r, err := call.Do()
	if err != nil {
		return nil, err
	}
	return r, nil
}

// ListFile list files, include folders.
func (cli *DriveClient) ListFile(parent string, limit int64) (*drive.FileList, error) {
	if parent == "" {
		parent = cli.Root
	}
	// Not folders and belongs to parent.
	q := fmt.Sprintf("'%s' in parents", parent)
	return cli.list(q, limit, "")
}

// ListFileByNameDesc list files order by name desc, include folders.
func (cli *DriveClient) ListFileByNameDesc(parent string, limit int64) (*drive.FileList, error) {
	if parent == "" {
		parent = cli.Root
	}
	// Not folders and belongs to parent.
	q := fmt.Sprintf("'%s' in parents", parent)
	return cli.list(q, limit, "name desc")
}

// ListFolder list folders.
func (cli *DriveClient) ListFolder(parent string, limit int64) (*drive.FileList, error) {
	if parent == "" {
		parent = cli.Root
	}
	// Folders only and belongs to parent.
	q := fmt.Sprintf("mimeType='application/vnd.google-apps.folder' AND '%s' in parents", parent)
	return cli.list(q, limit, "")
}

// ListFolderByName list folders.
func (cli *DriveClient) ListFolderByName(
	name string, parent string, limit int64,
) (*drive.FileList, error) {
	if parent == "" {
		parent = cli.Root
	}
	// Folders only and belongs to parent.
	q := fmt.Sprintf(
		"mimeType='application/vnd.google-apps.folder' AND '%s' in parents AND name contains '%s'",
		parent, name)
	return cli.list(q, limit, "")
}
