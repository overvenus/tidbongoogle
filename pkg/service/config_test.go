package service

import (
	"testing"

	"github.com/BurntSushi/toml"
)

func TestConfigToml(t *testing.T) {
	sample := `
drive-root-id = "0B8BymDS1DJPAZVFvMDdZSmNMaak"

[google]
credentials = "var/cred.json"
drive-token-file = "var/dtok.json"
sheet-token-file = "var/stok.json"
`
	cfg := new(Config)
	_, err := toml.Decode(sample, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DriveRootID != "0B8BymDS1DJPAZVFvMDdZSmNMaak" {
		t.Fatalf("want %#v, got %#v", sample, cfg)
	}
	if cfg.Google.Credentials != "var/cred.json" {
		t.Fatalf("want %#v, got %#v", sample, cfg)
	}
	if cfg.Google.DriveTokenFile != "var/dtok.json" {
		t.Fatalf("want %#v, got %#v", sample, cfg)
	}
	if cfg.Google.SheetTokenFile != "var/stok.json" {
		t.Fatalf("want %#v, got %#v", sample, cfg)
	}
}
