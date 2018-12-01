package service

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
)

func TestConfigToml(t *testing.T) {
	sample := `
report-interval = "1m49s"
drive-root-id = "0B8BymDS1DJPAZVFvMDdZSmNMaak"
max-retry = 8897

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
	if cfg.MaxRetry != 8897 {
		t.Fatalf("want %#v, got %#v", sample, cfg)
	}
	if cfg.ReportInterval.Duration != time.Minute+time.Second*49 {
		t.Fatalf("want %#v, got %#v", sample, cfg)
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
