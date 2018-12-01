package sheet

import (
	"time"

	"github.com/overvenus/tidbongoogle/pkg/googleutil"
)

func (d *decoder) sync() {
	data := d.Dump()
	for k, v := range data {
		googleutil.CreateSheet(k, v)
	}
}

func (d *decoder) AutoSync() {
	go func() {
		for {
			select {
			case <-d.closechan:
				break
			case <-time.After(15 * time.Second):
				d.sync()
			}
		}
	}()
}
