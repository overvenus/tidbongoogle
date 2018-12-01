package sheet

import (
	"time"

	"github.com/overvenus/tidbongoogle/pkg/gsuite"
)

func (d *decoder) sync() {
	data := d.Dump()
	for k, v := range data {
		gsuite.Create(k, v)
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
