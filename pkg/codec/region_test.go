package codec

import (
	"testing"
)

func TestRegionEncodeDecode(t *testing.T) {
	ids := []uint64{0, 1, 3, 1024, 4096, (1 << 63) - 1}
	for _, rid := range ids {
		for _, pid := range ids {
			name := EncodeRegionFolder(rid, pid)
			r, p, e := DecodeRegionFolder(name)
			t.Log(name)
			if r != rid || p != pid || e != nil {
				t.Fatal(rid, pid, name)
			}
		}
	}
}

func TestSnapEncodeDecode(t *testing.T) {
	ids := []uint64{0, 1, 3, 1024, 4096, (1 << 63) - 1}
	for _, rid := range ids {
		for _, pid := range ids {
			name := EncodeSnapFolder(rid, pid)
			r, p, e := DecodeSnapFolder(name)
			t.Log(name)
			if r != rid || p != pid || e != nil {
				t.Fatal(rid, pid, name)
			}
		}
	}
}

func TestSnapshotEncodeDecode(t *testing.T) {
	ids := []uint64{0, 1, 3, 1024, 4096, (1 << 63) - 1}
	for _, applied := range ids {
		for _, truncated := range ids {
			for _, term := range ids {
				name := EncodeSnapshotFolder(applied, truncated, term)
				a, tr, te, e := DecodeSnapshotFolder(name)
				t.Log(name)
				if a != applied || tr != truncated || te != term || e != nil {
					t.Fatal(applied, truncated, term, name)
				}
			}
		}
	}
}
func TestRaftLogEncodeDecode(t *testing.T) {
	ids := []uint64{0, 1, 3, 1024, 4096, (1 << 63) - 1}
	for _, term := range ids {
		for _, index := range ids {
			name := EncodeRaftLog(term, index)
			tr, i, e := DecodeRaftLog(name)
			t.Log(name)
			if tr != term || i != index || e != nil {
				t.Fatal(term, index, name)
			}
		}
	}
}
