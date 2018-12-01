package codec

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	commonSEP      = "_"
	regionPrefix   = "region"
	raftLogPrefix  = "log"
	snapPrefix     = "snap"
	snapshotPerfix = "shot"
)

// DecodeRegionFolder decode a region folder name
// "region_2_3" => region_id: 2, peer_id: 3
func DecodeRegionFolder(name string) (uint64, uint64, error) {
	seqs := strings.Split(name, commonSEP)
	if len(seqs) != 3 || seqs[0] != regionPrefix {
		return 0, 0, fmt.Errorf("%s", name)
	}
	rid, err := strconv.ParseUint(seqs[1], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("%s", name)
	}
	pid, err := strconv.ParseUint(seqs[2], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("%s", name)
	}
	return uint64(rid), uint64(pid), nil
}

// EncodeRegionFolder encode region id and peer id to a region folder name
func EncodeRegionFolder(regionID, peerID uint64) string {
	return fmt.Sprintf("%s%s%016x%s%016x",
		regionPrefix, commonSEP, regionID, commonSEP, peerID)
}

// DecodeSnapFolder decode a region snap folder name
// "snap_2_3" => region_id: 2, peer_id: 3
func DecodeSnapFolder(name string) (uint64, uint64, error) {
	seqs := strings.Split(name, commonSEP)
	if len(seqs) != 3 || seqs[0] != snapPrefix {
		return 0, 0, fmt.Errorf("%s", name)
	}
	rid, err := strconv.ParseUint(seqs[1], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("%s", name)
	}
	pid, err := strconv.ParseUint(seqs[2], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("%s", name)
	}
	return uint64(rid), uint64(pid), nil
}

// EncodeSnapFolder encode region id and peer id to a snap folder name
func EncodeSnapFolder(regionID, peerID uint64) string {
	return fmt.Sprintf("%s%s%016x%s%016x",
		snapPrefix, commonSEP, regionID, commonSEP, peerID)
}

// DecodeSnapshotFolder decode a snapshot folder name
// "shot_7_7_4" => applied index: 7, truncated index: 7, term :4
func DecodeSnapshotFolder(name string) (uint64, uint64, uint64, error) {
	seqs := strings.Split(name, commonSEP)
	if len(seqs) != 4 || seqs[0] != snapshotPerfix {
		return 0, 0, 0, fmt.Errorf("%s", name)
	}
	applied, err := strconv.ParseUint(seqs[1], 16, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%s", name)
	}
	truncated, err := strconv.ParseUint(seqs[2], 16, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%s", name)
	}
	term, err := strconv.ParseUint(seqs[3], 16, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%s", name)
	}
	return uint64(applied), uint64(truncated), uint64(term), nil
}

// EncodeSnapshotFolder encode applied idx, truncated idx and term to
// a snapshot folder name.
func EncodeSnapshotFolder(appliedIdx, truncatedIdx, term uint64) string {
	return fmt.Sprintf("%s%s%016x%s%016x%s%016x",
		snapshotPerfix, commonSEP, appliedIdx, commonSEP, truncatedIdx, commonSEP, term)
}

// DecodeRaftLog decode file name to term and index
func DecodeRaftLog(name string) (uint64, uint64, error) {
	seqs := strings.Split(name, commonSEP)
	if len(seqs) != 3 || seqs[0] != raftLogPrefix {
		return 0, 0, fmt.Errorf("%s", name)
	}
	term, err := strconv.ParseUint(seqs[1], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("%s", name)
	}
	index, err := strconv.ParseUint(seqs[2], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("%s", name)
	}
	return uint64(term), uint64(index), nil
}

// EncodeRaftLog encode term and index to a file name
func EncodeRaftLog(term, index uint64) string {
	return fmt.Sprintf("%s%s%016x%s%016x",
		raftLogPrefix, commonSEP, term, commonSEP, index)
}
