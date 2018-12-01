package sheet

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/codec"
)

type TypeFlag byte

const (
	// StringMeta is the flag for string meta.
	StringMeta TypeFlag = 'S'
	// StringData is the flag for string data.
	StringData TypeFlag = 's'
	// HashMeta is the flag for hash meta.
	HashMeta TypeFlag = 'H'
	// HashData is the flag for hash data.
	HashData TypeFlag = 'h'
	// ListMeta is the flag for list meta.
	ListMeta TypeFlag = 'L'
	// ListData is the flag for list data.
	ListData TypeFlag = 'l'
)

func encodeHashDataKey(key []byte, field []byte) kv.Key {
	ek := make([]byte, 0, 10+len(key)+len(field)+30)
	ek = append(ek, []byte("m")...)
	ek = codec.EncodeBytes(ek, key)
	ek = codec.EncodeUint(ek, uint64(HashData))
	return codec.EncodeBytes(ek, field)
}

func decodeHashDataKey(ek []byte) ([]byte, []byte, error) {
	var (
		key   []byte
		field []byte
		err   error
		tp    uint64
	)

	if !bytes.HasPrefix(ek, []byte{'m'}) {
		return nil, nil, errors.New("invalid encoded hash data key prefix")
	}

	ek = ek[1:]

	ek, key, err = codec.DecodeBytes(ek, nil)
	if err != nil {
		return nil, nil, err
	}

	ek, tp, err = codec.DecodeUint(ek)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid encoded hash data key flag %c", byte(tp))
	}

	_, field, err = codec.DecodeBytes(ek, nil)
	return key, field, err
}
