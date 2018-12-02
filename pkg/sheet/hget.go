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

func decodeHashDataKey(ek []byte) ([]byte, []byte, uint8, error) {
	var (
		key   []byte
		field []byte
		err   error
		tp    uint64
	)

	if !bytes.HasPrefix(ek, []byte{'m'}) {
		return nil, nil, 0, errors.New("invalid encoded hash data key prefix")
	}

	ek = ek[1:]

	ek, key, err = codec.DecodeBytes(ek, nil)
	if err != nil {
		return nil, nil, 0, err
	}

	ek, tp, err = codec.DecodeUint(ek)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("invalid encoded hash data key flag %c", byte(tp))
	}

	switch tp {
	case 'S', 's', 'H', 'L':
		return key, nil, uint8(tp & 0xff), nil
	case 'h':
		_, field, err = codec.DecodeBytes(ek, nil)
		return key, field, 'h', err
	case 'l':
		_, fieldInt, err := codec.DecodeUint(ek)
		field := codec.EncodeUint(field, fieldInt)
		return key, field, 'l', err
	default:
		return nil, nil, 0, fmt.Errorf("Failed to decode key, undefined type: %x", uint8(tp&0xFF))
	}

	return nil, nil, 0, fmt.Errorf("???")
}
