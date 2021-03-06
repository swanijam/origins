/*
The storage package defines an interface for implementing stores. A store
is used to read and writing sets of data including raw statements, parsed facts,
and materialized EAV structures.

This package contains packages that provide an implementation to the Store
interface.
*/
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/chop-dbhi/origins/fact"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
)

const (
	// Version of the store. This is checked against the store header to
	// determine if a migration needs to occur.
	Version = 0

	// Takes the store name.
	storeKeyFmt = "origins.%s"

	// Size of the message framing prefix size for a fact in bytes.
	framePrefixFactSize = 2

	// Maximum fact size in bytes. The fixed size is prefixed to the fact to
	// for length-prefix framing.
	maxFactSize = 1 << (framePrefixFactSize * 8)
)

type Store struct {
	// Name is the given name of the store. This is used to as the top-level
	// prefix of storage keys.
	Name string

	// Version is the version of the store from the last usage. This
	// is used to determin compatibility of the store with the program.
	Version int

	// Fixed signature of the store
	// TODO(bjr): is it good practice to add this?
	// see https://en.wikipedia.org/wiki/List_of_file_signatures
	// Sig []byte

	storeKey string
	engine   Engine
	parts    map[string]*partition

	// Embed mutex methods in the store.
	sync.Mutex
}

// String satisfies the fmt.Stringer interface.
func (s *Store) String() string {
	return s.Name
}

func (s *Store) Proto() proto.Message {
	return &ProtoStore{}
}

func (s *Store) ToProto() (proto.Message, error) {
	return &ProtoStore{
		Name:    proto.String(s.Name),
		Version: proto.Int32(int32(s.Version)),
	}, nil
}

func (s *Store) FromProto(v proto.Message) error {
	m := v.(*ProtoStore)
	s.Name = m.GetName()
	s.Version = int(m.GetVersion())
	return nil
}

func (s *Store) init() error {
	// Key for the store header.
	s.storeKey = fmt.Sprintf(storeKeyFmt, s.Name)

	// Load the header if it exists.
	b, err := s.engine.Get(s.storeKey)

	if err != nil {
		return err
	}

	// Record of store does not exist.
	if b == nil {
		s.Version = Version

		if err = s.writeHeader(); err != nil {
			return err
		}

		logrus.Debugf("Initialized new store named '%s'", s.Name)
	} else {
		if err := UnmarshalProto(b, s); err != nil {
			return err
		}

		if Version != s.Version {
			logrus.Fatalf("Store version is '%s', but using '%s' version of the client.", s.Version, Version)
		}

		logrus.Debugf("Initialized existing store named '%s'", s.Name)
	}

	// Setup internal fields.
	s.parts = make(map[string]*partition)

	return nil
}

// writeHeader writes the current state of the store to storage.
func (s *Store) writeHeader() error {
	b, err := MarshalProto(s)

	if err != nil {
		return err
	}

	return s.engine.Set(s.storeKey, b)
}

func (s *Store) initPartition(domain string) (*partition, error) {
	var (
		p   *partition
		ok  bool
		err error
	)

	// Check if in local cache.
	p, ok = s.parts[domain]

	if ok {
		logrus.Debugf("found partition %v in local cache", domain)
		return p, nil
	}

	// TODO(bjr) check in other cache (e.g. memcache)

	// Partition key
	storeKey := fmt.Sprintf(partitionKeyFmt, s.storeKey, domain)

	p, err = initPartition(storeKey, s.engine)

	if err != nil {
		return nil, err
	}

	// Store in the local cache.
	s.parts[domain] = p

	return p, nil
}

// Close closes the underlying engine. Store operations that rely on the
// engine should be assumed to be fail.
func (s *Store) Close() error {
	return s.engine.Close()
}

func (s *Store) Reader(domain string) (*Reader, error) {
	return s.RangeReader(domain, 0, 0)
}

func (s *Store) RangeReader(domain string, min, max int64) (*Reader, error) {
	_, ok := s.parts[domain]

	if !ok {
		if _, err := s.initPartition(domain); err != nil {
			return nil, err
		}
	}

	return &Reader{
		Min:    min,
		Max:    max,
		store:  s,
		reader: s.parts[domain].Reader(min, max),
	}, nil
}

// WriteSegment writes a segment to storage. The number of bytes written are returned
// or an error.
func (s *Store) WriteSegment(domain string, tx interface{}, facts fact.Facts, commit bool) (int, error) {
	if len(facts) == 0 {
		logrus.Warn("No facts to write.")
		return 0, nil
	}

	var (
		err  error
		key  uint64
		part *partition
		ok   bool
	)

	switch x := tx.(type) {
	case uint64:
		key = x
	case int64:
		key = uint64(x)
	case int:
		key = uint64(x)
	default:
		return 0, errors.New(fmt.Sprintf("Segment key must be an integer value, got %T", tx))
	}

	part, ok = s.parts[domain]

	if !ok {
		logrus.Debugf("initializing partition for %s", domain)

		if part, err = s.initPartition(domain); err != nil {
			logrus.Error(err)
			return 0, err
		}
	}

	var (
		// Size of the fact in bytes.
		size int
		// Total size of the segment.
		total int
		// 2 byte length prefix per fact.
		prefix = make([]byte, 2, 2)
		// The full segment of bytes.
		segment = make([]byte, 0)
		// Fact buffer
		buf []byte
	)

	for _, f := range facts {
		buf, err = MarshalProto(f)

		if err != nil {
			return 0, err
		}

		size = len(buf)

		if size > maxFactSize {
			err = errors.New(fmt.Sprintf("fact size %d exceeds maximum allowed %d", size, maxFactSize))
			logrus.Error(err)
			return 0, err
		}

		// Encode the prefix
		binary.PutUvarint(prefix, uint64(size))

		// Append the prefix, then the buffer
		segment = append(segment, prefix...)
		segment = append(segment, buf...)

		total += size
	}

	if commit {
		err = part.Write(key, segment)

		if err != nil {
			return 0, err
		}
	}

	return total, nil
}
