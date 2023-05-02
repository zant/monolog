package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	// off is the offset _within_ the index i.e. 0, 1, 2
	// this is relative to a base offset which is a variable
	offWidth uint64 = 4
	// pos is the position _within_ the store
	// i.e. 0, 10, 25, 30 etc
	// pos is the amount of bytes that were in
	// the store at which the entry was appended
	posWidth uint64 = 8
	// this is the amount of bytes in total to
	// get all the information to store an antity
	entWidth = offWidth + posWidth
)

// index stores offsets and
// positions within a file (at w bytes the entity starts)
// to then access the store w this information
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	// the size of the index is the size of the file
	idx.size = uint64(fi.Size())
	// truncate to max to use
	// memory mappings
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	// create memory map
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_ASYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	// this could be improved with
	// modular arithmetic to handle all negative numbers
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	// check if the write will fit in the memorymap
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	// first write the offWidth
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	// write the posWidth
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
