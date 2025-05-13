package partition

import (
	"io"
	"math"
	"sync"
	"time"
)

type Partition struct {
	data       [][]byte
	keys       map[string]int
	timestamps []time.Time
	dataMtx    *sync.Mutex
}

func NewPartition() *Partition {
	return &Partition{
		data:       make([][]byte, 0, 1024),
		keys:       make(map[string]int),
		timestamps: make([]time.Time, 0, 1024),
		dataMtx:    &sync.Mutex{},
	}
}

func (p *Partition) ReadData(offset, max int) ([][]byte, error) {
	p.dataMtx.Lock()
	defer p.dataMtx.Unlock()

	if offset >= len(p.data) {
		return nil, io.EOF
	}
	end := math.Min(float64(offset+max), float64(len(p.data)))
	return p.data[offset:int(end)], nil
}

func (p *Partition) cleanup(offset int) int {
	p.dataMtx.Lock()
	defer p.dataMtx.Unlock()

	var lastDeleted int
	for idx := range p.data {
		if p.timestamps[idx].Add(5 * time.Minute).Before(time.Now()) {
			delete(p.keys, string(p.data[idx]))
			lastDeleted = idx
		}
	}
	if lastDeleted > 0 {
		newData := make([][]byte, len(p.data)-lastDeleted)
		newTimestamps := make([]time.Time, len(p.data)-lastDeleted)
		copy(newData, p.data[lastDeleted:])
		copy(newTimestamps, p.timestamps[lastDeleted:])

		p.data = newData
		p.timestamps = newTimestamps
		for k, val := range p.keys {
			p.keys[k] = val - lastDeleted
		}
	}
	return lastDeleted
}
