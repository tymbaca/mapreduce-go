package mapreduce

import (
	"fmt"
	"sync/atomic"
)

var GlobalStats = &Stats{}

type Stats struct {
	MapIn, MapOut       atomic.Uint64
	ReduceIn, ReduceOut atomic.Uint64
}

func (s *Stats) String() string {
	mi := s.MapIn.Load()
	mo := s.MapOut.Load()
	ri := s.ReduceIn.Load()
	ro := s.ReduceOut.Load()
	return fmt.Sprintf("MapIn: %d, MapOut: %d, ReduceIn: %d, ReduceOut: %d", mi, mo, ri, ro)
}
