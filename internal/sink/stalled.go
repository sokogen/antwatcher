package sink

import (
	"sort"
	"sync"
	"time"

	"github.com/sokogen/antwatcher/internal/metrics"
)

// stalledLogEvery is how many attempts pass between error log lines for the
// same stalled message after the first one. The broker paces the attempts,
// so this bounds the log volume of a message that stays stalled for days.
const stalledLogEvery = 10

// maxStalledEntries caps the messages one sink tracks individually. A sink
// whose destination rejects everything permanently — an expired OTLP
// credential, a table whose schema no longer matches — would otherwise grow
// an entry per message for as long as the bus keeps redelivering, and put all
// of them in every /status response. Past the cap the failures are still
// counted and logged, but no longer remembered per message: the first entries
// already say what is wrong.
const maxStalledEntries = 1000

// StalledInfo describes one message a sink cannot process because of a
// permanent error. The message stays on the bus and is re-attempted at the
// broker's pace; the entry disappears when the same message succeeds.
type StalledInfo struct {
	// Reason is the last permanent error, as text.
	Reason string `json:"reason"`
	// FirstSeen is when the message first failed permanently on this sink.
	FirstSeen time.Time `json:"first_seen"`
	// LastSeen is when the message last failed.
	LastSeen time.Time `json:"last_seen"`
	// Attempts counts the permanent failures of this message on this sink.
	Attempts int `json:"attempts"`
}

// StalledEntry is a stalled message as listed in /status.
type StalledEntry struct {
	UUID string `json:"uuid"`
	StalledInfo
}

// stalledSet tracks the stalled messages of one sink by message UUID and
// keeps antwatcher_sink_stalled{sink} and antwatcher_sink_stalled_messages{sink}
// in step with it. It holds at most maxStalledEntries messages, so both the
// set and the /status document it feeds stay bounded.
type stalledSet struct {
	sink    string
	metrics *metrics.Metrics

	mu      sync.Mutex
	entries map[string]*StalledInfo
	// untracked counts permanent failures of messages that arrived when the
	// set was full. It only paces their logging; it is not a message count.
	untracked int
}

func newStalledSet(sink string, m *metrics.Metrics) *stalledSet {
	s := &stalledSet{sink: sink, metrics: m, entries: map[string]*StalledInfo{}}
	s.publish()
	return s
}

// record upserts the entry for uuid and returns the attempt count and
// whether this attempt should be logged at error level: the first one and
// then every stalledLogEvery attempts. Once the set holds maxStalledEntries
// messages a new one is not tracked; its failures report one attempt and are
// logged at the same pace.
func (s *stalledSet) record(uuid string, reason error, now time.Time) (attempts int, logNow bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, ok := s.entries[uuid]
	if !ok {
		if len(s.entries) >= maxStalledEntries {
			s.untracked++
			return 1, s.untracked == 1 || s.untracked%stalledLogEvery == 0
		}
		info = &StalledInfo{FirstSeen: now}
		s.entries[uuid] = info
	}
	info.Attempts++
	info.LastSeen = now
	info.Reason = reason.Error()
	s.publishLocked()
	return info.Attempts, info.Attempts == 1 || info.Attempts%stalledLogEvery == 0
}

// clear removes the entry for uuid and reports whether one existed. Only the
// same UUID clears its entry: another message succeeding says nothing about
// the stalled one.
func (s *stalledSet) clear(uuid string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entries[uuid]; !ok {
		return false
	}
	delete(s.entries, uuid)
	s.publishLocked()
	return true
}

// len returns the number of stalled messages the set tracks, at most
// maxStalledEntries.
func (s *stalledSet) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// list returns the tracked stalled messages ordered by first occurrence, then
// UUID; at most maxStalledEntries of them, so /status stays bounded.
func (s *stalledSet) list() []StalledEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]StalledEntry, 0, len(s.entries))
	for uuid, info := range s.entries {
		out = append(out, StalledEntry{UUID: uuid, StalledInfo: *info})
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].FirstSeen.Equal(out[j].FirstSeen) {
			return out[i].FirstSeen.Before(out[j].FirstSeen)
		}
		return out[i].UUID < out[j].UUID
	})
	return out
}

func (s *stalledSet) publish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publishLocked()
}

func (s *stalledSet) publishLocked() {
	if s.metrics == nil {
		return
	}
	n := len(s.entries)
	stalled := 0.0
	if n > 0 {
		stalled = 1
	}
	s.metrics.SinkStalled.WithLabelValues(s.sink).Set(stalled)
	s.metrics.SinkStalledMessages.WithLabelValues(s.sink).Set(float64(n))
}
