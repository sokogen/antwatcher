package recovery

import (
	"sort"
	"time"

	"github.com/sokogen/antwatcher/internal/ghclient"
)

// attempts summarizes every delivery attempt GitHub made for one GUID.
type attempts struct {
	latestID  int64     // delivery ID of the most recent attempt
	latestAt  time.Time // when the most recent attempt was made
	succeeded bool      // at least one attempt got a 2xx
}

// group folds the attempts of a scan by GUID. The latest attempt is the one
// with the newest DeliveredAt; among equal timestamps the higher ID wins, so
// a redelivery recorded in the same second as its original is the latest.
func group(deliveries []ghclient.Delivery) map[string]attempts {
	out := make(map[string]attempts, len(deliveries))
	for _, d := range deliveries {
		if d.GUID == "" {
			continue
		}
		a := out[d.GUID]
		if d.Succeeded() {
			a.succeeded = true
		}
		if a.latestID == 0 || d.DeliveredAt.After(a.latestAt) || (d.DeliveredAt.Equal(a.latestAt) && d.ID > a.latestID) {
			a.latestID, a.latestAt = d.ID, d.DeliveredAt
		}
		out[d.GUID] = a
	}
	return out
}

// Plan decides which deliveries to ask GitHub to redeliver, given the attempts
// of one scan. Attempts are grouped by GUID: a GUID with any 2xx attempt
// (original or redelivery) reached the receiver and is skipped; a GUID whose
// latest attempt is younger than grace is skipped because it may still be in
// flight or was just redelivered; every other GUID is missing and the ID of
// its latest attempt is returned, oldest first. Plan is pure: it keeps no
// state and never talks to GitHub.
func Plan(deliveries []ghclient.Delivery, now time.Time, grace time.Duration) []int64 {
	grouped := group(deliveries)
	type candidate struct {
		id int64
		at time.Time
	}
	var picked []candidate
	for _, a := range grouped {
		if a.succeeded || now.Sub(a.latestAt) < grace {
			continue
		}
		picked = append(picked, candidate{id: a.latestID, at: a.latestAt})
	}
	sort.Slice(picked, func(i, j int) bool {
		if !picked[i].at.Equal(picked[j].at) {
			return picked[i].at.Before(picked[j].at)
		}
		return picked[i].id < picked[j].id
	})
	ids := make([]int64, 0, len(picked))
	for _, c := range picked {
		ids = append(ids, c.id)
	}
	return ids
}
