package recovery

import (
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
