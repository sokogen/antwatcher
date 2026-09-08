package model

// Status values GitHub reports for runs, jobs, and steps.
const (
	StatusCompleted  = "completed"
	StatusInProgress = "in_progress"
	StatusQueued     = "queued"
	StatusWaiting    = "waiting"
	StatusRequested  = "requested"
	StatusPending    = "pending"
)

// Status ranks returned by StatusRank.
const (
	RankUnknown    = 0
	RankScheduled  = 1
	RankInProgress = 2
	RankCompleted  = 3
)

// StatusRank orders statuses by how far along the lifecycle they are:
// completed (3) > in_progress (2) > queued, waiting, requested, pending (1) >
// anything else, including empty (0).
//
// Analytics uses it to let the latest state win when events of the same entity
// arrive out of order or are replayed, and logs use it to pick severity. It is
// deliberately the only place that knows this ordering.
func StatusRank(status string) int {
	switch status {
	case StatusCompleted:
		return RankCompleted
	case StatusInProgress:
		return RankInProgress
	case StatusQueued, StatusWaiting, StatusRequested, StatusPending:
		return RankScheduled
	default:
		return RankUnknown
	}
}
