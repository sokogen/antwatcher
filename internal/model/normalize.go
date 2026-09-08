package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/sokogen/antwatcher/internal/event"
)

// GitHub webhook event names that Normalize understands.
const (
	EventWorkflowRun = "workflow_run"
	EventWorkflowJob = "workflow_job"
)

// ErrMalformed is returned when a workflow_run or workflow_job payload cannot
// be decoded into the model: invalid JSON, a missing entity object, or missing
// identity fields. It is permanent by definition: redelivering the same bytes
// cannot make them well-formed.
var ErrMalformed = errors.New("malformed payload")

// Normalize builds the canonical Execution for an envelope.
//
// workflow_run events (every action) yield KindRun; workflow_job events (every
// action) yield KindJob with the steps carried by that event. Any other event
// yields KindNone with a nil Run and Job and no error, so consumers can still
// log or archive it. A known event whose payload does not decode returns an
// error wrapping ErrMalformed.
func Normalize(env event.Envelope) (Execution, error) {
	exec := Execution{Kind: KindNone, Envelope: env}
	switch env.Event {
	case EventWorkflowRun:
		run, err := decodeRun(env)
		if err != nil {
			return Execution{}, err
		}
		exec.Kind, exec.Run = KindRun, run
	case EventWorkflowJob:
		job, err := decodeJob(env)
		if err != nil {
			return Execution{}, err
		}
		exec.Kind, exec.Job = KindJob, job
	}
	return exec, nil
}

// rawRepository is the part of the payload's repository object the model uses.
type rawRepository struct {
	ID       int64  `json:"id"`
	FullName string `json:"full_name"`
}

// rawActor is a GitHub user object reduced to its login.
type rawActor struct {
	Login string `json:"login"`
}

// rawRunPayload mirrors the workflow_run webhook body.
type rawRunPayload struct {
	WorkflowRun *struct {
		ID           int64      `json:"id"`
		Name         *string    `json:"name"`
		Path         string     `json:"path"`
		RunNumber    int64      `json:"run_number"`
		RunAttempt   int64      `json:"run_attempt"`
		Event        string     `json:"event"`
		Status       *string    `json:"status"`
		Conclusion   *string    `json:"conclusion"`
		WorkflowID   int64      `json:"workflow_id"`
		CreatedAt    time.Time  `json:"created_at"`
		UpdatedAt    time.Time  `json:"updated_at"`
		RunStartedAt *time.Time `json:"run_started_at"`
		HeadBranch   *string    `json:"head_branch"`
		HeadSHA      string     `json:"head_sha"`
		Actor        *rawActor  `json:"actor"`
		HTMLURL      string     `json:"html_url"`
	} `json:"workflow_run"`
	Repository *rawRepository `json:"repository"`
}

// rawJobPayload mirrors the workflow_job webhook body.
type rawJobPayload struct {
	WorkflowJob *struct {
		ID              int64      `json:"id"`
		RunID           int64      `json:"run_id"`
		RunAttempt      int64      `json:"run_attempt"`
		Name            string     `json:"name"`
		WorkflowName    *string    `json:"workflow_name"`
		Status          *string    `json:"status"`
		Conclusion      *string    `json:"conclusion"`
		CreatedAt       time.Time  `json:"created_at"`
		StartedAt       *time.Time `json:"started_at"`
		CompletedAt     *time.Time `json:"completed_at"`
		RunnerName      *string    `json:"runner_name"`
		RunnerGroupName *string    `json:"runner_group_name"`
		Labels          []string   `json:"labels"`
		HeadBranch      *string    `json:"head_branch"`
		HeadSHA         string     `json:"head_sha"`
		HTMLURL         string     `json:"html_url"`
		Steps           []struct {
			Number      int64      `json:"number"`
			Name        string     `json:"name"`
			Status      *string    `json:"status"`
			Conclusion  *string    `json:"conclusion"`
			StartedAt   *time.Time `json:"started_at"`
			CompletedAt *time.Time `json:"completed_at"`
		} `json:"steps"`
	} `json:"workflow_job"`
	Repository *rawRepository `json:"repository"`
}

func decodeRun(env event.Envelope) (*Run, error) {
	var raw rawRunPayload
	if err := decode(env, &raw); err != nil {
		return nil, err
	}
	wr := raw.WorkflowRun
	if wr == nil {
		return nil, malformed(env, "missing workflow_run object")
	}
	if wr.ID == 0 || wr.RunAttempt == 0 {
		return nil, malformed(env, "workflow_run.id and workflow_run.run_attempt are required")
	}
	repoID, repoName, err := repository(env, raw.Repository)
	if err != nil {
		return nil, err
	}
	run := &Run{
		RepositoryID: repoID,
		Repository:   repoName,
		WorkflowID:   wr.WorkflowID,
		WorkflowName: wr.Name,
		WorkflowPath: wr.Path,
		RunID:        wr.ID,
		RunNumber:    wr.RunNumber,
		RunAttempt:   wr.RunAttempt,
		Status:       deref(wr.Status),
		Conclusion:   wr.Conclusion,
		CreatedAt:    wr.CreatedAt.UTC(),
		UpdatedAt:    wr.UpdatedAt.UTC(),
		StartedAt:    utc(wr.RunStartedAt),
		TriggerEvent: wr.Event,
		HeadBranch:   wr.HeadBranch,
		HeadSHA:      wr.HeadSHA,
		HTMLURL:      wr.HTMLURL,
	}
	if wr.Actor != nil {
		login := wr.Actor.Login
		run.Actor = &login
	}
	return run, nil
}

func decodeJob(env event.Envelope) (*Job, error) {
	var raw rawJobPayload
	if err := decode(env, &raw); err != nil {
		return nil, err
	}
	wj := raw.WorkflowJob
	if wj == nil {
		return nil, malformed(env, "missing workflow_job object")
	}
	if wj.ID == 0 || wj.RunID == 0 || wj.RunAttempt == 0 {
		return nil, malformed(env, "workflow_job.id, workflow_job.run_id, and workflow_job.run_attempt are required")
	}
	repoID, repoName, err := repository(env, raw.Repository)
	if err != nil {
		return nil, err
	}
	job := &Job{
		RepositoryID: repoID,
		Repository:   repoName,
		RunID:        wj.RunID,
		RunAttempt:   wj.RunAttempt,
		JobID:        wj.ID,
		Name:         wj.Name,
		WorkflowName: wj.WorkflowName,
		Status:       deref(wj.Status),
		Conclusion:   wj.Conclusion,
		CreatedAt:    wj.CreatedAt.UTC(),
		StartedAt:    utc(wj.StartedAt),
		CompletedAt:  utc(wj.CompletedAt),
		RunnerName:   wj.RunnerName,
		RunnerGroup:  wj.RunnerGroupName,
		Labels:       wj.Labels,
		HeadBranch:   wj.HeadBranch,
		HeadSHA:      wj.HeadSHA,
		HTMLURL:      wj.HTMLURL,
	}
	if len(wj.Steps) > 0 {
		job.Steps = make([]Step, 0, len(wj.Steps))
		for _, s := range wj.Steps {
			job.Steps = append(job.Steps, Step{
				Number:      s.Number,
				Name:        s.Name,
				Status:      deref(s.Status),
				Conclusion:  s.Conclusion,
				StartedAt:   utc(s.StartedAt),
				CompletedAt: utc(s.CompletedAt),
			})
		}
	}
	return job, nil
}

// decode unmarshals the payload of a known event; any JSON error is malformed.
func decode(env event.Envelope, into any) error {
	if len(env.Payload) == 0 {
		return malformed(env, "empty payload")
	}
	if err := json.Unmarshal(env.Payload, into); err != nil {
		return fmt.Errorf("%w: %s %q: %w", ErrMalformed, env.Event, env.DeliveryGUID, err)
	}
	return nil
}

// repository resolves the repository identity from the payload, falling back
// to the envelope (which was extracted from the same payload at ingress). A
// workflow event without a repository id has no usable identity.
func repository(env event.Envelope, raw *rawRepository) (int64, string, error) {
	id, name := env.RepositoryID, env.Repository
	if raw != nil && raw.ID != 0 {
		id, name = raw.ID, raw.FullName
	}
	if id == 0 {
		return 0, "", malformed(env, "missing repository.id")
	}
	return id, name, nil
}

func malformed(env event.Envelope, reason string) error {
	return fmt.Errorf("%w: %s %q: %s", ErrMalformed, env.Event, env.DeliveryGUID, reason)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// utc returns a copy of t in UTC so equal instants compare equal regardless of
// the offset GitHub happened to serialize.
func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
