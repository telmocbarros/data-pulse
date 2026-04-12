package jobmanager

import (
	"context"
	"sync"

	"github.com/telmocbarros/data-pulse/internal/models"
	jobRepo "github.com/telmocbarros/data-pulse/internal/repository/job"
)

// JobFunc is the work function executed by the worker pool.
// It receives a cancellable context and a callback to report progress (0-100).
type JobFunc func(ctx context.Context, progressFn func(int)) error

// Manager coordinates background job execution with a fixed-size worker pool.
type Manager struct {
	sem     chan struct{}
	mu      sync.Mutex
	cancels map[string]context.CancelFunc
}

// Default is the global job manager, initialised via Init.
var Default *Manager

// Init creates the global job manager with the given worker pool size.
func Init(workers int) {
	Default = &Manager{
		sem:     make(chan struct{}, workers),
		cancels: make(map[string]context.CancelFunc),
	}
}

// Submit enqueues a job for execution. The goroutine waits for a worker
// slot before running fn.
func (m *Manager) Submit(jobID string, fn JobFunc) {
	go func() {
		m.sem <- struct{}{}
		defer func() { <-m.sem }()

		ctx, cancel := context.WithCancel(context.Background())

		m.mu.Lock()
		m.cancels[jobID] = cancel
		m.mu.Unlock()

		defer func() {
			m.mu.Lock()
			delete(m.cancels, jobID)
			m.mu.Unlock()
			cancel()
		}()

		jobRepo.UpdateJobStatus(jobID, models.JobStatusRunning)

		progressFn := func(pct int) {
			jobRepo.UpdateJobProgress(jobID, pct)
		}

		err := fn(ctx, progressFn)

		if err != nil {
			if ctx.Err() == context.Canceled {
				jobRepo.UpdateJobStatus(jobID, models.JobStatusCancelled)
			} else {
				jobRepo.FailJob(jobID, err.Error())
			}
			return
		}

		jobRepo.UpdateJobStatus(jobID, models.JobStatusCompleted)
		jobRepo.UpdateJobProgress(jobID, 100)
	}()
}

// Cancel cancels a running job. Returns true if the job was found and cancelled.
func (m *Manager) Cancel(jobID string) bool {
	m.mu.Lock()
	cancel, ok := m.cancels[jobID]
	m.mu.Unlock()
	if ok {
		cancel()
	}
	return ok
}
