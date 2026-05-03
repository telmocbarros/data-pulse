// Package job persists Job records and their lifecycle state.
package job

import (
	"log/slog"

	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/models"
)

// CreateJob inserts a new pending job and returns its generated id.
func CreateJob(fileName, fileType string) (string, error) {
	var id string
	err := config.Storage.QueryRow(
		`INSERT INTO jobs (file_name, file_type, status, progress)
		 VALUES ($1, $2, 'pending', 0)
		 RETURNING id`,
		fileName, fileType,
	).Scan(&id)
	if err != nil {
		slog.Error("CreateJob insert failed", "err", err, "fileType", fileType)
		return "", err
	}
	return id, nil
}

// GetJob returns the job row for the given id.
func GetJob(id string) (*models.Job, error) {
	job := &models.Job{}
	err := config.Storage.QueryRow(
		`SELECT id, file_name, file_type, status, progress, COALESCE(error, ''), created_at, updated_at
		 FROM jobs WHERE id = $1`, id,
	).Scan(&job.ID, &job.FileName, &job.FileType, &job.Status, &job.Progress,
		&job.Error, &job.CreatedAt, &job.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// UpdateJobStatus sets the status column and refreshes updated_at.
func UpdateJobStatus(id string, status models.JobStatus) error {
	_, err := config.Storage.Exec(
		`UPDATE jobs SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`,
		status, id,
	)
	return err
}

// UpdateJobProgress sets the progress column (0-100) and refreshes updated_at.
func UpdateJobProgress(id string, progress int) error {
	_, err := config.Storage.Exec(
		`UPDATE jobs SET progress = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`,
		progress, id,
	)
	return err
}

// FailJob marks the job failed and records the error message.
func FailJob(id string, errMsg string) error {
	_, err := config.Storage.Exec(
		`UPDATE jobs SET status = 'failed', error = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`,
		errMsg, id,
	)
	return err
}
