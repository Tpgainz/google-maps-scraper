package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/gosom/scrapemate"
)

// StatusManager handles job status updates and parent-child tracking.
type StatusManager struct {
	db        *sql.DB
	apiClient *APIClient
}

// NewStatusManager creates a new StatusManager.
func NewStatusManager(db *sql.DB, apiClient *APIClient) *StatusManager {
	return &StatusManager{
		db:        db,
		apiClient: apiClient,
	}
}

// MarkDone marks a job as done and handles parent-child tracking.
func (s *StatusManager) MarkDone(ctx context.Context, job scrapemate.IJob, childJobsCreated int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if childJobsCreated == 0 {
		q := `UPDATE gmaps_jobs SET status = $1 WHERE id = $2`
		_, err = tx.ExecContext(ctx, q, statusDone, job.GetID())
		if err != nil {
			return err
		}

		// Check if this is a ROOT job (no parent) with no children
		// Only call completion API for root jobs that have completed entirely
		var parentID sql.NullString
		var childCount int
		err = tx.QueryRowContext(ctx,
			`SELECT parent_id, child_jobs_count FROM gmaps_jobs WHERE id = $1`,
			job.GetID()).Scan(&parentID, &childCount)
		if err == nil && !parentID.Valid && childCount == 0 {
			// This is a root job with no children - call completion API
			var payload []byte
			err = tx.QueryRowContext(ctx, `SELECT payload FROM gmaps_jobs WHERE id = $1`, job.GetID()).Scan(&payload)
			if err == nil {
				s.apiClient.CallJobCompletionAPIAsync(ctx, job.GetID(), payload)
			}
		}

		if err := s.checkAndMarkParentDone(ctx, tx, job.GetID()); err != nil {
			return err
		}
	} else {
		q := `UPDATE gmaps_jobs SET status = $1 WHERE id = $2`
		_, err = tx.ExecContext(ctx, q, statusProcessing, job.GetID())
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// MarkFailed marks a job as failed and updates parent tracking.
func (s *StatusManager) MarkFailed(ctx context.Context, job scrapemate.IJob) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	q := `UPDATE gmaps_jobs SET status = $1 WHERE id = $2`
	log := scrapemate.GetLoggerFromContext(ctx)
	log.Info(fmt.Sprintf("Marking job %s as failed", job.GetID()))
	_, err = tx.ExecContext(ctx, q, statusFailed, job.GetID())
	if err != nil {
		return err
	}

	if err := s.incrementParentFailedCounter(ctx, tx, job.GetID()); err != nil {
		return err
	}

	if err := s.checkAndMarkParentDone(ctx, tx, job.GetID()); err != nil {
		return err
	}

	return tx.Commit()
}

// incrementParentFailedCounter increments the failed counter on the parent job.
func (s *StatusManager) incrementParentFailedCounter(ctx context.Context, tx *sql.Tx, jobID string) error {
	var parentID sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT parent_id FROM gmaps_jobs WHERE id = $1`, jobID).Scan(&parentID)
	if err != nil || !parentID.Valid {
		return err
	}

	_, err = tx.ExecContext(ctx, `UPDATE gmaps_jobs SET child_jobs_failed = child_jobs_failed + 1 WHERE id = $1`, parentID.String)
	if err != nil {
		return err
	}

	return nil
}

// checkAndMarkParentDone checks if all child jobs are done and marks the parent as done.
func (s *StatusManager) checkAndMarkParentDone(ctx context.Context, tx *sql.Tx, jobID string) error {
	var parentID sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT parent_id FROM gmaps_jobs WHERE id = $1`, jobID).Scan(&parentID)
	if err != nil || !parentID.Valid {
		return err
	}

	var shouldIncrementCompleted bool
	var currentStatus string
	err = tx.QueryRowContext(ctx, `SELECT status FROM gmaps_jobs WHERE id = $1`, jobID).Scan(&currentStatus)
	if err == nil && currentStatus == statusDone {
		shouldIncrementCompleted = true
	}

	if shouldIncrementCompleted {
		_, err = tx.ExecContext(ctx, `UPDATE gmaps_jobs SET child_jobs_completed = child_jobs_completed + 1 WHERE id = $1`, parentID.String)
		if err != nil {
			return err
		}
	}

	var childCount, completedCount, failedCount int
	err = tx.QueryRowContext(ctx,
		`SELECT child_jobs_count, child_jobs_completed, child_jobs_failed FROM gmaps_jobs WHERE id = $1`,
		parentID.String).Scan(&childCount, &completedCount, &failedCount)
	if err != nil {
		return err
	}

	totalProcessed := completedCount + failedCount
	if totalProcessed >= childCount && childCount > 0 {
		_, err = tx.ExecContext(ctx, `UPDATE gmaps_jobs SET status = $1 WHERE id = $2`, statusDone, parentID.String)
		if err != nil {
			return err
		}

		// Check if the parent that just completed is a ROOT job (no grandparent)
		// Only call completion API once when the ROOT job finishes
		var grandParentID sql.NullString
		err = tx.QueryRowContext(ctx, `SELECT parent_id FROM gmaps_jobs WHERE id = $1`, parentID.String).Scan(&grandParentID)
		if err == nil && !grandParentID.Valid {
			// parentID is the ROOT job - all children completed, call completion API
			var payload []byte
			err = tx.QueryRowContext(ctx, `SELECT payload FROM gmaps_jobs WHERE id = $1`, parentID.String).Scan(&payload)
			if err == nil {
				s.apiClient.CallJobCompletionAPIAsync(ctx, parentID.String, payload)
			}
			// No need to recurse - we've reached the root
			return nil
		}

		// Continue up the hierarchy
		return s.checkAndMarkParentDone(ctx, tx, parentID.String)
	}

	return nil
}
