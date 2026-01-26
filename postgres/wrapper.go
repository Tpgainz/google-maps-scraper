package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/gosom/google-maps-scraper/gmaps"
	"github.com/gosom/scrapemate"
)

// jobWrapper wraps jobs to handle marking them as done after processing.
type jobWrapper struct {
	scrapemate.IJob
	provider *provider
}

// Process handles job processing and child job management.
func (w *jobWrapper) Process(ctx context.Context, resp *scrapemate.Response) (any, []scrapemate.IJob, error) {
	log := scrapemate.GetLoggerFromContext(ctx)
	log.Info(fmt.Sprintf("jobWrapper.Process: Processing job %s (type: %T)", w.IJob.GetID(), w.IJob))

	ctx = context.WithValue(ctx, providerKey{}, w.provider)
	ctx = context.WithValue(ctx, gmaps.CompanyDataCheckerKey{}, w.provider)

	data, nextJobs, err := w.IJob.Process(ctx, resp)

	if err == nil {
		_, isCompanyJob := w.IJob.(*gmaps.CompanyJob)
		_, isPlaceJob := w.IJob.(*gmaps.PlaceJob)

		if len(nextJobs) > 0 {
			if isCompanyJob {
				w.provider.pushChildJobsAsync(ctx, w.IJob, nextJobs)
			} else if isPlaceJob {
				if err := w.provider.pushChildJobsForPlaceJob(ctx, w.IJob, nextJobs); err != nil {
					log.Error(fmt.Sprintf("jobWrapper.Process: Error pushing child jobs for place job: %v", err))
					return data, nextJobs, fmt.Errorf("while pushing jobs: %w", err)
				}
			} else {
				if err := w.provider.pushChildJobs(ctx, w.IJob, nextJobs); err != nil {
					log.Error(fmt.Sprintf("jobWrapper.Process: Error pushing child jobs: %v", err))
					return data, nextJobs, fmt.Errorf("while pushing jobs: %w", err)
				}
			}
		}

		if err := w.provider.statusManager.MarkDone(ctx, w.IJob, len(nextJobs)); err != nil {
			return data, nextJobs, err
		}

		if gmapJob, ok := w.IJob.(*gmaps.GmapJob); ok {
			w.provider.apiClient.CallRevalidationAPI(ctx, gmapJob.OwnerID)
		}

		if isCompanyJob {
			return data, nil, err
		}

		var wrappedNextJobs []scrapemate.IJob
		if len(nextJobs) > 0 {
			wrappedNextJobs = make([]scrapemate.IJob, len(nextJobs))
			for i := range nextJobs {
				wrappedNextJobs[i] = &jobWrapper{IJob: nextJobs[i], provider: w.provider}
			}
		}

		return data, wrappedNextJobs, err
	}

	_ = w.provider.statusManager.MarkFailed(ctx, w.IJob)

	return data, nextJobs, err
}

// ChildJobManager handles pushing child jobs to the database.
type ChildJobManager struct {
	db            *sql.DB
	codecRegistry *CodecRegistry
}

// NewChildJobManager creates a new ChildJobManager.
func NewChildJobManager(db *sql.DB, codecRegistry *CodecRegistry) *ChildJobManager {
	return &ChildJobManager{
		db:            db,
		codecRegistry: codecRegistry,
	}
}

// pushChildJobs pushes child jobs synchronously within a transaction.
func (p *provider) pushChildJobs(ctx context.Context, parentJob scrapemate.IJob, childJobs []scrapemate.IJob) error {
	if len(childJobs) == 0 {
		return nil
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	updateParentQuery := `UPDATE gmaps_jobs SET child_jobs_count = child_jobs_count + $1 WHERE id = $2`
	_, err = tx.ExecContext(ctx, updateParentQuery, len(childJobs), parentJob.GetID())
	if err != nil {
		return err
	}

	for _, childJob := range childJobs {
		if err := p.pushJobWithParent(ctx, tx, childJob, parentJob.GetID()); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// pushChildJobsAsync pushes child jobs asynchronously.
func (p *provider) pushChildJobsAsync(ctx context.Context, parentJob scrapemate.IJob, childJobs []scrapemate.IJob) {
	if len(childJobs) == 0 {
		return
	}

	go func() {
		if err := p.pushChildJobs(context.Background(), parentJob, childJobs); err != nil {
			log := scrapemate.GetLoggerFromContext(ctx)
			log.Error(fmt.Sprintf("Error pushing child jobs asynchronously: %v", err))
		}
	}()
}

// pushChildJobsForPlaceJob handles the special case of pushing child jobs for PlaceJob.
func (p *provider) pushChildJobsForPlaceJob(ctx context.Context, parentJob scrapemate.IJob, childJobs []scrapemate.IJob) error {
	if len(childJobs) == 0 {
		return nil
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	updateParentQuery := `UPDATE gmaps_jobs SET child_jobs_count = child_jobs_count + $1 WHERE id = $2`
	_, err = tx.ExecContext(ctx, updateParentQuery, len(childJobs), parentJob.GetID())
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	go func() {
		asyncCtx := context.Background()
		asyncTx, err := p.db.BeginTx(asyncCtx, nil)
		if err != nil {
			log := scrapemate.GetLoggerFromContext(ctx)
			log.Error(fmt.Sprintf("Error starting transaction for async child jobs: %v", err))
			return
		}
		defer asyncTx.Rollback()

		for _, childJob := range childJobs {
			if err := p.pushJobWithParent(asyncCtx, asyncTx, childJob, parentJob.GetID()); err != nil {
				log := scrapemate.GetLoggerFromContext(ctx)
				log.Error(fmt.Sprintf("Error pushing child job asynchronously: %v", err))
				continue
			}
		}

		if err := asyncTx.Commit(); err != nil {
			log := scrapemate.GetLoggerFromContext(ctx)
			log.Error(fmt.Sprintf("Error committing async child jobs: %v", err))
		}
	}()

	return nil
}

// pushJobWithParent inserts a job with a parent reference.
func (p *provider) pushJobWithParent(ctx context.Context, tx *sql.Tx, job scrapemate.IJob, parentID string) error {
	q := `INSERT INTO gmaps_jobs
		(id, parent_id, priority, payload_type, payload, created_at, status)
		VALUES
		($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING`

	log := scrapemate.GetLoggerFromContext(ctx)

	actualJob := job
	if wrapper, ok := job.(*jobWrapper); ok {
		actualJob = wrapper.IJob
	}

	log.Info(fmt.Sprintf("pushJobWithParent: job type=%T, URL=%s, parentID=%s", actualJob, actualJob.GetURL(), parentID))

	jsonJob, jobType, err := p.codecRegistry.EncodeJob(actualJob)
	if err != nil {
		log.Error(fmt.Sprintf("invalid job type in pushJobWithParent: %T", actualJob))
		return fmt.Errorf("invalid job type in pushJobWithParent: %w", err)
	}

	jsonJob.ParentID = &parentID

	if jsonJob.ID == "" {
		jsonJob.ID = uuid.New().String()
	}

	payload, err := json.Marshal(jsonJob)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	result, err := tx.ExecContext(ctx, q,
		jsonJob.ID,
		parentID,
		jsonJob.Priority,
		jobType,
		payload,
		time.Now().UTC(),
		statusNew,
	)

	if err != nil {
		return fmt.Errorf("failed to insert job: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		_, err = tx.ExecContext(ctx, `UPDATE gmaps_jobs SET child_jobs_failed = child_jobs_failed + 1 WHERE id = $1`, parentID)
		if err != nil {
			return fmt.Errorf("failed to increment failed counter: %w", err)
		}
	}

	return nil
}
