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
	ctx = context.WithValue(ctx, providerKey{}, w.provider)
	ctx = context.WithValue(ctx, gmaps.CompanyDataCheckerKey{}, w.provider)

	data, nextJobs, err := w.IJob.Process(ctx, resp)

	if err != nil {
		_ = w.provider.statusManager.MarkFailed(ctx, w.IJob)
		return data, nil, err
	}

	// Handle enrichment jobs (email, company, pappers) - fire-and-forget
	if isEnrichmentJob(w.IJob) {
		_ = w.provider.statusManager.MarkEnrichmentDone(ctx, w.IJob)

		// Direct UPDATE on results table based on result type
		switch result := data.(type) {
		case *gmaps.EmailEnrichmentResult:
			go w.provider.updateResultEmails(context.Background(), result)
		case *gmaps.CompanyEnrichmentResult:
			go w.provider.updateResultCompanyData(context.Background(), result)
			// If CompanyJob produced PappersJob(s), push them
			if companyJob, ok := w.IJob.(*gmaps.CompanyJob); ok && len(companyJob.EnrichmentJobs) > 0 {
				go w.provider.pushEnrichmentJobs(context.Background(), companyJob.EnrichmentJobs)
			}
		case *gmaps.PappersEnrichmentResult:
			go w.provider.updateResultPappers(context.Background(), result)
		}

		return data, nil, nil
	}

	// Handle PlaceJob: check duplicate, copy enrichment data, push enrichment jobs
	if placeJob, ok := w.IJob.(*gmaps.PlaceJob); ok {
		entry, isEntry := data.(*gmaps.Entry)

		// Check if this place already exists for this user/org
		if isEntry && entry != nil {
			isDup := w.provider.checkDuplicatePlace(ctx, entry.Link, placeJob.OwnerID, placeJob.OrganizationID)
			if isDup {
				_ = w.provider.statusManager.MarkFailed(ctx, w.IJob)
				return nil, nil, nil
			}

			// Check if enrichment data already exists from another user/org
			if placeJob.ExtractEmail || placeJob.ExtractBodacc {
				existing := w.provider.findExistingEnrichmentData(ctx, entry.Title, entry.Address)
				if existing != nil {
					if len(existing.Emails) > 0 && len(entry.Emails) == 0 {
						entry.Emails = existing.Emails
					}
					if existing.SocieteSiren != "" && entry.SocieteSiren == "" {
						entry.SocieteDirigeants = existing.SocieteDirigeants
						entry.SocieteSiren = existing.SocieteSiren
						entry.SocieteForme = existing.SocieteForme
						entry.SocieteCreation = existing.SocieteCreation
						entry.SocieteCloture = existing.SocieteCloture
						entry.SocieteLink = existing.SocieteLink
						entry.SocieteDiffusion = existing.SocieteDiffusion
					}
					// Skip enrichment jobs since we already have the data
					placeJob.EnrichmentJobs = nil
				}
			}
		}

		if err := w.provider.statusManager.MarkDone(ctx, w.IJob, 0); err != nil {
			return data, nil, err
		}
		if len(placeJob.EnrichmentJobs) > 0 {
			go w.provider.pushEnrichmentJobs(context.Background(), placeJob.EnrichmentJobs)
		}
		return data, nil, nil
	}

	log := scrapemate.GetLoggerFromContext(ctx)

	// Handle GmapJob (search): push PlaceJobs to DB, don't return them to scrapemate
	if gmapJob, ok := w.IJob.(*gmaps.GmapJob); ok {
		if len(nextJobs) > 0 {
			if err := w.provider.pushChildJobs(ctx, w.IJob, nextJobs); err != nil {
				log.Error(fmt.Sprintf("jobWrapper.Process: Error pushing child jobs: %v", err))
				return data, nil, fmt.Errorf("while pushing jobs: %w", err)
			}
		}
		if err := w.provider.statusManager.MarkDone(ctx, w.IJob, len(nextJobs)); err != nil {
			return data, nil, err
		}
		w.provider.apiClient.CallRevalidationAPI(ctx, gmapJob.OwnerID)
		return data, nil, nil
	}

	// Default: any other job type
	if len(nextJobs) > 0 {
		if err := w.provider.pushChildJobs(ctx, w.IJob, nextJobs); err != nil {
			log.Error(fmt.Sprintf("jobWrapper.Process: Error pushing child jobs: %v", err))
			return data, nil, fmt.Errorf("while pushing jobs: %w", err)
		}
	}
	if err := w.provider.statusManager.MarkDone(ctx, w.IJob, len(nextJobs)); err != nil {
		return data, nil, err
	}

	return data, nil, nil
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

// pushJobWithParent inserts a job with a parent reference.
func (p *provider) pushJobWithParent(ctx context.Context, tx *sql.Tx, job scrapemate.IJob, parentID string) error {
	q := `INSERT INTO gmaps_jobs
		(id, parent_id, priority, payload_type, payload, created_at, status)
		VALUES
		($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING`

	actualJob := job
	if wrapper, ok := job.(*jobWrapper); ok {
		actualJob = wrapper.IJob
	}

	jsonJob, jobType, err := p.codecRegistry.EncodeJob(actualJob)
	if err != nil {
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

	_, err = tx.ExecContext(ctx, q,
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

	return nil
}
