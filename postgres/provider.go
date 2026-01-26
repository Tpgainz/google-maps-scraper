package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gosom/scrapemate"

	"github.com/google/uuid"
	"github.com/gosom/google-maps-scraper/entreprise"
	"github.com/gosom/google-maps-scraper/gmaps"
)

const (
	statusNew        = "new"
	statusQueued     = "queued"
	statusProcessing = "processing"
	statusDone       = "done"
	statusFailed     = "failed"
)

var _ scrapemate.JobProvider = (*provider)(nil)

// JSONJob represents a job in JSON format for storage.
type JSONJob struct {
	ID         string                 `json:"id"`
	Priority   int                    `json:"priority"`
	URL        string                 `json:"url"`
	URLParams  map[string]string      `json:"url_params"`
	MaxRetries int                    `json:"max_retries"`
	JobType    string                 `json:"job_type"`
	Metadata   map[string]interface{} `json:"metadata"`
	ParentID   *string                `json:"parent_id,omitempty"`
}

type provider struct {
	db            *sql.DB
	mu            *sync.Mutex
	jobc          chan scrapemate.IJob
	errc          chan error
	started       bool
	apiClient     *APIClient
	statusManager *StatusManager
	codecRegistry *CodecRegistry
}

type providerKey struct{}

var _ gmaps.CompanyDataChecker = (*provider)(nil)

// CheckCompanyDataExists checks if company data exists in the database.
func (p *provider) CheckCompanyDataExists(ctx context.Context, title, address, ownerID, organizationID string) (*entreprise.CompanyInfo, bool, error) {
	query := NewCompanyDataQuery(title, address, ownerID, organizationID)
	q, args, ok := query.Build()
	if !ok {
		return nil, false, nil
	}

	var societeDirigeants, societeSiren, societeForme, societeCreation, societeCloture, societeLink sql.NullString
	var societeDiffusion sql.NullBool
	err := p.db.QueryRowContext(ctx, q, args...).Scan(
		&societeDirigeants, &societeSiren, &societeForme,
		&societeCreation, &societeCloture, &societeLink, &societeDiffusion,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to check BODACC data: %w", err)
	}

	data := &entreprise.CompanyInfo{}
	if societeDirigeants.Valid && societeDirigeants.String != "" {
		data.SocieteDirigeants = strings.Split(societeDirigeants.String, ",")
		for i := range data.SocieteDirigeants {
			data.SocieteDirigeants[i] = strings.TrimSpace(data.SocieteDirigeants[i])
		}
	}
	if societeSiren.Valid {
		data.SocieteSiren = societeSiren.String
	}
	if societeForme.Valid {
		data.SocieteForme = societeForme.String
	}
	if societeCreation.Valid {
		data.SocieteCreation = societeCreation.String
	}
	if societeCloture.Valid {
		data.SocieteCloture = societeCloture.String
	}
	if societeLink.Valid {
		data.SocieteLink = societeLink.String
	}
	if societeDiffusion.Valid {
		data.SocieteDiffusion = societeDiffusion.Bool
	}

	return data, true, nil
}

// NewProvider creates a new JobProvider backed by PostgreSQL.
func NewProvider(db *sql.DB, revalidationAPIURL, jobCompletionAPIURL string) scrapemate.JobProvider {
	apiClient := NewAPIClient(revalidationAPIURL, jobCompletionAPIURL)
	codecRegistry := NewCodecRegistry()

	prov := provider{
		db:            db,
		mu:            &sync.Mutex{},
		errc:          make(chan error, 1),
		jobc:          make(chan scrapemate.IJob, 100),
		apiClient:     apiClient,
		statusManager: NewStatusManager(db, apiClient),
		codecRegistry: codecRegistry,
	}

	return &prov
}

// Jobs returns channels for jobs and errors.
//
//nolint:gocritic // it contains about unnamed results
func (p *provider) Jobs(ctx context.Context) (<-chan scrapemate.IJob, <-chan error) {
	outc := make(chan scrapemate.IJob)
	errc := make(chan error, 1)

	p.mu.Lock()
	if !p.started {
		go p.fetchJobs(ctx)
		p.started = true
	}
	p.mu.Unlock()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-p.errc:
				errc <- err
				return
			case job, ok := <-p.jobc:
				if !ok {
					return
				}

				wrappedJob := &jobWrapper{
					IJob:     job,
					provider: p,
				}

				select {
				case outc <- wrappedJob:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return outc, errc
}

// Push inserts a job into the database.
func (p *provider) Push(ctx context.Context, job scrapemate.IJob) error {
	q := `INSERT INTO gmaps_jobs
		(id, parent_id, priority, payload_type, payload, created_at, status)
		VALUES
		($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING`

	log := scrapemate.GetLoggerFromContext(ctx)

	jsonJob, jobType, err := p.codecRegistry.EncodeJob(job)
	if err != nil {
		log.Error(fmt.Sprintf("invalid job type in Push: %T", job))
		return fmt.Errorf("invalid job type: %w", err)
	}

	// Extract parentID from the job
	var parentID *string
	actualJob := job
	if wrapper, ok := job.(*jobWrapper); ok {
		actualJob = wrapper.IJob
	}

	switch j := actualJob.(type) {
	case *gmaps.GmapJob:
		if j.ParentID != "" {
			parentID = &j.ParentID
		}
	case *gmaps.PlaceJob:
		if j.ParentID != "" {
			parentID = &j.ParentID
		}
	case *gmaps.EmailExtractJob:
		if j.ParentID != "" {
			parentID = &j.ParentID
		}
	case *gmaps.CompanyJob:
		if j.ParentID != "" {
			parentID = &j.ParentID
		}
	case *gmaps.PappersJob:
		if j.ParentID != "" {
			parentID = &j.ParentID
		}
	}

	if jsonJob.ID == "" {
		jsonJob.ID = uuid.New().String()
	}

	payload, err := json.Marshal(jsonJob)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	_, err = p.db.ExecContext(ctx, q,
		jsonJob.ID,
		parentID,
		jsonJob.Priority,
		jobType,
		payload,
		time.Now().UTC(),
		statusNew,
	)

	return err
}

// fetchJobs fetches jobs from the database and sends them to the job channel.
func (p *provider) fetchJobs(ctx context.Context) {
	defer close(p.jobc)
	defer close(p.errc)

	q := `
	WITH updated AS (
		UPDATE gmaps_jobs
		SET status = $1
		WHERE id IN (
			SELECT id from gmaps_jobs
			WHERE status = $2
			ORDER BY priority ASC, created_at ASC FOR UPDATE SKIP LOCKED
		LIMIT 50
		)
		RETURNING *
	)
	SELECT payload_type, payload from updated ORDER by priority ASC, created_at ASC
	`

	baseDelay := time.Second
	maxDelay := time.Minute
	factor := 2
	currentDelay := baseDelay

	jobs := make([]scrapemate.IJob, 0, 50)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rows, err := p.db.QueryContext(ctx, q, statusQueued, statusNew)
		if err != nil {
			p.errc <- err
			return
		}

		for rows.Next() {
			var (
				payloadType string
				payload     []byte
			)

			if err := rows.Scan(&payloadType, &payload); err != nil {
				p.errc <- err
				return
			}

			job, err := p.codecRegistry.DecodeJob(payloadType, payload)
			if err != nil {
				p.errc <- err
				return
			}

			jobs = append(jobs, job)
		}

		if err := rows.Err(); err != nil {
			p.errc <- err
			return
		}

		if err := rows.Close(); err != nil {
			p.errc <- err
			return
		}

		if len(jobs) > 0 {
			for _, job := range jobs {
				select {
				case p.jobc <- job:
				case <-ctx.Done():
					return
				}
			}

			jobs = jobs[:0]
			currentDelay = baseDelay
		} else {
			select {
			case <-time.After(currentDelay):
				currentDelay = time.Duration(float64(currentDelay) * float64(factor))
				if currentDelay > maxDelay {
					currentDelay = maxDelay
				}
			case <-ctx.Done():
				return
			}
		}
	}
}
