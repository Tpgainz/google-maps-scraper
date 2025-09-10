package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gosom/scrapemate"

	"github.com/google/uuid"
	"github.com/gosom/google-maps-scraper/gmaps"
)

const (
	statusNew    = "new"
	statusQueued = "queued"
    statusProcessing = "processing" 
	statusDone   = "done"
	statusFailed = "failed"
)

var _ scrapemate.JobProvider = (*provider)(nil)

type JSONJob struct {
    ID         string                 `json:"id"`
    Priority   int                    `json:"priority"`
    URL        string                 `json:"url"`
    URLParams  map[string]string      `json:"url_params"`
    MaxRetries int                    `json:"max_retries"`
    JobType    string                 `json:"job_type"`   
    Metadata   map[string]interface{} `json:"metadata"`
    ParentID   *string               `json:"parent_id,omitempty"`
}

type provider struct {
	db                 *sql.DB
	mu                 *sync.Mutex
	jobc               chan scrapemate.IJob
	errc               chan error
	started            bool
	revalidationAPIURL string
	httpClient         *http.Client
}

func NewProvider(db *sql.DB, revalidationAPIURL string) scrapemate.JobProvider {
	prov := provider{
		db:                 db,
		mu:                 &sync.Mutex{},
		errc:               make(chan error, 1),
		jobc:               make(chan scrapemate.IJob, 100),
		revalidationAPIURL: revalidationAPIURL,
		httpClient:         &http.Client{Timeout: 10 * time.Second},
	}

	return &prov
}

type jobWrapper struct {
    scrapemate.IJob
    provider *provider
}

func (w *jobWrapper) Process(ctx context.Context, resp *scrapemate.Response) (any, []scrapemate.IJob, error) {
    data, nextJobs, err := w.IJob.Process(ctx, resp)
    
    if err == nil {
        if len(nextJobs) > 0 {
            if err := w.provider.pushChildJobs(ctx, w.IJob, nextJobs); err != nil {
                return data, nextJobs, err
            }
        }
        
        if err := w.provider.markJobDone(ctx, w.IJob, len(nextJobs)); err != nil {
            return data, nextJobs, err
        }
        
        if gmapJob, ok := w.IJob.(*gmaps.GmapJob); ok {
            w.provider.callRevalidationAPI(ctx, gmapJob.OwnerID)
        }
    } else {
        _ = w.provider.MarkFailed(ctx, w.IJob)
    }
    
    return data, nextJobs, err
}

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

func (p *provider) pushJobWithParent(ctx context.Context, tx *sql.Tx, job scrapemate.IJob, parentID string) error {
    q := `INSERT INTO gmaps_jobs
        (id, parent_id, priority, payload_type, payload, created_at, status)
        VALUES
        ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING`

    jsonJob := &JSONJob{
        ID:         job.GetID(),
        Priority:   job.GetPriority(),
        URL:        job.GetURL(),
        URLParams:  job.GetURLParams(),
        MaxRetries: job.GetMaxRetries(),
        ParentID:   &parentID,
    }

    switch j := job.(type) {
    case *gmaps.GmapJob:
        jsonJob.JobType = "search"
        jsonJob.Metadata = map[string]interface{}{
            "max_depth":     j.MaxDepth,
            "lang_code":     j.LangCode,
            "extract_email": j.ExtractEmail,
            "owner_id":       j.OwnerID,
            "organization_id": j.OrganizationID,
        }
    case *gmaps.PlaceJob:
        jsonJob.JobType = "place"
        jsonJob.Metadata = map[string]interface{}{
            "usage_in_results": j.UsageInResultststs,
            "extract_email":    j.ExtractEmail,
            "owner_id":          j.OwnerID,
            "organization_id": j.OrganizationID,
        }
    case *gmaps.EmailExtractJob:
        jsonJob.JobType = "email"
        jsonJob.Metadata = map[string]interface{}{
            "entry":     j.Entry,
            "parent_id": j.Job.ParentID,
            "owner_id": j.OwnerID,
            "organization_id": j.OrganizationID,
        }
    case *gmaps.SocieteJob:
        jsonJob.JobType = "societe"
        jsonJob.Metadata = map[string]interface{}{
            "extract_email": j.ExtractEmail,
            "owner_id":       j.OwnerID,
            "organization_id": j.OrganizationID,
        }
    default:
        return errors.New("invalid job type")
    }

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
        jsonJob.JobType,
        payload,
        time.Now().UTC(),
        statusNew,
    )

    return err
}

func (p *provider) markJobDone(ctx context.Context, job scrapemate.IJob, childJobsCreated int) error {
    tx, err := p.db.BeginTx(ctx, nil)
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
        
        if err := p.checkAndMarkParentDone(ctx, tx, job.GetID()); err != nil {
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

func (p *provider) checkAndMarkParentDone(ctx context.Context, tx *sql.Tx, jobID string) error {
    var parentID sql.NullString
    err := tx.QueryRowContext(ctx, `SELECT parent_id FROM gmaps_jobs WHERE id = $1`, jobID).Scan(&parentID)
    if err != nil || !parentID.Valid {
        return err
    }
    
    _, err = tx.ExecContext(ctx, `UPDATE gmaps_jobs SET child_jobs_completed = child_jobs_completed + 1 WHERE id = $1`, parentID.String)
    if err != nil {
        return err
    }
    
    var childCount, completedCount int
    err = tx.QueryRowContext(ctx, 
        `SELECT child_jobs_count, child_jobs_completed FROM gmaps_jobs WHERE id = $1`, 
        parentID.String).Scan(&childCount, &completedCount)
    if err != nil {
        return err
    }
    
    if completedCount >= childCount && childCount > 0 {
        _, err = tx.ExecContext(ctx, `UPDATE gmaps_jobs SET status = $1 WHERE id = $2`, statusDone, parentID.String)
        if err != nil {
            return err
        }
        
        return p.checkAndMarkParentDone(ctx, tx, parentID.String)
    }
    
    return nil
}

func (p *provider) callRevalidationAPI(ctx context.Context, userID string) {
	if p.revalidationAPIURL == "" || userID == "" {
		log := scrapemate.GetLoggerFromContext(ctx)
		if p.revalidationAPIURL == "" {
			log.Info(fmt.Sprintf("Skipping revalidation API call: revalidationAPIURL is empty (userID=%s)", userID))
		}
		if userID == "" {
			log.Info(fmt.Sprintf("Skipping revalidation API call: userID is empty (revalidationAPIURL=%s)", p.revalidationAPIURL))
		}
		return
	}

	payload := map[string]string{"userId": userID}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", p.revalidationAPIURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")

	log := scrapemate.GetLoggerFromContext(ctx)
	log.Info(fmt.Sprintf("Calling revalidation API: %s", p.revalidationAPIURL))

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	log.Info(fmt.Sprintf("Revalidation API response: %v", resp))
}

func (p *provider) MarkFailed(ctx context.Context, job scrapemate.IJob) error {
    tx, err := p.db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // Marquer le job comme failed
    q := `UPDATE gmaps_jobs SET status = $1 WHERE id = $2`
    log := scrapemate.GetLoggerFromContext(ctx)
    log.Info(fmt.Sprintf("Marking job %s as failed", job.GetID()))
    _, err = tx.ExecContext(ctx, q, statusFailed, job.GetID())
    if err != nil {
        return err
    }
    log.Info(fmt.Sprintf("Incrementing parent counter for job %s", job.GetID()))
    // Incrémenter le compteur du parent
    if err := p.checkAndMarkParentDone(ctx, tx, job.GetID()); err != nil {
        return err
    }

    return tx.Commit()
}

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

				// Wrap the job to handle marking it as done after processing
				wrappedJob := &jobWrapper{
					IJob:     job,
					provider: p,
				}

				select {
				case outc <- wrappedJob:
				case outc <- wrappedJob:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return outc, errc
}

// Modifier la méthode Push pour inclure parent_id
func (p *provider) Push(ctx context.Context, job scrapemate.IJob) error {
    q := `INSERT INTO gmaps_jobs
        (id, parent_id, priority, payload_type, payload, created_at, status)
        VALUES
        ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING`

    jsonJob := &JSONJob{
        ID:         job.GetID(),
        Priority:   job.GetPriority(),
        URL:        job.GetURL(),
        URLParams:  job.GetURLParams(),
        MaxRetries: job.GetMaxRetries(),
    }

    // Récupérer le parentID du job et les métadonnées
    var parentID *string
    switch j := job.(type) {
    case *gmaps.GmapJob:
        if j.ParentID != "" {
            parentID = &j.ParentID
        }
        jsonJob.JobType = "search"
        jsonJob.Metadata = map[string]interface{}{
            "max_depth":     j.MaxDepth,
            "lang_code":     j.LangCode,
            "extract_email": j.ExtractEmail,
            "owner_id":       j.OwnerID,
            "organization_id": j.OrganizationID,
        }
    case *gmaps.PlaceJob:
        if j.ParentID != "" {
            parentID = &j.ParentID
        }
        jsonJob.JobType = "place"
        jsonJob.Metadata = map[string]interface{}{
            "usage_in_results": j.UsageInResultststs,
            "extract_email":    j.ExtractEmail,
            "owner_id":          j.OwnerID,
            "organization_id": j.OrganizationID,
        }
    case *gmaps.EmailExtractJob:
        if j.ParentID != "" {
            parentID = &j.ParentID
        }
        jsonJob.JobType = "email"
        jsonJob.Metadata = map[string]interface{}{
            "entry":     j.Entry,
            "parent_id": j.Job.ParentID,
            "owner_id": j.OwnerID,
            "organization_id": j.OrganizationID,
        }
    case *gmaps.SocieteJob:
        if j.ParentID != "" {
            parentID = &j.ParentID
        }
        jsonJob.JobType = "societe"
        jsonJob.Metadata = map[string]interface{}{
            "extract_email": j.ExtractEmail,
            "owner_id":       j.OwnerID,
            "organization_id": j.OrganizationID,
        }
    default:
        return errors.New("invalid job type")
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
        jsonJob.JobType,
        payload,
        time.Now().UTC(),
        statusNew,
    )

    return err
}

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

			job, err := decodeJob(payloadType, payload)
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
		} else if len(jobs) == 0 {
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

func decodeJob(payloadType string, payload []byte) (scrapemate.IJob, error) {
    // If the payload is a string, we need to unmarshal it first
    var rawJSON string
    err := json.Unmarshal(payload, &rawJSON)
    if err == nil {
        // If it was a string, use the unmarshaled content
        payload = []byte(rawJSON)
    }
    
    var jsonJob JSONJob
    if err := json.Unmarshal(payload, &jsonJob); err != nil {
        return nil, fmt.Errorf("failed to unmarshal job: %w", err)
    }
    
    switch payloadType {
    case "search":
        maxDepth, err := getIntFromMetadata(jsonJob.Metadata, "max_depth")
        if err != nil {
            return nil, fmt.Errorf("failed to get max_depth: %w", err)
        }
        
        langCode, ok := jsonJob.Metadata["lang_code"].(string)
        if !ok {
            return nil, fmt.Errorf("lang_code is missing or not a string")
        }
        
        extractEmail, ok := jsonJob.Metadata["extract_email"].(bool)
        if !ok {
            return nil, fmt.Errorf("extract_email is missing or not a boolean")
        }
        
        ownerID, ok := jsonJob.Metadata["owner_id"].(string)
        if !ok {
            return nil, fmt.Errorf("owner_id is missing or not a string")
        }

        organizationID, ok := jsonJob.Metadata["organization_id"].(string)
        if !ok {
            return nil, fmt.Errorf("organization_id is not a string")
        }
        
        var parentID string
        if jsonJob.ParentID != nil {
            parentID = *jsonJob.ParentID
        }
        
        job := &gmaps.GmapJob{
            Job: scrapemate.Job{
                ID:         jsonJob.ID,
                ParentID:   parentID,
                URL:        jsonJob.URL,
                URLParams:  jsonJob.URLParams,
                MaxRetries: jsonJob.MaxRetries,
                Priority:   jsonJob.Priority,
            },
            MaxDepth:     maxDepth,
            LangCode:     langCode,
            ExtractEmail: extractEmail,
            OwnerID:       ownerID,
            OrganizationID: organizationID,
        }
        
        return job, nil
    case "place":
        usageInResults, ok := jsonJob.Metadata["usage_in_results"].(bool)
        if !ok {
            return nil, fmt.Errorf("usage_in_results is missing or not a boolean")
        }
        
        extractEmail, ok := jsonJob.Metadata["extract_email"].(bool)
        if !ok {
            return nil, fmt.Errorf("extract_email is missing or not a boolean")
        }
        
        ownerID, ok := jsonJob.Metadata["owner_id"].(string)
        if !ok {
            return nil, fmt.Errorf("owner_id is missing or not a string")
        }
        
        organizationID, ok := jsonJob.Metadata["organization_id"].(string)
        if !ok {
            return nil, fmt.Errorf("organization_id is not a string")
        }

        var parentID string
        if jsonJob.ParentID != nil {
            parentID = *jsonJob.ParentID
        }

        job := &gmaps.PlaceJob{
            Job: scrapemate.Job{
                ID:         jsonJob.ID,
                ParentID:   parentID,
                URL:        jsonJob.URL,
                URLParams:  jsonJob.URLParams,
                MaxRetries: jsonJob.MaxRetries,
                Priority:   jsonJob.Priority,
            },
            UsageInResultststs: usageInResults,
            ExtractEmail:       extractEmail,
            OwnerID:             ownerID,
            OrganizationID:      organizationID,
        }
        
        return job, nil
    case "societe":
        extractEmail, ok := jsonJob.Metadata["extract_email"].(bool)
        if !ok {
            return nil, fmt.Errorf("extract_email is missing or not a boolean")
        }
        
        ownerID, ok := jsonJob.Metadata["owner_id"].(string)
        if !ok {
            return nil, fmt.Errorf("owner_id is missing or not a string")
        }

        organizationID, ok := jsonJob.Metadata["organization_id"].(string)
        if !ok {
            return nil, fmt.Errorf("organization_id is not a string")
        }
        
        var parentID string
        if jsonJob.ParentID != nil {
            parentID = *jsonJob.ParentID
        }

        job := &gmaps.SocieteJob{
            Job: scrapemate.Job{
                ID:         jsonJob.ID,
                ParentID:   parentID,
                URL:        jsonJob.URL,
                URLParams:  jsonJob.URLParams,
                MaxRetries: jsonJob.MaxRetries,
                Priority:   jsonJob.Priority,
            },
            ExtractEmail: extractEmail,
            OwnerID:       ownerID,
            OrganizationID: organizationID,
        }
        return job, nil
    case "email":
        parentIDI, ok := jsonJob.Metadata["parent_id"].(string)
        if !ok {
            return nil, fmt.Errorf("parent_id is missing or not a string")
        }

        entryMap, ok := jsonJob.Metadata["entry"].(map[string]any)
        if !ok {
            return nil, fmt.Errorf("entry is missing or not an object")
        }

        entryBytes, err := json.Marshal(entryMap)
        if err != nil {
            return nil, fmt.Errorf("failed to marshal entry: %w", err)
        }

        var entry gmaps.Entry
        if err := json.Unmarshal(entryBytes, &entry); err != nil {
            return nil, fmt.Errorf("failed to unmarshal entry: %w", err)
        }

        ownerID, ok := jsonJob.Metadata["owner_id"].(string)
        if !ok {
            return nil, fmt.Errorf("owner_id is missing or not a string")
        }

        organizationID, ok := jsonJob.Metadata["organization_id"].(string)
        if !ok {
            return nil, fmt.Errorf("organization_id is missing or not a string")
        }

        var parentID string
        if jsonJob.ParentID != nil {
            parentID = *jsonJob.ParentID
        }

        job := gmaps.NewEmailJob(parentIDI, &entry, ownerID, organizationID)
        job.Job.ID = jsonJob.ID
        job.Job.ParentID = parentID
        job.Job.URL = jsonJob.URL
        job.Job.URLParams = jsonJob.URLParams
        job.Job.MaxRetries = jsonJob.MaxRetries
        job.Job.Priority = jsonJob.Priority
        job.OwnerID = ownerID
        job.OrganizationID = organizationID 

        return job, nil
    default:
        return nil, fmt.Errorf("invalid payload type: %s", payloadType)
    }
}

func getIntFromMetadata(metadata map[string]interface{}, key string) (int, error) {
    value, ok := metadata[key]
    if !ok {
        return 0, fmt.Errorf("missing key %s in metadata", key)
    }
    
    floatValue, ok := value.(float64)
    if !ok {
        return 0, fmt.Errorf("value for key %s is not a number", key)
    }
    
    return int(floatValue), nil
}

