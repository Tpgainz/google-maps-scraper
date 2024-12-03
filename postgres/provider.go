package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gosom/scrapemate"

	"github.com/gosom/google-maps-scraper/gmaps"
)

const (
	statusNew    = "new"
	statusQueued = "queued"
)

var _ scrapemate.JobProvider = (*provider)(nil)

type JSONJob struct {
    ID         string                 `json:"id"`
    Priority   int                    `json:"priority"`
    URL        string                 `json:"url"`
    URLParams  map[string]string      `json:"url_params"`
    MaxRetries int                    `json:"max_retries"`
    JobType    string                 `json:"job_type"`    // "search" ou "place"
    Metadata   map[string]interface{} `json:"metadata"`    // données spécifiques au type
}

type provider struct {
	db      *sql.DB
	mu      *sync.Mutex
	jobc    chan scrapemate.IJob
	errc    chan error
	started bool
}

func NewProvider(db *sql.DB) scrapemate.JobProvider {
	prov := provider{
		db:   db,
		mu:   &sync.Mutex{},
		errc: make(chan error, 1),
		jobc: make(chan scrapemate.IJob, 100),
	}

	return &prov
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

				select {
				case outc <- job:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return outc, errc
}

// Push pushes a job to the job provider
func (p *provider) Push(ctx context.Context, job scrapemate.IJob) error {
    q := `INSERT INTO gmaps_jobs
        (id, priority, payload_type, payload, created_at, status)
        VALUES
        ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING`

    jsonJob := &JSONJob{
        ID:         job.GetID(),
        Priority:   job.GetPriority(),
        URL:        job.GetURL(),
        URLParams:  job.GetURLParams(),
        MaxRetries: job.GetMaxRetries(),
    }

    switch j := job.(type) {
    case *gmaps.GmapJob:
        jsonJob.JobType = "search"
        jsonJob.Metadata = map[string]interface{}{
            "max_depth":     j.MaxDepth,
            "lang_code":     j.LangCode,
            "extract_email": j.ExtractEmail,
        }
    case *gmaps.PlaceJob:
        jsonJob.JobType = "place"
        jsonJob.Metadata = map[string]interface{}{
            "usage_in_results": j.UsageInResultststs,
            "extract_email":    j.ExtractEmail,
        }	
    default:
        return errors.New("invalid job type")
    }

    payload, err := json.Marshal(jsonJob)
    if err != nil {
        return fmt.Errorf("failed to marshal job: %w", err)
    }

    _, err = p.db.ExecContext(ctx, q,
        job.GetID(), job.GetPriority(), jsonJob.JobType, payload, time.Now().UTC(), statusNew,
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
        
        job := &gmaps.GmapJob{
            Job: scrapemate.Job{
                ID:         jsonJob.ID,
                URL:        jsonJob.URL,
                URLParams:  jsonJob.URLParams,
                MaxRetries: jsonJob.MaxRetries,
                Priority:   jsonJob.Priority,
            },
            MaxDepth:     maxDepth,
            LangCode:     jsonJob.Metadata["lang_code"].(string),
            ExtractEmail: jsonJob.Metadata["extract_email"].(bool),
        }
        return job, nil
    case "place":
        job := &gmaps.PlaceJob{
            Job: scrapemate.Job{
                ID:         jsonJob.ID,
                URL:        jsonJob.URL,
                URLParams:  jsonJob.URLParams,
                MaxRetries: jsonJob.MaxRetries,
                Priority:   jsonJob.Priority,
            },
            UsageInResultststs: jsonJob.Metadata["usage_in_results"].(bool),
            ExtractEmail:       jsonJob.Metadata["extract_email"].(bool),
        }
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
