package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gosom/scrapemate"

	"github.com/gosom/google-maps-scraper/gmaps"
)

type dbEntry struct {
	UserID              string
	OrganizationID      string
	ParentID            string
	Link                string
	PayloadType         string
	Title               string
	Category            string
	Address             string
	Website             string
	Phone               string
	Emails              []string
	SocieteDirigeant    string
	SocieteDirigeantLink string
	SocieteForme        string
	SocieteEffectif     string
	SocieteCreation     string
	SocieteCloture      string
	SocieteLink         string
}

func NewResultWriter(db *sql.DB, revalidationAPIURL string) scrapemate.ResultWriter {
	return &resultWriter{
		db:                 db,
		revalidationAPIURL: revalidationAPIURL,
		httpClient:         &http.Client{Timeout: 10 * time.Second},
	}
}

type resultWriter struct {
	db                 *sql.DB
	revalidationAPIURL string
	httpClient         *http.Client
}

func (r *resultWriter) checkDuplicateURL(ctx context.Context, url, userID, organizationID string) (bool, error) {
	if url == "" {
		return false, nil
	}

	var q string
	var args []interface{}

	if userID != "" && organizationID != "" {
		q = `SELECT COUNT(*) FROM results 
		WHERE link = $1 AND (user_id = $2 OR organization_id = $3)`
		args = []interface{}{url, userID, organizationID}
	} else if userID != "" {
		q = `SELECT COUNT(*) FROM results 
		WHERE link = $1 AND user_id = $2`
		args = []interface{}{url, userID}
	} else if organizationID != "" {
		q = `SELECT COUNT(*) FROM results 
		WHERE link = $1 AND organization_id = $2`
		args = []interface{}{url, organizationID}
	} else {
		return false, nil
	}

	var count int
	err := r.db.QueryRowContext(ctx, q, args...).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check duplicate URL: %w", err)
	}

	return count > 0, nil
}

func (r *resultWriter) getParentJobID(ctx context.Context, jobID string) (string, error) {
	var parentID sql.NullString
	q := `SELECT parent_id FROM gmaps_jobs WHERE id = $1`
	err := r.db.QueryRowContext(ctx, q, jobID).Scan(&parentID)
	
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("failed to get parent job ID: %w", err)
	}
	
	if parentID.Valid {
		return parentID.String, nil
	}
	
	return "", nil
}

func (r *resultWriter) getRootParentJobID(ctx context.Context, jobID string) (string, error) {
	currentJobID := jobID
	visitedJobs := make(map[string]bool)
	
	for {
		if visitedJobs[currentJobID] {
			return "", fmt.Errorf("circular reference detected in job hierarchy")
		}
		visitedJobs[currentJobID] = true
		
		parentID, err := r.getParentJobID(ctx, currentJobID)
		if err != nil {
			return "", err
		}
		
		if parentID == "" {
			return currentJobID, nil
		}
		
		currentJobID = parentID
	}
}

func (r *resultWriter) callRevalidationAPI(ctx context.Context, userID string) {
	if r.revalidationAPIURL == "" || userID == "" {
		return
	}

	payload := map[string]string{"userId": userID}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", r.revalidationAPIURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func (r *resultWriter) notifyRevalidation(ctx context.Context, entries []dbEntry) {
	if r.revalidationAPIURL == "" {
		return
	}

	// Extract unique user IDs
	userIDs := make(map[string]bool)
	for _, entry := range entries {
		if entry.UserID != "" {
			userIDs[entry.UserID] = true
		}
	}

	// Call revalidation API for each unique user ID
	for userID := range userIDs {
		go r.callRevalidationAPI(ctx, userID)
	}
}

func (r *resultWriter) Run(ctx context.Context, in <-chan scrapemate.Result) error {
	const maxBatchSize = 50

	log := scrapemate.GetLoggerFromContext(ctx)
	buff := make([]dbEntry, 0, 50)
	lastSave := time.Now().UTC()
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case result, ok := <-in:
			if !ok {
				if len(buff) > 0 {
					if err := r.batchSave(ctx, buff); err != nil {
						return err
					}
				}
				return nil
			}

			entry, ok := result.Data.(*gmaps.Entry)
			if !ok {
				return errors.New("invalid data type")
			}

			simpleEntry := entry.ToSimpleEntry()

			payloadType := "place"
			
			if result.Job != nil {
				switch result.Job.(type) {
				case *gmaps.GmapJob:
					payloadType = "search"
				case *gmaps.PlaceJob:
					payloadType = "place"
				case *gmaps.SocieteJob:
					payloadType = "societe"
				}
			}

			var userID string
			var organizationID string
			var parentJobID string
			var actualJob scrapemate.IJob = result.Job

			if wrapper, ok := result.Job.(*jobWrapper); ok {
				actualJob = wrapper.IJob
			}

			if job, ok := actualJob.(*gmaps.GmapJob); ok {
				userID = job.OwnerID
				organizationID = job.OrganizationID
				
				rootParentID, err := r.getRootParentJobID(ctx, job.GetID())
				if err != nil {
					log.Error(fmt.Sprintf("Error getting root parent job ID: %v", err))
					parentJobID = job.GetID()
				} else {
					parentJobID = rootParentID
				}
			} else if job, ok := actualJob.(*gmaps.PlaceJob); ok {
				userID = job.OwnerID
				organizationID = job.OrganizationID
				
				rootParentID, err := r.getRootParentJobID(ctx, job.GetID())
				if err != nil {
					log.Error(fmt.Sprintf("Error getting root parent job ID: %v", err))
					parentJobID = job.ParentID
				} else {
					parentJobID = rootParentID
				}
			} else if job, ok := actualJob.(*gmaps.EmailExtractJob); ok {
				userID = job.OwnerID
				organizationID = job.OrganizationID
				
				rootParentID, err := r.getRootParentJobID(ctx, job.GetID())
				if err != nil {
					log.Error(fmt.Sprintf("Error getting root parent job ID: %v", err))
					parentJobID = job.ParentID
				} else {
					parentJobID = rootParentID
				}
			} else if job, ok := actualJob.(*gmaps.SocieteJob); ok {
				userID = job.OwnerID
				organizationID = job.OrganizationID
				
				rootParentID, err := r.getRootParentJobID(ctx, job.GetID())
				if err != nil {
					log.Error(fmt.Sprintf("Error getting root parent job ID: %v", err))
					rootParentID = job.GetID()
				}
				parentJobID = rootParentID
			}

			isDuplicate, err := r.checkDuplicateURL(ctx, simpleEntry.Link, userID, organizationID)
			if err != nil {
				log.Error(fmt.Sprintf("Error checking duplicate URL: %v", err))
				continue
			}

			if isDuplicate {
				log.Info(fmt.Sprintf("Skipping duplicate URL %s for user %s", simpleEntry.Link, userID))
				continue
			}

			dbEntry := dbEntry{
				UserID:              userID,
				OrganizationID:      organizationID,
				ParentID:            parentJobID,
				Link:                simpleEntry.Link,
				PayloadType:         payloadType,
				Title:               simpleEntry.Title,
				Category:            simpleEntry.Category,
				Address:             simpleEntry.Address,
				Website:             simpleEntry.WebSite,
				Phone:               simpleEntry.Phone,
				Emails:              simpleEntry.Emails,
				SocieteDirigeant:    "",
				SocieteDirigeantLink: "",
				SocieteForme:        "",
				SocieteEffectif:     "",
				SocieteCreation:     "",
				SocieteCloture:      "",
				SocieteLink:         "",
			}

			buff = append(buff, dbEntry)

			if len(buff) >= maxBatchSize {
				err := r.batchSave(ctx, buff)
				if err != nil {
					return err
				}

				buff = buff[:0]
				lastSave = time.Now().UTC()
			}
		case <-ticker.C:
			if len(buff) > 0 && time.Since(lastSave) >= time.Second*5 {
				if err := r.batchSave(ctx, buff); err != nil {
					return err
				}
				buff = buff[:0]
				lastSave = time.Now().UTC()
			}
		case <-ctx.Done():
			if len(buff) > 0 {
				saveCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				defer cancel()
				_ = r.batchSave(saveCtx, buff)
			}
			return ctx.Err()
		}
	}
}

func (r *resultWriter) batchSave(ctx context.Context, entries []dbEntry) error {
	if len(entries) == 0 {
		return nil
	}

	log := scrapemate.GetLoggerFromContext(ctx)
	log.Info(fmt.Sprintf("Saving %d entries", len(entries)))

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO results (
			parent_id, user_id, organization_id, link, payload_type, 
			title, category, address, website, phone, emails,
			societe_dirigeant, societe_dirigeant_link, societe_forme, 
			societe_effectif, societe_creation, societe_cloture, societe_link
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 
			$12, $13, $14, $15, $16, $17, $18
		)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, entry := range entries {
		_, err := stmt.ExecContext(ctx,
			entry.ParentID, entry.UserID, entry.OrganizationID, entry.Link, entry.PayloadType,
			entry.Title, entry.Category, entry.Address, entry.Website, entry.Phone, entry.Emails,
			entry.SocieteDirigeant, entry.SocieteDirigeantLink, entry.SocieteForme,
			entry.SocieteEffectif, entry.SocieteCreation, entry.SocieteCloture, entry.SocieteLink,
		)
		if err != nil {
			return fmt.Errorf("failed to insert entry: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Info(fmt.Sprintf("Successfully saved %d entries", len(entries)))
	
	// Call revalidation API for unique user IDs
	r.notifyRevalidation(ctx, entries)
	
	return nil
}
