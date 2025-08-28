package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gosom/scrapemate"

	"github.com/gosom/google-maps-scraper/gmaps"
)

type dbEntry struct {
	UserID              string
	OrganizationID      string
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

func NewResultWriter(db *sql.DB) scrapemate.ResultWriter {
	return &resultWriter{
		db: db,
	}
}

type resultWriter struct {
	db *sql.DB
}

// checkDuplicateURL vérifie si une URL existe déjà pour un utilisateur donné
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
			var actualJob scrapemate.IJob = result.Job

			if wrapper, ok := result.Job.(*jobWrapper); ok {
				actualJob = wrapper.IJob
			}

			if job, ok := actualJob.(*gmaps.GmapJob); ok {
				userID = job.OwnerID
				organizationID = job.OrganizationID
			} else if job, ok := actualJob.(*gmaps.PlaceJob); ok {
				userID = job.OwnerID
				organizationID = job.OrganizationID
			} else if job, ok := actualJob.(*gmaps.EmailExtractJob); ok {
				userID = job.OwnerID
				organizationID = job.OrganizationID
			} else if job, ok := actualJob.(*gmaps.SocieteJob); ok {
				userID = job.OwnerID
				organizationID = job.OrganizationID
			}

			// Vérifier si cette URL existe déjà pour cet utilisateur
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

	q := `INSERT INTO results
		(user_id, organization_id, link, payload_type, title, category, address, website, phone, emails,
		 societe_dirigeant, societe_dirigeant_link, societe_forme, societe_effectif, 
		 societe_creation, societe_cloture, societe_link)
		VALUES
		`
	elements := make([]string, 0, len(entries))
	args := make([]interface{}, 0, len(entries)*17)

	for i, item := range entries {
		elements = append(elements, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)", 
			i*17+1, i*17+2, i*17+3, i*17+4, i*17+5, i*17+6, i*17+7, i*17+8, 
			i*17+9, i*17+10, i*17+11, i*17+12, i*17+13, i*17+14, i*17+15, i*17+16, i*17+17))
		args = append(args, 
			item.UserID, item.OrganizationID, item.Link, item.PayloadType, item.Title, item.Category, 
			item.Address, item.Website, item.Phone, item.Emails, item.SocieteDirigeant, 
			item.SocieteDirigeantLink, item.SocieteForme, item.SocieteEffectif, 
			item.SocieteCreation, item.SocieteCloture, item.SocieteLink)
	}

	q += strings.Join(elements, ", ")

	log.Info(fmt.Sprintf("Saving %d entries with query: %s", len(entries), q))

	log.Info(fmt.Sprintf("Saving %d entries with query: %s", len(entries), q))

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		_ = tx.Rollback()
	}()

	_, err = tx.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}

	err = tx.Commit()

	log.Info(fmt.Sprintf("Saved %d entries", len(entries)))

	log.Info(fmt.Sprintf("Saved %d entries", len(entries)))

	return err
}
