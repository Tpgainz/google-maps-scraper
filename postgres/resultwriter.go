package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gosom/scrapemate"

	"github.com/gosom/google-maps-scraper/gmaps"
)

type entryWithType struct {
	entry      *gmaps.SimpleEntry
	payloadType string
	userID      string
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
func (r *resultWriter) checkDuplicateURL(ctx context.Context, url, userID string) (bool, error) {
	if url == "" || userID == "" {
		return false, nil
	}

	const q = `SELECT COUNT(*) FROM results 
		WHERE data->>'link' = $1 AND user_id = $2`
	
	var count int
	err := r.db.QueryRowContext(ctx, q, url, userID).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check duplicate URL: %w", err)
	}

	return count > 0, nil
}

func (r *resultWriter) Run(ctx context.Context, in <-chan scrapemate.Result) error {
	const maxBatchSize = 50

	log := scrapemate.GetLoggerFromContext(ctx)

	buff := make([]entryWithType, 0, 50)
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
			var actualJob scrapemate.IJob = result.Job

			if wrapper, ok := result.Job.(*jobWrapper); ok {
				actualJob = wrapper.IJob
			}

			if job, ok := actualJob.(*gmaps.GmapJob); ok {
				userID = job.OwnerID
			} else if job, ok := actualJob.(*gmaps.PlaceJob); ok {
				userID = job.OwnerID
			} else if job, ok := actualJob.(*gmaps.SocieteJob); ok {
				userID = job.OwnerID
			}

			// Vérifier si cette URL existe déjà pour cet utilisateur
			isDuplicate, err := r.checkDuplicateURL(ctx, simpleEntry.Link, userID)
			if err != nil {
				log.Error(fmt.Sprintf("Error checking duplicate URL: %v", err))
				continue
			}

			if isDuplicate {
				log.Info(fmt.Sprintf("Skipping duplicate URL %s for user %s", simpleEntry.Link, userID))
				continue
			}

			buff = append(buff, entryWithType{entry: &simpleEntry, payloadType: payloadType, userID: userID})

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

func (r *resultWriter) batchSave(ctx context.Context, entries []entryWithType) error {
	if len(entries) == 0 {
		return nil
	}

	log := scrapemate.GetLoggerFromContext(ctx)

	log.Info(fmt.Sprintf("Saving %d entries", len(entries)))

	q := `INSERT INTO results
		(data, payload_type, user_id)
		VALUES
		`
	elements := make([]string, 0, len(entries))
	args := make([]interface{}, 0, len(entries)*3)

	for i, item := range entries {
		data, err := json.Marshal(item.entry)
		if err != nil {
			return err
		}

		elements = append(elements, fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3))
		args = append(args, data, item.payloadType, item.userID)
	}

	q += strings.Join(elements, ", ")

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

	return err
}
