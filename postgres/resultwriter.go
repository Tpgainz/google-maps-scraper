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
	entry      *gmaps.Entry
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

func (r *resultWriter) Run(ctx context.Context, in <-chan scrapemate.Result) error {
	const maxBatchSize = 50

	log := scrapemate.GetLoggerFromContext(ctx)

	buff := make([]entryWithType, 0, 50)
	lastSave := time.Now().UTC()
	ticker := time.NewTicker(time.Second * 10) // Add a ticker to periodically save entries
	defer ticker.Stop()

	for {
		select {
		case result, ok := <-in:
			if !ok {
				// Channel closed, save any remaining entries
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

			// Déterminer le payload_type avec une valeur par défaut
			payloadType := "place" // Valeur par défaut
			
			// Si le job est disponible dans le résultat, on peut extraire son type
			if result.Job != nil {
				// Essayer de déterminer le type à partir du job
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
			var actualJob scrapemate.IJob = result.Job // Start with the job as is

			// Check if the job is wrapped
			if wrapper, ok := result.Job.(*jobWrapper); ok {
				actualJob = wrapper.IJob // Use the embedded job
			}

			// Now perform the type assertion on the actual job
			if job, ok := actualJob.(*gmaps.GmapJob); ok {
				userID = job.OwnerID
				log.Info(fmt.Sprintf("OwnerID for GmapJob with job ID %s", job.ID))
			} else if job, ok := actualJob.(*gmaps.PlaceJob); ok {
				userID = job.OwnerID
				log.Info(fmt.Sprintf("OwnerID for PlaceJob with job ID %s", job.ID))
			} else if job, ok := actualJob.(*gmaps.SocieteJob); ok {
				userID = job.OwnerID
				log.Info(fmt.Sprintf("OwnerID for SocieteJob with job ID %s", job.ID))
			} else {
				log.Info(fmt.Sprintf("Unknown actual job type %T, cannot extract OwnerID", actualJob))
			}

			buff = append(buff, entryWithType{entry: entry, payloadType: payloadType, userID: userID})

			if len(buff) >= maxBatchSize {
				err := r.batchSave(ctx, buff)
				if err != nil {
					return err
				}

				buff = buff[:0]
				lastSave = time.Now().UTC()
			}
		case <-ticker.C:
			// Save any pending entries every tick interval
			if len(buff) > 0 && time.Since(lastSave) >= time.Second*5 {
				if err := r.batchSave(ctx, buff); err != nil {
					return err
				}
				buff = buff[:0]
				lastSave = time.Now().UTC()
			}
		case <-ctx.Done():
			// Context was cancelled, save any remaining entries
			if len(buff) > 0 {
				// Use background context for final save attempt
				saveCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				defer cancel()
				_ = r.batchSave(saveCtx, buff) // Best effort save
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
	q += " ON CONFLICT DO NOTHING"

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
