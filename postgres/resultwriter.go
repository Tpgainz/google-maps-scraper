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

	"github.com/tpgainz/google-maps-scraper/gmaps"
)

type entryWithType struct {
	entry      *gmaps.Entry
	payloadType string
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

	buff := make([]entryWithType, 0, 50)
	lastSave := time.Now().UTC()

	for result := range in {
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

		buff = append(buff, entryWithType{entry: entry, payloadType: payloadType})

		if len(buff) >= maxBatchSize || time.Now().UTC().Sub(lastSave) >= time.Minute {
			err := r.batchSave(ctx, buff)
			if err != nil {
				return err
			}

			buff = buff[:0]
			lastSave = time.Now().UTC()
		}
	}

	if len(buff) > 0 {
		err := r.batchSave(ctx, buff)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *resultWriter) batchSave(ctx context.Context, entries []entryWithType) error {
	if len(entries) == 0 {
		return nil
	}

	q := `INSERT INTO results
		(data, payload_type)
		VALUES
		`
	elements := make([]string, 0, len(entries))
	args := make([]interface{}, 0, len(entries)*2)

	for i, item := range entries {
		data, err := json.Marshal(item.entry)
		if err != nil {
			return err
		}

		elements = append(elements, fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2))
		args = append(args, data, item.payloadType)
	}

	q += strings.Join(elements, ", ")
	q += " ON CONFLICT DO NOTHING"

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

	return err
}
