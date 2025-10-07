package bodacc

import (
	"context"
	"log"

	"github.com/gosom/scrapemate"
)

type DirectorsWriter struct {
	directors []string
}

func NewDirectorsWriter() *DirectorsWriter {
	return &DirectorsWriter{
		directors: make([]string, 0),
	}
}

func (w *DirectorsWriter) Run(ctx context.Context, in <-chan scrapemate.Result) error {
	for result := range in {
		if pappersResult, ok := result.Data.(*PappersScrapingResult); ok {
			w.directors = append(w.directors, pappersResult.Directors...)
			log.Printf("Captured directors: %v", pappersResult.Directors)
		}
	}
	return nil
}

func (w *DirectorsWriter) GetDirectors() []string {
	return w.directors
}
