package gmaps

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/uuid"
	"github.com/gosom/google-maps-scraper/exiter"
	"github.com/gosom/scrapemate"
)

type PappersJobOptions func(*PappersJob)

type PappersJob struct {
	scrapemate.Job
	OwnerID        string
	OrganizationID string
	Entry          *Entry
	ExitMonitor    exiter.Exiter
}

func NewPappersJob(pappersURL string, entry *Entry, ownerID, organizationID string, opts ...PappersJobOptions) *PappersJob {
	const (
		defaultPrio       = scrapemate.PriorityHigh
		defaultMaxRetries = 2
	)

	job := PappersJob{
		Job: scrapemate.Job{
			ID:         uuid.New().String(),
			Method:     http.MethodGet,
			URL:        pappersURL,
			MaxRetries: defaultMaxRetries,
			Priority:   defaultPrio,
		},
		Entry:          entry,
		OwnerID:        ownerID,
		OrganizationID: organizationID,
	}

	for _, opt := range opts {
		opt(&job)
	}

	return &job
}

func WithPappersJobParentID(parentID string) PappersJobOptions {
	return func(j *PappersJob) {
		j.ParentID = parentID
	}
}

func WithPappersJobExitMonitor(exitMonitor exiter.Exiter) PappersJobOptions {
	return func(j *PappersJob) {
		j.ExitMonitor = exitMonitor
	}
}

func (j *PappersJob) Process(ctx context.Context, resp *scrapemate.Response) (any, []scrapemate.IJob, error) {
	defer func() {
		resp.Document = nil
		resp.Body = nil
		resp.Meta = nil
	}()

	defer func() {
		if j.ExitMonitor != nil {
			j.ExitMonitor.IncrPlacesCompleted(1)
		}
	}()

	log := scrapemate.GetLoggerFromContext(ctx)

	if resp.Error != nil {
		log.Info(fmt.Sprintf("Pappers scraping failed for %s: %v", j.Entry.Title, resp.Error))
		return j.Entry, nil, nil
	}

	if resp.Document == nil {
		log.Info(fmt.Sprintf("No document available for Pappers scraping: %s", j.Entry.Title))
		return j.Entry, nil, nil
	}

	doc, ok := resp.Document.(*goquery.Document)
	if !ok {
		log.Info(fmt.Sprintf("Could not convert document to goquery for: %s", j.Entry.Title))
		return j.Entry, nil, nil
	}

	directors := j.extractDirectors(doc)

	if len(directors) > 0 {
		j.Entry.SocieteDirigeants = directors
		log.Info(fmt.Sprintf("Scraped %d directors from Pappers for %s: %v", len(directors), j.Entry.Title, directors))
	} else {
		log.Info(fmt.Sprintf("No directors found on Pappers for: %s", j.Entry.Title))
	}

	return j.Entry, nil, nil
}

func (j *PappersJob) extractDirectors(doc *goquery.Document) []string {
	var directors []string

	doc.Find("td.info-dirigeant a.underline").Each(func(i int, s *goquery.Selection) {
		directorName := strings.TrimSpace(s.Text())
		if directorName != "" {
			directors = append(directors, directorName)
		}
	})

	return directors
}

func (j *PappersJob) UseInResults() bool {
	return true
}

func (j *PappersJob) ProcessOnFetchError() bool {
	return true
}

