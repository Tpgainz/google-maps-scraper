package gmaps

import (
	"context"
	"net/http"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/uuid"
	"github.com/gosom/google-maps-scraper/exiter"
	"github.com/gosom/scrapemate"
)

type PappersEnrichmentResult struct {
	PlaceLink         string
	OwnerID           string
	OrganizationID    string
	SocieteDirigeants []string
}

type PappersJobOptions func(*PappersJob)

type PappersJob struct {
	scrapemate.Job
	OwnerID        string
	OrganizationID string
	PlaceLink      string
	ExitMonitor    exiter.Exiter
}

func NewPappersJob(pappersURL string, placeLink, ownerID, organizationID string, opts ...PappersJobOptions) *PappersJob {
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
		PlaceLink:      placeLink,
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

	result := &PappersEnrichmentResult{
		PlaceLink:      j.PlaceLink,
		OwnerID:        j.OwnerID,
		OrganizationID: j.OrganizationID,
	}

	if resp.Error != nil || resp.Document == nil {
		return result, nil, nil
	}

	doc, ok := resp.Document.(*goquery.Document)
	if !ok {
		return result, nil, nil
	}

	result.SocieteDirigeants = j.extractDirectors(doc)

	return result, nil, nil
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
	return false
}

func (j *PappersJob) ProcessOnFetchError() bool {
	return true
}

