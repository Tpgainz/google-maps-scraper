package gmaps

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/gosom/google-maps-scraper/bodacc"
	"github.com/gosom/google-maps-scraper/exiter"
	"github.com/gosom/scrapemate"
	"github.com/playwright-community/playwright-go"
)

type BodaccJobOptions func(*BodaccJob)

type BodaccJob struct {
	scrapemate.Job
	OwnerID        string
	OrganizationID string
	CompanyName    string
	Address        string
	Entry          *Entry
	ExitMonitor    exiter.Exiter
}

func NewBodaccJob(companyName, address, ownerID, organizationID string, entry *Entry, opts ...BodaccJobOptions) *BodaccJob {
	const (
		defaultPrio       = scrapemate.PriorityHigh
		defaultMaxRetries = 2
	)

	job := BodaccJob{
		Job: scrapemate.Job{
			ID:         uuid.New().String(),
			Method:     http.MethodGet,
			URL:        "", // BODACC service doesn't need a URL
			MaxRetries: defaultMaxRetries,
			Priority:   defaultPrio,
		},
		CompanyName:    companyName,
		Address:        address,
		OwnerID:        ownerID,
		OrganizationID: organizationID,
		Entry:          entry,
	}

	for _, opt := range opts {
		opt(&job)
	}

	return &job
}

func WithBodaccJobParentID(parentID string) BodaccJobOptions {
	return func(j *BodaccJob) {
		j.ParentID = parentID
	}
}

func WithBodaccJobPriority(priority int) BodaccJobOptions {
	return func(j *BodaccJob) {
		j.Priority = priority
	}
}

func WithBodaccJobExitMonitor(exitMonitor exiter.Exiter) BodaccJobOptions {
	return func(j *BodaccJob) {
		j.ExitMonitor = exitMonitor
	}
}

func (j *BodaccJob) Process(ctx context.Context, resp *scrapemate.Response) (any, []scrapemate.IJob, error) {
	defer func() {
		resp.Document = nil
		resp.Body = nil
		resp.Meta = nil
	}()

	logr := scrapemate.GetLoggerFromContext(ctx)

	bodaccService := bodacc.NewBodaccService()
	result, err := bodaccService.SearchCompany(j.CompanyName, j.Address)
	if err != nil {
		logr.Info(fmt.Sprintf("BODACC search failed for %s: %v", j.CompanyName, err))
		return j.Entry, nil, nil
	}

	if !result.Success {
		logr.Info(fmt.Sprintf("BODACC search unsuccessful for %s: %s", j.CompanyName, result.Error))
		return j.Entry, nil, nil
	}

	if len(result.Data) == 0 {
		logr.Info(fmt.Sprintf("No BODACC data found for: %s", j.CompanyName))
		return j.Entry, nil, nil
	}

	company := result.Data[0]
	j.Entry.SocieteDirigeants = company.SocieteDirigeants
	j.Entry.SocieteForme = company.SocieteForme
	j.Entry.SocieteCreation = company.SocieteCreation
	j.Entry.SocieteCloture = company.SocieteCloture
	j.Entry.SocieteSiren = company.SocieteSiren
	j.Entry.SocieteLink = company.SocieteLink
	j.Entry.PappersURL = company.PappersURL

	logr.Info(fmt.Sprintf("Updated entry %s with BODACC data: SIREN=%s, Directors=%v", 
		j.Entry.Title, company.SocieteSiren, company.SocieteDirigeants))

	if len(company.SocieteDirigeants) == 0 && company.PappersURL != "" {
		logr.Info(fmt.Sprintf("No directors found in BODACC for %s, creating Pappers scraping job: %s", 
			j.CompanyName, company.PappersURL))

		var childJobs []scrapemate.IJob
		opts := []PappersJobOptions{WithPappersJobParentID(j.ID)}
		if j.ExitMonitor != nil {
			opts = append(opts, WithPappersJobExitMonitor(j.ExitMonitor))
		}

		pappersJob := NewPappersJob(company.PappersURL, j.Entry, j.OwnerID, j.OrganizationID, opts...)
		childJobs = append(childJobs, pappersJob)

		return nil, childJobs, nil
	}

	if j.ExitMonitor != nil {
		j.ExitMonitor.IncrPlacesCompleted(1)
	}

	return j.Entry, nil, nil
}

func (j *BodaccJob) UseInResults() bool {
	return true
}

func (j *BodaccJob) BrowserActions(ctx context.Context, page playwright.Page) scrapemate.Response {
	var resp scrapemate.Response
	resp.URL = "bodacc://api"
	resp.StatusCode = 200
	return resp
}

