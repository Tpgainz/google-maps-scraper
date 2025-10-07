package gmaps

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/google/uuid"
	"github.com/gosom/google-maps-scraper/bodacc"
	"github.com/gosom/google-maps-scraper/exiter"
	"github.com/gosom/scrapemate"
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

	// Use the BODACC service to search for company information
	bodaccService := bodacc.NewBodaccService()
	result, err := bodaccService.SearchCompany(j.CompanyName, j.Address)
	if err != nil {
		return nil, nil, fmt.Errorf("BODACC search failed: %w", err)
	}

	if !result.Success {
		return nil, nil, fmt.Errorf("BODACC search failed: %s", result.Error)
	}

	// Update the entry with BODACC data if found
	if len(result.Data) > 0 {
		company := result.Data[0]
		j.Entry.SocieteDirigeants = company.SocieteDirigeants
		j.Entry.SocieteForme = company.SocieteForme
		j.Entry.SocieteCreation = company.SocieteCreation
		j.Entry.SocieteCloture = company.SocieteCloture
		j.Entry.SocieteSiren = company.SocieteSiren
		j.Entry.SocieteLink = company.SocieteLink
		j.Entry.PappersURL = company.PappersURL

		log.Printf("Updated entry %s with BODACC data: SIREN=%s, Directors=%v", 
			j.Entry.ID, company.SocieteSiren, company.SocieteDirigeants)
	}

	// Return the updated entry
	return j.Entry, nil, nil
}

func (j *BodaccJob) UseInResults() bool {
	return true
}

