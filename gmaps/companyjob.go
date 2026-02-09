package gmaps

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/gosom/google-maps-scraper/entreprise"
	"github.com/gosom/google-maps-scraper/exiter"
	"github.com/gosom/scrapemate"
	"github.com/playwright-community/playwright-go"
)

type CompanyDataChecker interface {
	CheckCompanyDataExists(ctx context.Context, title, address, ownerID, organizationID string) (*entreprise.CompanyInfo, bool, error)
}

type CompanyEnrichmentResult struct {
	PlaceLink         string
	OwnerID           string
	OrganizationID    string
	SocieteDirigeants []string
	SocieteSiren      string
	SocieteForme      string
	SocieteCreation   string
	SocieteCloture    string
	SocieteLink       string
	SocieteDiffusion  bool
	PappersURL        string
}

type CompanyJobOptions func(*CompanyJob)

type CompanyJob struct {
	scrapemate.Job
	OwnerID        string
	OrganizationID string
	CompanyName    string
	Address        string
	PlaceLink      string
	ExitMonitor    exiter.Exiter
	EnrichmentJobs []scrapemate.IJob `json:"-"`
}

func NewCompanyJob(companyName, address, ownerID, organizationID, placeLink string, opts ...CompanyJobOptions) *CompanyJob {
	const (
		defaultPrio       = scrapemate.PriorityHigh
		defaultMaxRetries = 2
	)

	job := CompanyJob{
		Job: scrapemate.Job{
			ID:         uuid.New().String(),
			Method:     http.MethodGet,
			URL:        "",
			MaxRetries: defaultMaxRetries,
			Priority:   defaultPrio,
		},
		CompanyName:    companyName,
		Address:        address,
		OwnerID:        ownerID,
		OrganizationID: organizationID,
		PlaceLink:      placeLink,
	}

	for _, opt := range opts {
		opt(&job)
	}

	return &job
}

func WithCompanyJobParentID(parentID string) CompanyJobOptions {
	return func(j *CompanyJob) {
		j.ParentID = parentID
	}
}

func WithCompanyJobPriority(priority int) CompanyJobOptions {
	return func(j *CompanyJob) {
		j.Priority = priority
	}
}

func WithCompanyJobExitMonitor(exitMonitor exiter.Exiter) CompanyJobOptions {
	return func(j *CompanyJob) {
		j.ExitMonitor = exitMonitor
	}
}

func (j *CompanyJob) Process(ctx context.Context, resp *scrapemate.Response) (any, []scrapemate.IJob, error) {
	defer func() {
		resp.Document = nil
		resp.Body = nil
		resp.Meta = nil
	}()

	logr := scrapemate.GetLoggerFromContext(ctx)

	enrichResult := &CompanyEnrichmentResult{
		PlaceLink:      j.PlaceLink,
		OwnerID:        j.OwnerID,
		OrganizationID: j.OrganizationID,
	}

	checker := GetCompanyDataCheckerFromContext(ctx)
	if checker != nil {
		existingData, exists, err := checker.CheckCompanyDataExists(ctx, j.CompanyName, j.Address, j.OwnerID, j.OrganizationID)
		if err != nil {
			logr.Info(fmt.Sprintf("CheckCompanyDataExists error for %s: %v", j.CompanyName, err))
		} else if exists && existingData != nil {
			enrichResult.SocieteDirigeants = existingData.SocieteDirigeants
			enrichResult.SocieteForme = existingData.SocieteForme
			enrichResult.SocieteCreation = existingData.SocieteCreation
			enrichResult.SocieteCloture = existingData.SocieteCloture
			enrichResult.SocieteSiren = existingData.SocieteSiren
			enrichResult.SocieteLink = existingData.SocieteLink
			enrichResult.SocieteDiffusion = existingData.SocieteDiffusion

			if len(enrichResult.SocieteDirigeants) == 0 && enrichResult.SocieteSiren != "" {
				service := entreprise.NewService()
				directorInfo := service.GetDirectors(enrichResult.SocieteSiren, "")
				if directorInfo != nil && directorInfo.Nom != "" && directorInfo.Prenom != "" {
					prenomFormatted := strings.ToUpper(string(directorInfo.Prenom[0])) + strings.ToLower(directorInfo.Prenom[1:])
					directorName := directorInfo.Nom + " " + prenomFormatted
					enrichResult.SocieteDirigeants = []string{directorName}
				}
			}

			return enrichResult, nil, nil
		}
	}

	service := entreprise.NewService()
	result, err := service.SearchCompany(j.CompanyName, j.Address)

	if err != nil {
		return enrichResult, nil, nil
	}

	if !result.Success || len(result.Data) == 0 {
		return enrichResult, nil, nil
	}

	company := result.Data[0]
	enrichResult.SocieteDirigeants = company.SocieteDirigeants
	enrichResult.SocieteForme = company.SocieteForme
	enrichResult.SocieteCreation = company.SocieteCreation
	enrichResult.SocieteCloture = company.SocieteCloture
	enrichResult.SocieteSiren = company.SocieteSiren
	enrichResult.SocieteLink = company.SocieteLink
	enrichResult.SocieteDiffusion = company.SocieteDiffusion
	enrichResult.PappersURL = company.PappersURL

	if len(company.SocieteDirigeants) == 0 && company.SocieteSiren != "" {
		directorInfo := service.GetDirectors(company.SocieteSiren, "")
		if directorInfo != nil && directorInfo.Nom != "" && directorInfo.Prenom != "" {
			prenomFormatted := strings.ToUpper(string(directorInfo.Prenom[0])) + strings.ToLower(directorInfo.Prenom[1:])
			directorName := directorInfo.Nom + " " + prenomFormatted
			enrichResult.SocieteDirigeants = []string{directorName}
		}
	}

	// If PappersURL is available, create a PappersJob for director scraping
	if enrichResult.PappersURL != "" {
		pappersJob := NewPappersJob(enrichResult.PappersURL, j.PlaceLink, j.OwnerID, j.OrganizationID,
			WithPappersJobParentID(j.GetID()),
		)
		j.EnrichmentJobs = append(j.EnrichmentJobs, pappersJob)
	}

	return enrichResult, nil, nil
}

type CompanyDataCheckerKey struct{}

func GetCompanyDataCheckerFromContext(ctx context.Context) CompanyDataChecker {
	if checker, ok := ctx.Value(CompanyDataCheckerKey{}).(CompanyDataChecker); ok {
		return checker
	}
	return nil
}

func (j *CompanyJob) UseInResults() bool {
	return false
}

func (j *CompanyJob) BrowserActions(ctx context.Context, page playwright.Page) scrapemate.Response {
	var resp scrapemate.Response
	resp.URL = "entreprise://api"
	resp.StatusCode = 200
	return resp
}

