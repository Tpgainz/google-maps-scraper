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

type CompanyJobOptions func(*CompanyJob)

type CompanyJob struct {
	scrapemate.Job
	OwnerID        string
	OrganizationID string
	CompanyName    string
	Address        string
	Entry          *Entry
	ExitMonitor    exiter.Exiter
}

func NewCompanyJob(companyName, address, ownerID, organizationID string, entry *Entry, opts ...CompanyJobOptions) *CompanyJob {
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
		Entry:          entry,
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

	checker := GetCompanyDataCheckerFromContext(ctx)
	if checker != nil {
		existingData, exists, err := checker.CheckCompanyDataExists(ctx, j.CompanyName, j.Address, j.OwnerID, j.OrganizationID)
		if err != nil {
			logr.Info(fmt.Sprintf("Error checking existing company data for %s: %v", j.CompanyName, err))
		} else if exists && existingData != nil {
			logr.Info(fmt.Sprintf("Found existing company data in DB for %s at %s", j.CompanyName, j.Address))
			j.Entry.SocieteDirigeants = existingData.SocieteDirigeants
			j.Entry.SocieteForme = existingData.SocieteForme
			j.Entry.SocieteCreation = existingData.SocieteCreation
			j.Entry.SocieteCloture = existingData.SocieteCloture
			j.Entry.SocieteSiren = existingData.SocieteSiren
			j.Entry.SocieteLink = existingData.SocieteLink
			j.Entry.SocieteDiffusion = existingData.SocieteDiffusion

			if len(j.Entry.SocieteDirigeants) == 0 && j.Entry.SocieteSiren != "" {
				logr.Info(fmt.Sprintf("No directors found in DB for %s, trying to get directors via service", 
					j.CompanyName))

				service := entreprise.NewService()
				directorInfo := service.GetDirectors(j.Entry.SocieteSiren, "")
				if directorInfo != nil && directorInfo.Nom != "" && directorInfo.Prenom != "" {
					prenomFormatted := strings.ToUpper(string(directorInfo.Prenom[0])) + strings.ToLower(directorInfo.Prenom[1:])
					directorName := directorInfo.Nom + " " + prenomFormatted
					j.Entry.SocieteDirigeants = []string{directorName}
					logr.Info(fmt.Sprintf("Found director for %s: %s", j.CompanyName, directorName))
				}
			}

			if j.ExitMonitor != nil {
				j.ExitMonitor.IncrPlacesCompleted(1)
			}

			return j.Entry, nil, nil
		}
	}

	service := entreprise.NewService()
	result, err := service.SearchCompany(j.CompanyName, j.Address)

	if err != nil {
		logr.Info(fmt.Sprintf("Service search failed for %s: %v", j.CompanyName, err))
		return j.Entry, nil, nil
	}

	if !result.Success {
		logr.Info(fmt.Sprintf("Service search unsuccessful for %s: %s", j.CompanyName, result.Error))
		return j.Entry, nil, nil
	}

	if len(result.Data) == 0 {
		logr.Info(fmt.Sprintf("No data found for: %s", j.CompanyName))
		return j.Entry, nil, nil
	}

	company := result.Data[0]
	j.Entry.SocieteDirigeants = company.SocieteDirigeants
	j.Entry.SocieteForme = company.SocieteForme
	j.Entry.SocieteCreation = company.SocieteCreation
	j.Entry.SocieteCloture = company.SocieteCloture
	j.Entry.SocieteSiren = company.SocieteSiren
	j.Entry.SocieteLink = company.SocieteLink
	j.Entry.SocieteDiffusion = company.SocieteDiffusion
	j.Entry.PappersURL = company.PappersURL

	logr.Info(fmt.Sprintf("Updated entry %s with service data: SIREN=%s, Directors=%v", 
		j.Entry.Title, company.SocieteSiren, company.SocieteDirigeants))

	if len(company.SocieteDirigeants) == 0 && company.SocieteSiren != "" {
		logr.Info(fmt.Sprintf("No directors found for %s, trying to get directors via service", 
			j.CompanyName))

		directorInfo := service.GetDirectors(company.SocieteSiren, "")
		if directorInfo != nil && directorInfo.Nom != "" && directorInfo.Prenom != "" {
			prenomFormatted := strings.ToUpper(string(directorInfo.Prenom[0])) + strings.ToLower(directorInfo.Prenom[1:])
			directorName := directorInfo.Nom + " " + prenomFormatted
			j.Entry.SocieteDirigeants = []string{directorName}
			logr.Info(fmt.Sprintf("Found director for %s: %s", j.CompanyName, directorName))
		}
	}

	if j.ExitMonitor != nil {
		j.ExitMonitor.IncrPlacesCompleted(1)
	}

	return j.Entry, nil, nil
}

type CompanyDataCheckerKey struct{}

func GetCompanyDataCheckerFromContext(ctx context.Context) CompanyDataChecker {
	if checker, ok := ctx.Value(CompanyDataCheckerKey{}).(CompanyDataChecker); ok {
		return checker
	}
	return nil
}

func (j *CompanyJob) UseInResults() bool {
	return true
}

func (j *CompanyJob) BrowserActions(ctx context.Context, page playwright.Page) scrapemate.Response {
	var resp scrapemate.Response
	resp.URL = "entreprise://api"
	resp.StatusCode = 200
	return resp
}

