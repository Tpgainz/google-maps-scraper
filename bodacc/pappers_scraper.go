package bodacc

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/uuid"
	"github.com/gosom/scrapemate"
)

type PappersScraperJob struct {
	scrapemate.Job
	CompanyInfo *BodaccCompanyInfo
}

func NewPappersScraperJob(companyInfo *BodaccCompanyInfo) *PappersScraperJob {
	const (
		defaultPrio       = scrapemate.PriorityHigh
		defaultMaxRetries = 2
	)

	job := PappersScraperJob{
		Job: scrapemate.Job{
			ID:         uuid.New().String(),
			Method:     http.MethodGet,
			URL:        companyInfo.PappersURL,
			MaxRetries: defaultMaxRetries,
			Priority:   defaultPrio,
		},
		CompanyInfo: companyInfo,
	}

	return &job
}

func (j *PappersScraperJob) Process(ctx context.Context, resp *scrapemate.Response) (any, []scrapemate.IJob, error) {
	defer func() {
		resp.Document = nil
		resp.Body = nil
		resp.Meta = nil
	}()

	if resp.Document == nil {
		return nil, nil, fmt.Errorf("no document available")
	}

	doc, ok := resp.Document.(*goquery.Document)
	if !ok {
		return nil, nil, fmt.Errorf("could not convert document to goquery.Document")
	}
	
	directors := j.extractDirectors(doc)
	
	result := &PappersScrapingResult{
		CompanyInfo: j.CompanyInfo,
		Directors:   directors,
	}

	return result, nil, nil
}

func (j *PappersScraperJob) extractDirectors(doc *goquery.Document) []string {
	var directors []string

	doc.Find("td.info-dirigeant a.underline").Each(func(i int, s *goquery.Selection) {
		directorName := strings.TrimSpace(s.Text())
		if directorName != "" {
			directors = append(directors, directorName)
		}
	})

	return directors
}

type PappersScrapingResult struct {
	CompanyInfo *BodaccCompanyInfo `json:"companyInfo"`
	Directors   []string           `json:"directors"`
}

func (r *PappersScrapingResult) GetID() string {
	return r.CompanyInfo.SocieteSiren
}

func (r *PappersScrapingResult) GetData() interface{} {
	return r
}
