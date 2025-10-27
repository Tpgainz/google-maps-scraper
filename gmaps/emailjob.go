package gmaps

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/uuid"
	"github.com/gosom/google-maps-scraper/exiter"
	"github.com/gosom/scrapemate"
	"github.com/mcnijman/go-emailaddress"
)

var (
	EmailRegex = regexp.MustCompile(`(?i)^[a-z0-9._%+\-]+@[a-z0-9\-]+\.[a-z\-]+$`)
	ExcludedDomains = []string{"sentry", "example", "wix"}
    ExcludedSuffixes = []string{".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp"}
)

type EmailExtractJobOptions func(*EmailExtractJob)

type EmailExtractJob struct {
	scrapemate.Job

	OwnerID string
	OrganizationID string
	Entry       *Entry
	ExitMonitor exiter.Exiter
}

func NewEmailJob(parentID string, entry *Entry, ownerID, organizationID string, opts ...EmailExtractJobOptions) *EmailExtractJob {
	const (
		defaultPrio       = scrapemate.PriorityHigh
		defaultMaxRetries = 0
	)

	job := EmailExtractJob{
		Job: scrapemate.Job{
			ID:         uuid.New().String(),
			ParentID:   parentID,
			Method:     "GET",
			URL:        entry.WebSite,
			MaxRetries: defaultMaxRetries,
			Priority:   defaultPrio,
		},
	}

	job.Entry = entry
	job.OwnerID = ownerID
	job.OrganizationID = organizationID
	for _, opt := range opts {
		opt(&job)
	}

	return &job
}

func WithEmailJobExitMonitor(exitMonitor exiter.Exiter) EmailExtractJobOptions {
	return func(j *EmailExtractJob) {
		j.ExitMonitor = exitMonitor
	}
}

func (j *EmailExtractJob) Process(ctx context.Context, resp *scrapemate.Response) (any, []scrapemate.IJob, error) {
	defer func() {
		resp.Document = nil
		resp.Body = nil
	}()

	defer func() {
		if j.ExitMonitor != nil {
			j.ExitMonitor.IncrPlacesCompleted(1)
		}
	}()

	log := scrapemate.GetLoggerFromContext(ctx)

	log.Info("Processing email job", "url", j.URL)

	// if html fetch failed just return
	if resp.Error != nil {
		return j.Entry, nil, nil
	}

	doc, ok := resp.Document.(*goquery.Document)
	if !ok {
		return j.Entry, nil, nil
	}

	emails := docEmailExtractor(doc)
	regexEmails := regexEmailExtractor(resp.Body)
	if len(regexEmails) > 0 {
		seen := map[string]bool{}
		for _, e := range emails {
			seen[e] = true
		}
		for _, e := range regexEmails {
			if !seen[e] {
				emails = append(emails, e)
				seen[e] = true
			}
		}
	}

	j.Entry.Emails = emails

	return j.Entry, nil, nil
}

func (j *EmailExtractJob) ProcessOnFetchError() bool {
	return true
}

func docEmailExtractor(doc *goquery.Document) []string {
	seen := map[string]bool{}

	var emails []string

	doc.Find("a[href^='mailto:']").Each(func(_ int, s *goquery.Selection) {
		mailto, ok := s.Attr("href")
		if !ok {
			return
		}
		value := strings.TrimPrefix(mailto, "mailto:")
		email, err := getValidEmail(value)
		if err != nil {
			return
		}
		if seen[email] {
			return
		}
		emails = append(emails, email)
		seen[email] = true
	})

	return emails
}

func regexEmailExtractor(body []byte) []string {
	seen := map[string]bool{}

	var emails []string

	addresses := emailaddress.Find(body, false)
	for i := range addresses {
		v := addresses[i].String()
		email, err := getValidEmail(v)
		if err != nil {
			continue
		}
		if seen[email] {
			continue
		}
		emails = append(emails, email)
		seen[email] = true
	}

	raw := string(body)
	matches := EmailRegex.FindAllString(raw, -1)
	for _, m := range matches {
		email, err := getValidEmail(m)
		if err != nil {
			continue
		}
		if seen[email] {
			continue
		}
		emails = append(emails, email)
		seen[email] = true
	}

	return emails
}

func getValidEmail(s string) (string, error) {
	email, err := emailaddress.Parse(strings.TrimSpace(s))
	if err != nil {
		return "", err
	}

	emailStr := email.String()

	lowerEmailStr := strings.ToLower(emailStr)
	lowerInput := strings.ToLower(s)

	if containsExcludedDomain(lowerEmailStr) {
		return "", errors.New("email contains excluded domain")
	}

	if containsExcludedSuffix(lowerEmailStr, lowerInput) {
		return "", errors.New("email contains excluded suffix")
	}

	return emailStr, nil
}

func containsExcludedDomain(email string) bool {
	for _, excludedDomain := range ExcludedDomains {
		if strings.Contains(email, excludedDomain) {
			return true
		}
	}
	return false
}

func containsExcludedSuffix(email, input string) bool {
	for _, suffix := range ExcludedSuffixes {
		if strings.HasSuffix(email, suffix) || strings.Contains(input, suffix) {
			return true
		}
	}
	return false
}
