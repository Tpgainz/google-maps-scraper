package gmaps

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/gosom/google-maps-scraper/exiter"
	"github.com/gosom/scrapemate"
	"github.com/playwright-community/playwright-go"
)

type PlaceJobOptions func(*PlaceJob)

type PlaceJob struct {
	scrapemate.Job
	OwnerID             string
	UsageInResultststs bool
	ExtractEmail       bool
	ExitMonitor        exiter.Exiter
}

func NewPlaceJob(parentID, langCode, u string, ownerID string, extractEmail bool, opts ...PlaceJobOptions) *PlaceJob {
	const (
		defaultPrio       = scrapemate.PriorityMedium
		defaultMaxRetries = 3
	)

	job := PlaceJob{
		Job: scrapemate.Job{
			ID:         uuid.New().String(),
			ParentID:   parentID,
			Method:     "GET",
			URL:        u,
			URLParams:  map[string]string{"hl": langCode},
			MaxRetries: defaultMaxRetries,
			Priority:   defaultPrio,
		},
	}

	job.UsageInResultststs = true
	job.ExtractEmail = extractEmail
	job.OwnerID = ownerID

	for _, opt := range opts {
		opt(&job)
	}

	return &job
}

func WithPlaceJobExitMonitor(exitMonitor exiter.Exiter) PlaceJobOptions {
	return func(j *PlaceJob) {
		j.ExitMonitor = exitMonitor
	}
}

func (j *PlaceJob) Process(_ context.Context, resp *scrapemate.Response) (any, []scrapemate.IJob, error) {
	defer func() {
		resp.Document = nil
		resp.Body = nil
		resp.Meta = nil
	}()

	raw, ok := resp.Meta["json"].([]byte)
	if !ok {
		return nil, nil, fmt.Errorf("could not convert to []byte")
	}

	entry, err := EntryFromJSON(raw)
	if err != nil {
		return nil, nil, err
	}

	entry.ID = j.ParentID

	if entry.Link == "" {
		entry.Link = j.GetURL()
	}

	if j.ExtractEmail && entry.IsWebsiteValidForEmail() {
		opts := []EmailExtractJobOptions{}
		if j.ExitMonitor != nil {
			opts = append(opts, WithEmailJobExitMonitor(j.ExitMonitor))
		}

		emailJob := NewEmailJob(j.ID, &entry, opts...)

		j.UsageInResultststs = false

		return nil, []scrapemate.IJob{emailJob}, nil
	} else if j.ExitMonitor != nil {
		j.ExitMonitor.IncrPlacesCompleted(1)
	}

	return &entry, nil, err
}

func (j *PlaceJob) BrowserActions(_ context.Context, page playwright.Page) scrapemate.Response {
	var resp scrapemate.Response

	pageResponse, err := page.Goto(j.GetURL(), playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateDomcontentloaded,
	})

	if err != nil {
		resp.Error = err

		return resp
	}

	if err = clickRejectCookiesIfRequired(page); err != nil {
		resp.Error = err

		return resp
	}

	const defaultTimeout = 10000

	err = page.WaitForURL(page.URL(), playwright.PageWaitForURLOptions{
		WaitUntil: playwright.WaitUntilStateNetworkidle,
		Timeout:   playwright.Float(defaultTimeout),
	})
	if err != nil {
		resp.Error = err

		return resp
	}

	resp.URL = pageResponse.URL()
	resp.StatusCode = pageResponse.Status()
	resp.Headers = make(http.Header, len(pageResponse.Headers()))

	for k, v := range pageResponse.Headers() {
		resp.Headers.Add(k, v)
	}

	page.On("console", func(msg playwright.ConsoleMessage) {
		if msg.Type() == "log" {
			fmt.Printf("CONSOLE: %s\n", msg.Text())
		}
	})

	rawI, err := page.Evaluate(js)
	if err != nil {
		resp.Error = err

		return resp
	}
	raw, ok := rawI.(string)
	if !ok {
		if rawI == nil {
			resp.Error = fmt.Errorf("JavaScript returned null - page structure may have changed or data not available")
		} else {
			resp.Error = fmt.Errorf("could not convert to string: got %T, expected string", rawI)
		}

		return resp
	}

	if raw == "" {
		resp.Error = fmt.Errorf("JavaScript returned empty string - no data found on page")
		return resp
	}

	const prefix = `)]}'`

	raw = strings.TrimSpace(strings.TrimPrefix(raw, prefix))

	if resp.Meta == nil {
		resp.Meta = make(map[string]any)
	}

	resp.Meta["json"] = []byte(raw)

	return resp
}

func (j *PlaceJob) UseInResults() bool {
	return j.UsageInResultststs
}


const js = `
function parse() {
	const appState = window.APP_INITIALIZATION_STATE[3];
	if (!appState) {
		return null;
	}

	for (let i = 65; i <= 90; i++) {
		const key = String.fromCharCode(i) + "f";
		if (appState[key] && appState[key][6]) {
		return appState[key][6];
		}
	}

	return null;
}
`
