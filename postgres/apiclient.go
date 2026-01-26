package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gosom/scrapemate"
)

// APIClient handles HTTP API calls for revalidation and job completion.
type APIClient struct {
	revalidationURL   string
	jobCompletionURL  string
	httpClient        *http.Client
}

// NewAPIClient creates a new APIClient with the given URLs.
func NewAPIClient(revalidationURL, jobCompletionURL string) *APIClient {
	return &APIClient{
		revalidationURL:  revalidationURL,
		jobCompletionURL: jobCompletionURL,
		httpClient:       &http.Client{Timeout: 10 * time.Second},
	}
}

// CallRevalidationAPI calls the revalidation API for the given userID.
func (c *APIClient) CallRevalidationAPI(ctx context.Context, userID string) {
	if c.revalidationURL == "" || userID == "" {
		log := scrapemate.GetLoggerFromContext(ctx)
		if c.revalidationURL == "" {
			log.Info(fmt.Sprintf("Skipping revalidation API call: revalidationURL is empty (userID=%s)", userID))
		}
		if userID == "" {
			log.Info(fmt.Sprintf("Skipping revalidation API call: userID is empty (revalidationURL=%s)", c.revalidationURL))
		}
		return
	}

	payload := map[string]string{"userId": userID}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.revalidationURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")

	log := scrapemate.GetLoggerFromContext(ctx)
	log.Info(fmt.Sprintf("Calling revalidation API: %s", c.revalidationURL))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	log.Info("Revalidation API response successful")
}

// CallJobCompletionAPIAsync calls the job completion API asynchronously.
func (c *APIClient) CallJobCompletionAPIAsync(ctx context.Context, jobID string, payload []byte) {
	if c.jobCompletionURL == "" {
		return
	}

	go func() {
		var rawJSON string
		if err := json.Unmarshal(payload, &rawJSON); err == nil {
			payload = []byte(rawJSON)
		}

		var jsonJob JSONJob
		if err := json.Unmarshal(payload, &jsonJob); err != nil {
			return
		}

		var ownerID, organizationID string
		if jsonJob.Metadata != nil {
			if id, ok := jsonJob.Metadata["owner_id"].(string); ok {
				ownerID = id
			}
			if id, ok := jsonJob.Metadata["organization_id"].(string); ok {
				organizationID = id
			}
		}

		apiPayload := map[string]interface{}{
			"jobId":          jobID,
			"userId":         ownerID,
			"organizationId": organizationID,
		}

		jsonData, err := json.Marshal(apiPayload)
		if err != nil {
			return
		}

		req, err := http.NewRequestWithContext(context.Background(), "POST", c.jobCompletionURL, bytes.NewBuffer(jsonData))
		if err != nil {
			return
		}

		req.Header.Set("Content-Type", "application/json")

		log := scrapemate.GetLoggerFromContext(ctx)
		log.Info(fmt.Sprintf("Calling job completion API: %s", c.jobCompletionURL))

		resp, err := c.httpClient.Do(req)
		if err != nil {
			log.Error(fmt.Sprintf("Job completion API call failed: %v", err))
			return
		}
		defer resp.Body.Close()

		log.Info(fmt.Sprintf("Job completion API response successful (status: %d)", resp.StatusCode))
	}()
}

// GetRevalidationURL returns the revalidation URL.
func (c *APIClient) GetRevalidationURL() string {
	return c.revalidationURL
}

// GetJobCompletionURL returns the job completion URL.
func (c *APIClient) GetJobCompletionURL() string {
	return c.jobCompletionURL
}
