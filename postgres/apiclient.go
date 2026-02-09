package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// APIClient handles HTTP API calls for revalidation and job completion.
type APIClient struct {
	revalidationURL      string
	jobCompletionURL     string
	httpClient           *http.Client
	revalidationMu       sync.Mutex
	lastRevalidation     map[string]time.Time
}

// NewAPIClient creates a new APIClient with the given URLs.
func NewAPIClient(revalidationURL, jobCompletionURL string) *APIClient {
	return &APIClient{
		revalidationURL:  revalidationURL,
		jobCompletionURL: jobCompletionURL,
		httpClient:       &http.Client{Timeout: 10 * time.Second},
		lastRevalidation: make(map[string]time.Time),
	}
}

// CallRevalidationAPI calls the revalidation API for the given userID.
// Debounces calls: skips if called within 5 seconds for the same user.
func (c *APIClient) CallRevalidationAPI(ctx context.Context, userID string) {
	if c.revalidationURL == "" || userID == "" {
		return
	}

	// Debounce: skip if called within 5 seconds for the same user
	c.revalidationMu.Lock()
	if last, ok := c.lastRevalidation[userID]; ok && time.Since(last) < 5*time.Second {
		c.revalidationMu.Unlock()
		return
	}
	c.lastRevalidation[userID] = time.Now()
	c.revalidationMu.Unlock()

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

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
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

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()
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
