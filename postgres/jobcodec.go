package postgres

import (
	"encoding/json"
	"fmt"

	"github.com/gosom/google-maps-scraper/gmaps"
	"github.com/gosom/scrapemate"
)

// JobCodec handles encoding and decoding of a specific job type.
type JobCodec interface {
	// JobType returns the type identifier for this codec.
	JobType() string
	// Encode converts a job to a JSONJob.
	Encode(job scrapemate.IJob) (*JSONJob, error)
	// Decode converts a JSONJob back to a job.
	Decode(jsonJob *JSONJob) (scrapemate.IJob, error)
}

// CodecRegistry manages job codecs by type.
type CodecRegistry struct {
	codecs map[string]JobCodec
}

// NewCodecRegistry creates a new registry with all supported codecs.
func NewCodecRegistry() *CodecRegistry {
	r := &CodecRegistry{
		codecs: make(map[string]JobCodec),
	}
	r.Register(&GmapJobCodec{})
	r.Register(&PlaceJobCodec{})
	r.Register(&EmailJobCodec{})
	r.Register(&CompanyJobCodec{})
	r.Register(&PappersJobCodec{})
	return r
}

// Register adds a codec to the registry.
func (r *CodecRegistry) Register(codec JobCodec) {
	r.codecs[codec.JobType()] = codec
}

// GetCodec returns the codec for the given job type.
func (r *CodecRegistry) GetCodec(jobType string) (JobCodec, bool) {
	codec, ok := r.codecs[jobType]
	return codec, ok
}

// EncodeJob encodes a job using the appropriate codec.
func (r *CodecRegistry) EncodeJob(job scrapemate.IJob) (*JSONJob, string, error) {
	// Unwrap if wrapped
	actualJob := job
	if wrapper, ok := job.(*jobWrapper); ok {
		actualJob = wrapper.IJob
	}

	var jobType string
	switch actualJob.(type) {
	case *gmaps.GmapJob:
		jobType = "search"
	case *gmaps.PlaceJob:
		jobType = "place"
	case *gmaps.EmailExtractJob:
		jobType = "email"
	case *gmaps.CompanyJob:
		jobType = "bodacc"
	case *gmaps.PappersJob:
		jobType = "pappers"
	default:
		return nil, "", fmt.Errorf("unsupported job type: %T", actualJob)
	}

	codec, ok := r.GetCodec(jobType)
	if !ok {
		return nil, "", fmt.Errorf("no codec registered for job type: %s", jobType)
	}

	jsonJob, err := codec.Encode(actualJob)
	if err != nil {
		return nil, "", err
	}

	return jsonJob, jobType, nil
}

// DecodeJob decodes a job using the appropriate codec.
func (r *CodecRegistry) DecodeJob(payloadType string, payload []byte) (scrapemate.IJob, error) {
	// If the payload is a string, unmarshal it first
	var rawJSON string
	if err := json.Unmarshal(payload, &rawJSON); err == nil {
		payload = []byte(rawJSON)
	}

	var jsonJob JSONJob
	if err := json.Unmarshal(payload, &jsonJob); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	codec, ok := r.GetCodec(payloadType)
	if !ok {
		return nil, fmt.Errorf("invalid payload type: %s", payloadType)
	}

	return codec.Decode(&jsonJob)
}

// GmapJobCodec handles GmapJob encoding/decoding.
type GmapJobCodec struct{}

func (c *GmapJobCodec) JobType() string { return "search" }

func (c *GmapJobCodec) Encode(job scrapemate.IJob) (*JSONJob, error) {
	j, ok := job.(*gmaps.GmapJob)
	if !ok {
		return nil, fmt.Errorf("expected *gmaps.GmapJob, got %T", job)
	}

	jsonJob := &JSONJob{
		ID:         j.GetID(),
		Priority:   j.GetPriority(),
		URL:        j.GetURL(),
		URLParams:  j.GetURLParams(),
		MaxRetries: j.GetMaxRetries(),
		JobType:    "search",
		Metadata: map[string]interface{}{
			"max_depth":       j.MaxDepth,
			"lang_code":       j.LangCode,
			"extract_email":   j.ExtractEmail,
			"extract_bodacc":  j.ExtractBodacc,
			"owner_id":        j.OwnerID,
			"organization_id": j.OrganizationID,
		},
	}

	if j.ParentID != "" {
		jsonJob.ParentID = &j.ParentID
	}

	return jsonJob, nil
}

func (c *GmapJobCodec) Decode(jsonJob *JSONJob) (scrapemate.IJob, error) {
	maxDepth, err := getIntFromMetadata(jsonJob.Metadata, "max_depth")
	if err != nil {
		return nil, fmt.Errorf("failed to get max_depth: %w", err)
	}

	langCode, ok := jsonJob.Metadata["lang_code"].(string)
	if !ok {
		return nil, fmt.Errorf("lang_code is missing or not a string")
	}

	extractEmail, ok := jsonJob.Metadata["extract_email"].(bool)
	if !ok {
		return nil, fmt.Errorf("extract_email is missing or not a boolean")
	}
	extractBodacc, _ := jsonJob.Metadata["extract_bodacc"].(bool)

	ownerID, ok := jsonJob.Metadata["owner_id"].(string)
	if !ok {
		return nil, fmt.Errorf("owner_id is missing or not a string")
	}

	organizationID, ok := jsonJob.Metadata["organization_id"].(string)
	if !ok {
		return nil, fmt.Errorf("organization_id is not a string")
	}

	var parentID string
	if jsonJob.ParentID != nil {
		parentID = *jsonJob.ParentID
	}

	return &gmaps.GmapJob{
		Job: scrapemate.Job{
			ID:         jsonJob.ID,
			ParentID:   parentID,
			URL:        jsonJob.URL,
			URLParams:  jsonJob.URLParams,
			MaxRetries: jsonJob.MaxRetries,
			Priority:   jsonJob.Priority,
		},
		MaxDepth:       maxDepth,
		LangCode:       langCode,
		ExtractEmail:   extractEmail,
		ExtractBodacc:  extractBodacc,
		OwnerID:        ownerID,
		OrganizationID: organizationID,
	}, nil
}

// PlaceJobCodec handles PlaceJob encoding/decoding.
type PlaceJobCodec struct{}

func (c *PlaceJobCodec) JobType() string { return "place" }

func (c *PlaceJobCodec) Encode(job scrapemate.IJob) (*JSONJob, error) {
	j, ok := job.(*gmaps.PlaceJob)
	if !ok {
		return nil, fmt.Errorf("expected *gmaps.PlaceJob, got %T", job)
	}

	jsonJob := &JSONJob{
		ID:         j.GetID(),
		Priority:   j.GetPriority(),
		URL:        j.GetURL(),
		URLParams:  j.GetURLParams(),
		MaxRetries: j.GetMaxRetries(),
		JobType:    "place",
		Metadata: map[string]interface{}{
			"usage_in_results": j.UsageInResultststs,
			"extract_email":    j.ExtractEmail,
			"extract_bodacc":   j.ExtractBodacc,
			"owner_id":         j.OwnerID,
			"organization_id":  j.OrganizationID,
		},
	}

	if j.ParentID != "" {
		jsonJob.ParentID = &j.ParentID
	}

	return jsonJob, nil
}

func (c *PlaceJobCodec) Decode(jsonJob *JSONJob) (scrapemate.IJob, error) {
	usageInResults, ok := jsonJob.Metadata["usage_in_results"].(bool)
	if !ok {
		return nil, fmt.Errorf("usage_in_results is missing or not a boolean")
	}

	extractEmail, ok := jsonJob.Metadata["extract_email"].(bool)
	if !ok {
		return nil, fmt.Errorf("extract_email is missing or not a boolean")
	}
	extractBodacc, _ := jsonJob.Metadata["extract_bodacc"].(bool)

	ownerID, ok := jsonJob.Metadata["owner_id"].(string)
	if !ok {
		return nil, fmt.Errorf("owner_id is missing or not a string")
	}

	organizationID, ok := jsonJob.Metadata["organization_id"].(string)
	if !ok {
		return nil, fmt.Errorf("organization_id is not a string")
	}

	var parentID string
	if jsonJob.ParentID != nil {
		parentID = *jsonJob.ParentID
	}

	return &gmaps.PlaceJob{
		Job: scrapemate.Job{
			ID:         jsonJob.ID,
			ParentID:   parentID,
			URL:        jsonJob.URL,
			URLParams:  jsonJob.URLParams,
			MaxRetries: jsonJob.MaxRetries,
			Priority:   jsonJob.Priority,
		},
		UsageInResultststs: usageInResults,
		ExtractEmail:       extractEmail,
		ExtractBodacc:      extractBodacc,
		OwnerID:            ownerID,
		OrganizationID:     organizationID,
	}, nil
}

// EmailJobCodec handles EmailExtractJob encoding/decoding.
type EmailJobCodec struct{}

func (c *EmailJobCodec) JobType() string { return "email" }

func (c *EmailJobCodec) Encode(job scrapemate.IJob) (*JSONJob, error) {
	j, ok := job.(*gmaps.EmailExtractJob)
	if !ok {
		return nil, fmt.Errorf("expected *gmaps.EmailExtractJob, got %T", job)
	}

	jsonJob := &JSONJob{
		ID:         j.GetID(),
		Priority:   j.GetPriority(),
		URL:        j.GetURL(),
		URLParams:  j.GetURLParams(),
		MaxRetries: j.GetMaxRetries(),
		JobType:    "email",
		Metadata: map[string]interface{}{
			"entry":           j.Entry,
			"parent_id":       j.Job.ParentID,
			"owner_id":        j.OwnerID,
			"organization_id": j.OrganizationID,
		},
	}

	if j.ParentID != "" {
		jsonJob.ParentID = &j.ParentID
	}

	return jsonJob, nil
}

func (c *EmailJobCodec) Decode(jsonJob *JSONJob) (scrapemate.IJob, error) {
	parentIDI, ok := jsonJob.Metadata["parent_id"].(string)
	if !ok {
		return nil, fmt.Errorf("parent_id is missing or not a string")
	}

	entryMap, ok := jsonJob.Metadata["entry"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("entry is missing or not an object")
	}

	entryBytes, err := json.Marshal(entryMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal entry: %w", err)
	}

	var entry gmaps.Entry
	if err := json.Unmarshal(entryBytes, &entry); err != nil {
		return nil, fmt.Errorf("failed to unmarshal entry: %w", err)
	}

	ownerID, ok := jsonJob.Metadata["owner_id"].(string)
	if !ok {
		return nil, fmt.Errorf("owner_id is missing or not a string")
	}

	organizationID, ok := jsonJob.Metadata["organization_id"].(string)
	if !ok {
		return nil, fmt.Errorf("organization_id is missing or not a string")
	}

	var parentID string
	if jsonJob.ParentID != nil {
		parentID = *jsonJob.ParentID
	}

	job := gmaps.NewEmailJob(parentIDI, &entry, ownerID, organizationID)
	job.Job.ID = jsonJob.ID
	job.Job.ParentID = parentID
	job.Job.URL = jsonJob.URL
	job.Job.URLParams = jsonJob.URLParams
	job.Job.MaxRetries = jsonJob.MaxRetries
	job.Job.Priority = jsonJob.Priority
	job.OwnerID = ownerID
	job.OrganizationID = organizationID

	return job, nil
}

// CompanyJobCodec handles CompanyJob encoding/decoding.
type CompanyJobCodec struct{}

func (c *CompanyJobCodec) JobType() string { return "bodacc" }

func (c *CompanyJobCodec) Encode(job scrapemate.IJob) (*JSONJob, error) {
	j, ok := job.(*gmaps.CompanyJob)
	if !ok {
		return nil, fmt.Errorf("expected *gmaps.CompanyJob, got %T", job)
	}

	jsonJob := &JSONJob{
		ID:         j.GetID(),
		Priority:   j.GetPriority(),
		URL:        j.GetURL(),
		URLParams:  j.GetURLParams(),
		MaxRetries: j.GetMaxRetries(),
		JobType:    "bodacc",
		Metadata: map[string]interface{}{
			"company_name":    j.CompanyName,
			"address":         j.Address,
			"owner_id":        j.OwnerID,
			"organization_id": j.OrganizationID,
			"entry":           j.Entry,
		},
	}

	if j.ParentID != "" {
		jsonJob.ParentID = &j.ParentID
	}

	return jsonJob, nil
}

func (c *CompanyJobCodec) Decode(jsonJob *JSONJob) (scrapemate.IJob, error) {
	companyName, ok := jsonJob.Metadata["company_name"].(string)
	if !ok {
		return nil, fmt.Errorf("company_name is missing or not a string")
	}

	address, ok := jsonJob.Metadata["address"].(string)
	if !ok {
		return nil, fmt.Errorf("address is missing or not a string")
	}

	ownerID, ok := jsonJob.Metadata["owner_id"].(string)
	if !ok {
		return nil, fmt.Errorf("owner_id is missing or not a string")
	}

	organizationID, ok := jsonJob.Metadata["organization_id"].(string)
	if !ok {
		return nil, fmt.Errorf("organization_id is missing or not a string")
	}

	var entry gmaps.Entry
	if entryMap, ok := jsonJob.Metadata["entry"].(map[string]any); ok {
		entryBytes, err := json.Marshal(entryMap)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal entry: %w", err)
		}
		if err := json.Unmarshal(entryBytes, &entry); err != nil {
			return nil, fmt.Errorf("failed to unmarshal entry: %w", err)
		}
	}

	var parentID string
	if jsonJob.ParentID != nil {
		parentID = *jsonJob.ParentID
	}

	return &gmaps.CompanyJob{
		Job: scrapemate.Job{
			ID:         jsonJob.ID,
			ParentID:   parentID,
			URL:        jsonJob.URL,
			URLParams:  jsonJob.URLParams,
			MaxRetries: jsonJob.MaxRetries,
			Priority:   jsonJob.Priority,
		},
		OwnerID:        ownerID,
		OrganizationID: organizationID,
		CompanyName:    companyName,
		Address:        address,
		Entry:          &entry,
	}, nil
}

// PappersJobCodec handles PappersJob encoding/decoding.
type PappersJobCodec struct{}

func (c *PappersJobCodec) JobType() string { return "pappers" }

func (c *PappersJobCodec) Encode(job scrapemate.IJob) (*JSONJob, error) {
	j, ok := job.(*gmaps.PappersJob)
	if !ok {
		return nil, fmt.Errorf("expected *gmaps.PappersJob, got %T", job)
	}

	jsonJob := &JSONJob{
		ID:         j.GetID(),
		Priority:   j.GetPriority(),
		URL:        j.GetURL(),
		URLParams:  j.GetURLParams(),
		MaxRetries: j.GetMaxRetries(),
		JobType:    "pappers",
		Metadata: map[string]interface{}{
			"owner_id":        j.OwnerID,
			"organization_id": j.OrganizationID,
			"entry":           j.Entry,
		},
	}

	if j.ParentID != "" {
		jsonJob.ParentID = &j.ParentID
	}

	return jsonJob, nil
}

func (c *PappersJobCodec) Decode(jsonJob *JSONJob) (scrapemate.IJob, error) {
	ownerID, ok := jsonJob.Metadata["owner_id"].(string)
	if !ok {
		return nil, fmt.Errorf("owner_id is missing or not a string")
	}

	organizationID, ok := jsonJob.Metadata["organization_id"].(string)
	if !ok {
		return nil, fmt.Errorf("organization_id is missing or not a string")
	}

	var entry gmaps.Entry
	if entryMap, ok := jsonJob.Metadata["entry"].(map[string]any); ok {
		entryBytes, err := json.Marshal(entryMap)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal entry: %w", err)
		}
		if err := json.Unmarshal(entryBytes, &entry); err != nil {
			return nil, fmt.Errorf("failed to unmarshal entry: %w", err)
		}
	}

	var parentID string
	if jsonJob.ParentID != nil {
		parentID = *jsonJob.ParentID
	}

	return &gmaps.PappersJob{
		Job: scrapemate.Job{
			ID:         jsonJob.ID,
			ParentID:   parentID,
			URL:        jsonJob.URL,
			URLParams:  jsonJob.URLParams,
			MaxRetries: jsonJob.MaxRetries,
			Priority:   jsonJob.Priority,
		},
		OwnerID:        ownerID,
		OrganizationID: organizationID,
		Entry:          &entry,
	}, nil
}

// getIntFromMetadata extracts an integer from metadata (stored as float64 in JSON).
func getIntFromMetadata(metadata map[string]interface{}, key string) (int, error) {
	value, ok := metadata[key]
	if !ok {
		return 0, fmt.Errorf("missing key %s in metadata", key)
	}

	floatValue, ok := value.(float64)
	if !ok {
		return 0, fmt.Errorf("value for key %s is not a number", key)
	}

	return int(floatValue), nil
}
