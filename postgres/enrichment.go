package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gosom/google-maps-scraper/gmaps"
	"github.com/gosom/scrapemate"
)

// pushEnrichmentJobs inserts enrichment jobs into the DB with parent_id = NULL.
// It waits a short delay to let the batch result writer flush the place result first.
func (p *provider) pushEnrichmentJobs(ctx context.Context, jobs []scrapemate.IJob) {
	log := scrapemate.GetLoggerFromContext(ctx)

	time.Sleep(2 * time.Second)

	for _, job := range jobs {
		jsonJob, jobType, err := p.codecRegistry.EncodeJob(job)
		if err != nil {
			log.Error(fmt.Sprintf("pushEnrichmentJobs: failed to encode job: %v", err))
			continue
		}

		// Clear parent_id so enrichment jobs are standalone
		jsonJob.ParentID = nil

		if jsonJob.ID == "" {
			jsonJob.ID = uuid.New().String()
		}

		payload, err := json.Marshal(jsonJob)
		if err != nil {
			log.Error(fmt.Sprintf("pushEnrichmentJobs: failed to marshal job: %v", err))
			continue
		}

		q := `INSERT INTO gmaps_jobs
			(id, parent_id, priority, payload_type, payload, created_at, status)
			VALUES
			($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING`

		_, err = p.db.ExecContext(ctx, q,
			jsonJob.ID,
			nil, // no parent
			jsonJob.Priority,
			jobType,
			payload,
			time.Now().UTC(),
			statusNew,
		)
		if err != nil {
			log.Error(fmt.Sprintf("pushEnrichmentJobs: failed to insert job: %v", err))
			continue
		}

	}
}

// updateResultEmails updates the emails field on an existing result row.
func (p *provider) updateResultEmails(ctx context.Context, result *gmaps.EmailEnrichmentResult) {
	log := scrapemate.GetLoggerFromContext(ctx)

	if len(result.Emails) == 0 {
		return
	}

	var q string
	var args []interface{}

	if result.OwnerID != "" && result.OrganizationID != "" {
		q = `UPDATE results SET emails = $1, updated_at = NOW()
			WHERE link = $2 AND (user_id = $3 OR organization_id = $4)
			AND (emails IS NULL OR emails = '{}')`
		args = []interface{}{result.Emails, result.PlaceLink, result.OwnerID, result.OrganizationID}
	} else if result.OwnerID != "" {
		q = `UPDATE results SET emails = $1, updated_at = NOW()
			WHERE link = $2 AND user_id = $3
			AND (emails IS NULL OR emails = '{}')`
		args = []interface{}{result.Emails, result.PlaceLink, result.OwnerID}
	} else {
		q = `UPDATE results SET emails = $1, updated_at = NOW()
			WHERE link = $2 AND organization_id = $3
			AND (emails IS NULL OR emails = '{}')`
		args = []interface{}{result.Emails, result.PlaceLink, result.OrganizationID}
	}

	_, err := p.db.ExecContext(ctx, q, args...)
	if err != nil {
		log.Error(fmt.Sprintf("updateResultEmails: failed to update: %v", err))
		return
	}

	p.apiClient.CallRevalidationAPI(ctx, result.OwnerID)
}

// updateResultCompanyData updates company/societe fields on an existing result row.
func (p *provider) updateResultCompanyData(ctx context.Context, result *gmaps.CompanyEnrichmentResult) {
	log := scrapemate.GetLoggerFromContext(ctx)

	dirigeants := strings.Join(result.SocieteDirigeants, ",")

	var idCond string
	var args []interface{}

	if result.OwnerID != "" && result.OrganizationID != "" {
		idCond = "(user_id = $2 OR organization_id = $3)"
		args = []interface{}{result.PlaceLink, result.OwnerID, result.OrganizationID}
	} else if result.OwnerID != "" {
		idCond = "user_id = $2"
		args = []interface{}{result.PlaceLink, result.OwnerID}
	} else {
		idCond = "organization_id = $2"
		args = []interface{}{result.PlaceLink, result.OrganizationID}
	}

	nextIdx := len(args) + 1

	q := fmt.Sprintf(`UPDATE results SET
		societe_dirigeants = CASE WHEN (societe_dirigeants IS NULL OR societe_dirigeants = '') AND $%d <> '' THEN $%d ELSE societe_dirigeants END,
		societe_siren = CASE WHEN (societe_siren IS NULL OR societe_siren = '') AND $%d <> '' THEN $%d ELSE societe_siren END,
		societe_forme = CASE WHEN (societe_forme IS NULL OR societe_forme = '') AND $%d <> '' THEN $%d ELSE societe_forme END,
		societe_creation = CASE WHEN (societe_creation IS NULL OR societe_creation = '') AND $%d <> '' THEN $%d ELSE societe_creation END,
		societe_cloture = CASE WHEN (societe_cloture IS NULL OR societe_cloture = '') AND $%d <> '' THEN $%d ELSE societe_cloture END,
		societe_link = CASE WHEN (societe_link IS NULL OR societe_link = '') AND $%d <> '' THEN $%d ELSE societe_link END,
		societe_diffusion = CASE WHEN societe_diffusion = false AND $%d = true THEN $%d ELSE societe_diffusion END,
		updated_at = NOW()
		WHERE link = $1 AND %s`,
		nextIdx, nextIdx,
		nextIdx+1, nextIdx+1,
		nextIdx+2, nextIdx+2,
		nextIdx+3, nextIdx+3,
		nextIdx+4, nextIdx+4,
		nextIdx+5, nextIdx+5,
		nextIdx+6, nextIdx+6,
		idCond,
	)

	args = append(args,
		dirigeants,
		result.SocieteSiren,
		result.SocieteForme,
		result.SocieteCreation,
		result.SocieteCloture,
		result.SocieteLink,
		result.SocieteDiffusion,
	)

	_, err := p.db.ExecContext(ctx, q, args...)
	if err != nil {
		log.Error(fmt.Sprintf("updateResultCompanyData: failed to update: %v", err))
		return
	}

	p.apiClient.CallRevalidationAPI(ctx, result.OwnerID)
}

// updateResultPappers updates director fields from Pappers scraping.
func (p *provider) updateResultPappers(ctx context.Context, result *gmaps.PappersEnrichmentResult) {
	log := scrapemate.GetLoggerFromContext(ctx)

	if len(result.SocieteDirigeants) == 0 {
		return
	}

	dirigeants := strings.Join(result.SocieteDirigeants, ",")

	var q string
	var args []interface{}

	if result.OwnerID != "" && result.OrganizationID != "" {
		q = `UPDATE results SET
			societe_dirigeants = $1,
			updated_at = NOW()
			WHERE link = $2 AND (user_id = $3 OR organization_id = $4)
			AND (societe_dirigeants IS NULL OR societe_dirigeants = '')`
		args = []interface{}{dirigeants, result.PlaceLink, result.OwnerID, result.OrganizationID}
	} else if result.OwnerID != "" {
		q = `UPDATE results SET
			societe_dirigeants = $1,
			updated_at = NOW()
			WHERE link = $2 AND user_id = $3
			AND (societe_dirigeants IS NULL OR societe_dirigeants = '')`
		args = []interface{}{dirigeants, result.PlaceLink, result.OwnerID}
	} else {
		q = `UPDATE results SET
			societe_dirigeants = $1,
			updated_at = NOW()
			WHERE link = $2 AND organization_id = $3
			AND (societe_dirigeants IS NULL OR societe_dirigeants = '')`
		args = []interface{}{dirigeants, result.PlaceLink, result.OrganizationID}
	}

	_, err := p.db.ExecContext(ctx, q, args...)
	if err != nil {
		log.Error(fmt.Sprintf("updateResultPappers: failed to update: %v", err))
		return
	}

	p.apiClient.CallRevalidationAPI(ctx, result.OwnerID)
}

// isEnrichmentJob returns true if the job is an enrichment job (email, company, pappers).
func isEnrichmentJob(job scrapemate.IJob) bool {
	actualJob := job
	if wrapper, ok := job.(*jobWrapper); ok {
		actualJob = wrapper.IJob
	}
	switch actualJob.(type) {
	case *gmaps.EmailExtractJob, *gmaps.CompanyJob, *gmaps.PappersJob:
		return true
	}
	return false
}

// checkDuplicatePlace checks if a result with the same link already exists for this user/org.
func (p *provider) checkDuplicatePlace(ctx context.Context, link, ownerID, organizationID string) bool {
	query := NewDuplicateURLQuery(link, ownerID, organizationID)
	q, args, ok := query.Build()
	if !ok {
		return false
	}

	var count int
	err := p.db.QueryRowContext(ctx, q, args...).Scan(&count)
	if err != nil {
		return false
	}

	return count > 0
}

// existingEnrichmentData holds enrichment data found from an existing result.
type existingEnrichmentData struct {
	Emails            []string
	SocieteDirigeants []string
	SocieteSiren      string
	SocieteForme      string
	SocieteCreation   string
	SocieteCloture    string
	SocieteLink       string
	SocieteDiffusion  *bool
}

// findExistingEnrichmentData looks up existing enrichment data by title+address
// across ALL users/orgs. Returns nil if nothing useful found.
func (p *provider) findExistingEnrichmentData(ctx context.Context, title, address string) *existingEnrichmentData {
	if title == "" || address == "" {
		return nil
	}

	q := `SELECT
		array_to_string(emails, ','),
		societe_dirigeants, societe_siren, societe_forme,
		societe_creation, societe_cloture, societe_link, societe_diffusion
		FROM results
		WHERE LOWER(TRIM(title)) = LOWER(TRIM($1))
		AND LOWER(TRIM(address)) = LOWER(TRIM($2))
		AND (
			(emails IS NOT NULL AND array_length(emails, 1) > 0)
			OR (societe_siren IS NOT NULL AND societe_siren != '')
		)
		LIMIT 1`

	var emailsStr, dirigeants, siren, forme, creation, cloture, link sql.NullString
	var diffusion sql.NullBool
	err := p.db.QueryRowContext(ctx, q, title, address).Scan(
		&emailsStr, &dirigeants, &siren, &forme,
		&creation, &cloture, &link, &diffusion,
	)
	if err != nil {
		return nil
	}

	data := &existingEnrichmentData{}
	hasData := false

	if emailsStr.Valid && emailsStr.String != "" {
		data.Emails = strings.Split(emailsStr.String, ",")
		hasData = true
	}
	if dirigeants.Valid && dirigeants.String != "" {
		data.SocieteDirigeants = strings.Split(dirigeants.String, ",")
		for i := range data.SocieteDirigeants {
			data.SocieteDirigeants[i] = strings.TrimSpace(data.SocieteDirigeants[i])
		}
		hasData = true
	}
	if siren.Valid && siren.String != "" {
		data.SocieteSiren = siren.String
		hasData = true
	}
	if forme.Valid {
		data.SocieteForme = forme.String
	}
	if creation.Valid {
		data.SocieteCreation = creation.String
	}
	if cloture.Valid {
		data.SocieteCloture = cloture.String
	}
	if link.Valid {
		data.SocieteLink = link.String
	}
	if diffusion.Valid {
		v := diffusion.Bool
		data.SocieteDiffusion = &v
	}

	if !hasData {
		return nil
	}

	return data
}
