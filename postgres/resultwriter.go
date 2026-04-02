package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/gosom/scrapemate"
	"github.com/nyaruka/phonenumbers"

	"github.com/gosom/google-maps-scraper/gmaps"
)

type dbEntry struct {
	UserID            string
	OrganizationID    string
	ParentID          string
	Link              string
	PayloadType       string
	Title             string
	Category          string
	Address           string
	Website           string
	Phones            []string
	Emails            []string
	Latitude          float64
	Longitude         float64
	SocieteDirigeants string
	SocieteSiren      string
	SocieteForme      string
	SocieteEffectif   string
	SocieteCreation   string
	SocieteCloture    string
	SocieteLink       string
	SocieteDiffusion  *bool
}

// countryNameToCode maps common country names (as returned by Google Maps) to ISO 3166-1 alpha-2 codes.
var countryNameToCode = map[string]string{
	"france": "FR", "united states": "US", "united kingdom": "GB",
	"germany": "DE", "spain": "ES", "italy": "IT", "canada": "CA",
	"belgium": "BE", "switzerland": "CH", "netherlands": "NL",
	"senegal": "SN", "côte d'ivoire": "CI", "ivory coast": "CI",
	"morocco": "MA", "tunisia": "TN", "south africa": "ZA",
	"nigeria": "NG", "australia": "AU", "japan": "JP", "brazil": "BR",
	"portugal": "PT", "austria": "AT", "ireland": "IE", "poland": "PL",
	"sweden": "SE", "norway": "NO", "denmark": "DK", "finland": "FI",
	"greece": "GR", "czech republic": "CZ", "czechia": "CZ",
	"romania": "RO", "hungary": "HU", "luxembourg": "LU",
	"new zealand": "NZ", "mexico": "MX", "argentina": "AR",
	"colombia": "CO", "chile": "CL", "india": "IN", "china": "CN",
	"south korea": "KR", "thailand": "TH", "singapore": "SG",
	"malaysia": "MY", "indonesia": "ID", "philippines": "PH",
	"vietnam": "VN", "turkey": "TR", "israel": "IL",
	"united arab emirates": "AE", "saudi arabia": "SA",
	"egypt": "EG", "kenya": "KE", "ghana": "GH",
	// French names (Google Maps can return localized names)
	"états-unis": "US", "royaume-uni": "GB", "allemagne": "DE",
	"espagne": "ES", "italie": "IT", "belgique": "BE",
	"suisse": "CH", "pays-bas": "NL", "sénégal": "SN",
	"maroc": "MA", "tunisie": "TN", "afrique du sud": "ZA",
	"nigéria": "NG", "australie": "AU", "japon": "JP", "brésil": "BR",
	"nouvelle-zélande": "NZ", "mexique": "MX", "argentine": "AR",
	"colombie": "CO", "chili": "CL", "inde": "IN", "chine": "CN",
	"corée du sud": "KR", "thaïlande": "TH", "singapour": "SG",
	"malaisie": "MY", "indonésie": "ID", "turquie": "TR",
	"israël": "IL", "émirats arabes unis": "AE", "arabie saoudite": "SA",
	"égypte": "EG",
}

// phoneToPhones normalizes a phone string to E.164 using the place's country for context.
// Google Maps often returns local-format numbers (e.g. "01 23 45 67 89" for France).
func phoneToPhones(phone, country string) []string {
	cleaned := strings.ReplaceAll(strings.TrimSpace(phone), " ", "")
	if cleaned == "" {
		return []string{}
	}

	// If already in international format, try to parse directly
	if strings.HasPrefix(cleaned, "+") {
		num, err := phonenumbers.Parse(cleaned, "")
		if err == nil && phonenumbers.IsValidNumber(num) {
			return []string{phonenumbers.Format(num, phonenumbers.E164)}
		}
		// Even if parsing fails, keep the raw cleaned number
		return []string{cleaned}
	}

	// Try to resolve country code from the place's country name
	regionCode := ""
	if country != "" {
		lower := strings.ToLower(strings.TrimSpace(country))
		if code, ok := countryNameToCode[lower]; ok {
			regionCode = code
		} else if len(country) == 2 {
			// Already an ISO code
			regionCode = strings.ToUpper(country)
		}
	}

	// Default to FR if we can't determine the country
	if regionCode == "" {
		regionCode = "FR"
	}

	num, err := phonenumbers.Parse(cleaned, regionCode)
	if err == nil && phonenumbers.IsValidNumber(num) {
		return []string{phonenumbers.Format(num, phonenumbers.E164)}
	}

	// Fallback: return raw cleaned number if parsing fails
	if cleaned != "" {
		return []string{cleaned}
	}
	return []string{}
}

// NewResultWriter creates a new ResultWriter backed by PostgreSQL.
func NewResultWriter(db *sql.DB, revalidationAPIURL string) scrapemate.ResultWriter {
	return &resultWriter{
		db:            db,
		apiClient:     NewAPIClient(revalidationAPIURL, ""),
		inMemoryIndex: make(map[string]int),
	}
}

type resultWriter struct {
	db            *sql.DB
	apiClient     *APIClient
	inMemoryIndex map[string]int
}

func (r *resultWriter) checkDuplicateURL(ctx context.Context, url, userID, organizationID string) (bool, error) {
	query := NewDuplicateURLQuery(url, userID, organizationID)
	q, args, ok := query.Build()
	if !ok {
		return false, nil
	}

	var count int
	err := r.db.QueryRowContext(ctx, q, args...).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check duplicate URL: %w", err)
	}

	return count > 0, nil
}

func (r *resultWriter) getParentJobID(ctx context.Context, jobID string) (string, error) {
	var parentID sql.NullString
	q := `SELECT parent_id FROM gmaps_jobs WHERE id = $1`
	err := r.db.QueryRowContext(ctx, q, jobID).Scan(&parentID)

	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("failed to get parent job ID: %w", err)
	}

	if parentID.Valid {
		return parentID.String, nil
	}

	return "", nil
}

func (r *resultWriter) getRootParentJobID(ctx context.Context, jobID string) (string, error) {
	currentJobID := jobID
	visitedJobs := make(map[string]bool)

	for {
		if visitedJobs[currentJobID] {
			return "", fmt.Errorf("circular reference detected in job hierarchy")
		}
		visitedJobs[currentJobID] = true

		parentID, err := r.getParentJobID(ctx, currentJobID)
		if err != nil {
			return "", err
		}

		if parentID == "" {
			return currentJobID, nil
		}

		currentJobID = parentID
	}
}

func (r *resultWriter) notifyRevalidation(ctx context.Context, entries []dbEntry) {
	if r.apiClient.GetRevalidationURL() == "" {
		return
	}

	// Extract unique user IDs
	userIDs := make(map[string]bool)
	for _, entry := range entries {
		if entry.UserID != "" {
			userIDs[entry.UserID] = true
		}
	}

	// Call revalidation API for each unique user ID
	for userID := range userIDs {
		go r.apiClient.CallRevalidationAPI(ctx, userID)
	}
}

func (r *resultWriter) Run(ctx context.Context, in <-chan scrapemate.Result) error {
	const maxBatchSize = 50

	log := scrapemate.GetLoggerFromContext(ctx)
	buff := make([]dbEntry, 0, 50)
	lastSave := time.Now().UTC()
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case result, ok := <-in:
			if !ok {
				if len(buff) > 0 {
					if err := r.batchSave(ctx, buff); err != nil {
						return err
					}
				}
				return nil
			}

			entry, ok := result.Data.(*gmaps.Entry)
			if !ok || entry == nil {
				continue
			}

			payloadType := "place"

			if result.Job != nil {
				switch result.Job.(type) {
				case *gmaps.GmapJob:
					payloadType = "search"
				case *gmaps.PlaceJob:
					payloadType = "place"
				}
			}

			var userID string
			var organizationID string
			var parentJobID string
			var actualJob scrapemate.IJob = result.Job

			if wrapper, ok := result.Job.(*jobWrapper); ok {
				actualJob = wrapper.IJob
			}

			// keep base place results; enrichment happens via merge/update

			if job, ok := actualJob.(*gmaps.GmapJob); ok {
				userID = job.OwnerID
				organizationID = job.OrganizationID

				rootParentID, err := r.getRootParentJobID(ctx, job.GetID())
				if err != nil {
					log.Error(fmt.Sprintf("Error getting root parent job ID: %v", err))
					parentJobID = job.GetID()
				} else {
					parentJobID = rootParentID
				}
			} else if job, ok := actualJob.(*gmaps.PlaceJob); ok {
				userID = job.OwnerID
				organizationID = job.OrganizationID

				rootParentID, err := r.getRootParentJobID(ctx, job.GetID())
				if err != nil {
					log.Error(fmt.Sprintf("Error getting root parent job ID: %v", err))
					parentJobID = job.ParentID
				} else {
					parentJobID = rootParentID
				}
			}

			isDuplicate, err := r.checkDuplicateURL(ctx, entry.Link, userID, organizationID)
			if err != nil {
				log.Error(fmt.Sprintf("Error checking duplicate URL: %v", err))
				continue
			}

			if isDuplicate {
				continue
			}

			dbEntry := dbEntry{
				UserID:            userID,
				OrganizationID:    organizationID,
				ParentID:          parentJobID,
				Link:              entry.Link,
				PayloadType:       payloadType,
				Title:             entry.Title,
				Category:          entry.Category,
				Address:           entry.Address,
				Website:           entry.WebSite,
				Phones:            phoneToPhones(entry.Phone, entry.CompleteAddress.Country),
				Emails:            entry.Emails,
				Latitude:          entry.Latitude,
				Longitude:         entry.Longtitude,
				SocieteDirigeants: strings.Join(entry.SocieteDirigeants, ","),
				SocieteSiren:      entry.SocieteSiren,
				SocieteForme:      entry.SocieteForme,
				SocieteEffectif:   "",
				SocieteCreation:   entry.SocieteCreation,
				SocieteCloture:    entry.SocieteCloture,
				SocieteLink:       entry.SocieteLink,
				SocieteDiffusion:  entry.SocieteDiffusion,
			}

			key := userID + "|" + organizationID + "|" + entry.Link
			if _, ok := r.inMemoryIndex[key]; ok {
				// Duplicate within the same batch - skip silently
				continue
			}
			r.inMemoryIndex[key] = len(buff)
			buff = append(buff, dbEntry)

			if len(buff) >= maxBatchSize {
				err := r.batchSave(ctx, buff)
				if err != nil {
					return err
				}

				buff = buff[:0]
				r.inMemoryIndex = make(map[string]int)
				lastSave = time.Now().UTC()
			}
		case <-ticker.C:
			if len(buff) > 0 && time.Since(lastSave) >= time.Second*5 {
				if err := r.batchSave(ctx, buff); err != nil {
					return err
				}
				buff = buff[:0]
				r.inMemoryIndex = make(map[string]int)
				lastSave = time.Now().UTC()
			}
		case <-ctx.Done():
			if len(buff) > 0 {
				saveCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				defer cancel()
				_ = r.batchSave(saveCtx, buff)
			}
			return ctx.Err()
		}
	}
}

func (r *resultWriter) batchSave(ctx context.Context, entries []dbEntry) error {
	if len(entries) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO results (
			parent_id, user_id, organization_id, link, payload_type,
			title, category, address, website, phones, emails, latitude, longitude,
			societe_dirigeants, societe_siren, societe_forme,
			societe_effectif, societe_creation, societe_cloture, societe_link, societe_diffusion
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
			$13, $14, $15, $16, $17, $18, $19, $20, $21
		)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, entry := range entries {
		_, err := stmt.ExecContext(ctx,
			entry.ParentID, entry.UserID, entry.OrganizationID, entry.Link, entry.PayloadType,
			entry.Title, entry.Category, entry.Address, entry.Website, entry.Phones, entry.Emails,
			entry.Latitude, entry.Longitude, entry.SocieteDirigeants, entry.SocieteSiren, entry.SocieteForme,
			entry.SocieteEffectif, entry.SocieteCreation, entry.SocieteCloture, entry.SocieteLink, entry.SocieteDiffusion,
		)
		if err != nil {
			return fmt.Errorf("failed to insert entry: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Call revalidation API for unique user IDs
	r.notifyRevalidation(ctx, entries)

	return nil
}
