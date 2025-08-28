package gmaps

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/gosom/google-maps-scraper/exiter"
	"github.com/gosom/scrapemate"
	"github.com/playwright-community/playwright-go"
)

type SocieteJobOptions func(*SocieteJob)

type SocieteJob struct {
	scrapemate.Job
	OwnerID       string
	OrganizationID string
	ExtractEmail bool
	ExitMonitor  exiter.Exiter
}

func NewSocieteJob(langCode, u, ownerID, organizationID string, extractEmail bool, opts ...SocieteJobOptions) *SocieteJob {
	const (
		defaultPrio       = scrapemate.PriorityMedium
		defaultMaxRetries = 3
	)

	job := SocieteJob{
		Job: scrapemate.Job{
			ID:         uuid.New().String(),
			Method:     "GET",
			URL:        u,
			URLParams:  map[string]string{"hl": langCode},
			MaxRetries: defaultMaxRetries,
			Priority:   defaultPrio,
		},
	}

	job.ExtractEmail = extractEmail
	job.OwnerID = ownerID
	job.OrganizationID = organizationID
	for _, opt := range opts {
		opt(&job)
	}

	return &job
}

func WithSocieteJobExitMonitor(exitMonitor exiter.Exiter) SocieteJobOptions {
	return func(j *SocieteJob) {
		j.ExitMonitor = exitMonitor
	}
}

func (j *SocieteJob) Process(_ context.Context, resp *scrapemate.Response) (any, []scrapemate.IJob, error) {
	defer func() {
		resp.Document = nil
		resp.Body = nil
		resp.Meta = nil
	}()

	raw, ok := resp.Meta["json"].([]byte)
	if !ok {
		return nil, nil, fmt.Errorf("could not convert to []byte")
	}

	// Créer une entrée à partir des données JSON
	entry := &Entry{
		ID:              j.ID,
		PopularTimes:    make(map[string]map[int]int),
		ReviewsPerRating: make(map[int]int),
		OpenHours:       make(map[string][]string),
	}

	// Analyser les données JSON pour extraire les informations de la société
	var societeData map[string]interface{}
	if err := json.Unmarshal(raw, &societeData); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal societe data: %w", err)
	}

	// Extraire les informations de la société à partir de societeData
	// Ceci dépendra de la structure exacte des données JSON
	// Exemple:
	if name, ok := extractStringValue(societeData, "name"); ok {
		entry.Title = name
	}
	
	if address, ok := extractStringValue(societeData, "address"); ok {
		entry.Address = address
	}
	
	if phone, ok := extractStringValue(societeData, "phone"); ok {
		entry.Phone = phone
	}
	
	if website, ok := extractStringValue(societeData, "website"); ok {
		entry.WebSite = website
	}
	
	if description, ok := extractStringValue(societeData, "description"); ok {
		entry.Description = description
	}
	
	// Handle categories
	if categoriesVal, ok := societeData["categories"]; ok {
		if categoriesArr, ok := categoriesVal.([]interface{}); ok {
			entry.Categories = make([]string, 0, len(categoriesArr))
			for _, cat := range categoriesArr {
				if catStr, ok := cat.(string); ok {
					entry.Categories = append(entry.Categories, catStr)
				}
			}
			
			if len(entry.Categories) > 0 {
				entry.Category = entry.Categories[0]
			}
		}
	}
	
	// Extract opening hours if available
	if hoursVal, ok := societeData["openingHours"]; ok {
		if hoursMap, ok := hoursVal.(map[string]interface{}); ok {
			for day, hours := range hoursMap {
				if hoursStr, ok := hours.(string); ok {
					entry.OpenHours[day] = []string{hoursStr}
				}
			}
		}
	}
	
	// Extract social links and other data
	if socialLinks, ok := societeData["socialLinks"].(map[string]interface{}); ok {
		// Store these links somewhere appropriate in the Entry structure
		// For example, we could add them to a Description field
		socialInfo := "\nSocial Links:\n"
		for platform, link := range socialLinks {
			if linkStr, ok := link.(string); ok && linkStr != "" {
				socialInfo += platform + ": " + linkStr + "\n"
			}
		}
		if len(socialInfo) > 20 { // Only append if we found some links
			entry.Description += socialInfo
		}
	}
	
	// Try to extract latitude and longitude
	if lat, ok := extractFloatValue(societeData, "latitude"); ok {
		entry.Latitude = lat
	}

	if lng, ok := extractFloatValue(societeData, "longitude"); ok {
		entry.Longtitude = lng // Note: Field is spelled "Longtitude" in the struct
	}

	// Try to extract review info
	if rating, ok := extractFloatValue(societeData, "rating"); ok {
		entry.ReviewRating = rating
	}

	if reviewCount, ok := extractIntValue(societeData, "reviewCount"); ok {
		entry.ReviewCount = reviewCount
	}

	// Extract SIRET or other business identifiers if available
	if siret, ok := extractStringValue(societeData, "siret"); ok {
		if entry.Description != "" {
			entry.Description += "\n"
		}
		entry.Description += "SIRET: " + siret
	}
	
	// Extraire d'autres informations comme SIRET, catégories, etc.
	
	// Si extraction d'email est demandée et qu'un site web est disponible
	if j.ExtractEmail && entry.IsWebsiteValidForEmail() {
		opts := []EmailExtractJobOptions{}
		if j.ExitMonitor != nil {
			opts = append(opts, WithEmailJobExitMonitor(j.ExitMonitor))
		}

		emailJob := NewEmailJob(j.ID, entry, j.OwnerID, j.OrganizationID, opts...)

		return nil, []scrapemate.IJob{emailJob}, nil
	} else if j.ExitMonitor != nil {
		j.ExitMonitor.IncrPlacesCompleted(1)
	}

	return entry, nil, nil
}

func (j *SocieteJob) BrowserActions(_ context.Context, page playwright.Page) scrapemate.Response {
	var resp scrapemate.Response

	pageResponse, err := page.Goto(j.GetURL(), playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateDomcontentloaded,
	})

	if err != nil {
		resp.Error = err
		return resp
	}

	// Gérer les cookies ou autres popups si nécessaire
	if err = clickRejectCookiesIfRequired(page); err != nil {
		resp.Error = err
		return resp
	}

	const defaultTimeout = 5000

	err = page.WaitForURL(page.URL(), playwright.PageWaitForURLOptions{
		WaitUntil: playwright.WaitUntilStateDomcontentloaded,
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

	// Exécuter un script JavaScript pour extraire les données
	// Vous devrez adapter ce script en fonction de la structure de la page
	rawI, err := page.Evaluate(societeJS)
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

	// Traiter les données brutes si nécessaire
	raw = strings.TrimSpace(raw)

	if resp.Meta == nil {
		resp.Meta = make(map[string]any)
	}

	resp.Meta["json"] = []byte(raw)

	return resp
}

// Fonction utilitaire pour extraire une valeur string d'une map imbriquée
func extractStringValue(data map[string]interface{}, path string) (string, bool) {
	parts := strings.Split(path, ".")
	current := data
	
	for i, part := range parts {
		if i == len(parts)-1 {
			if val, ok := current[part].(string); ok {
				return val, true
			}
			return "", false
		}
		
		if next, ok := current[part].(map[string]interface{}); ok {
			current = next
		} else {
			return "", false
		}
	}
	
	return "", false
}

// Fonction utilitaire pour extraire une valeur float d'une map imbriquée
func extractFloatValue(data map[string]interface{}, path string) (float64, bool) {
	parts := strings.Split(path, ".")
	current := data
	
	for i, part := range parts {
		if i == len(parts)-1 {
			switch val := current[part].(type) {
			case float64:
				return val, true
			case int:
				return float64(val), true
			case string:
				if f, err := strconv.ParseFloat(val, 64); err == nil {
					return f, true
				}
			}
			return 0, false
		}
		
		if next, ok := current[part].(map[string]interface{}); ok {
			current = next
		} else {
			return 0, false
		}
	}
	
	return 0, false
}

// Fonction utilitaire pour extraire une valeur int d'une map imbriquée
func extractIntValue(data map[string]interface{}, path string) (int, bool) {
	parts := strings.Split(path, ".")
	current := data
	
	for i, part := range parts {
		if i == len(parts)-1 {
			switch val := current[part].(type) {
			case int:
				return val, true
			case float64:
				return int(val), true
			case string:
				if i, err := strconv.Atoi(val); err == nil {
					return i, true
				}
			}
			return 0, false
		}
		
		if next, ok := current[part].(map[string]interface{}); ok {
			current = next
		} else {
			return 0, false
		}
	}
	
	return 0, false
}

// Script JavaScript pour extraire les données de la société
// Vous devrez adapter ce script en fonction de la structure de la page
const societeJS = `
function extractSocieteData() {
  try {
    // Exemple: extraire les données d'une variable globale
    // return window.SOCIETE_DATA;
    
    // Ou construire un objet à partir des éléments de la page
    const data = {
      name: document.querySelector('.company-name')?.textContent?.trim() || '',
      address: document.querySelector('.company-address')?.textContent?.trim() || '',
      phone: document.querySelector('.company-phone')?.textContent?.trim() || '',
      website: document.querySelector('.company-website')?.href || '',
      description: document.querySelector('.company-description')?.textContent?.trim() || '',
      siret: document.querySelector('.company-siret')?.textContent?.trim() || '',
      
      // Extraire les catégories
      categories: Array.from(document.querySelectorAll('.company-categories .category')).map(el => el.textContent?.trim() || ''),
      
      // Extraire les heures d'ouverture
      openingHours: Object.fromEntries(
        Array.from(document.querySelectorAll('.company-hours .day-hours')).map(el => [
          el.querySelector('.day')?.textContent?.trim() || '',
          el.querySelector('.hours')?.textContent?.trim() || ''
        ])
      ),
      
      // Extraire les liens sociaux
      socialLinks: {
        facebook: document.querySelector('a[href*="facebook.com"]')?.href || '',
        twitter: document.querySelector('a[href*="twitter.com"]')?.href || '',
        linkedin: document.querySelector('a[href*="linkedin.com"]')?.href || '',
        instagram: document.querySelector('a[href*="instagram.com"]')?.href || ''
      }
    };
    
    return JSON.stringify(data);
  } catch (error) {
    return JSON.stringify({ error: error.message });
  }
}

extractSocieteData();
`