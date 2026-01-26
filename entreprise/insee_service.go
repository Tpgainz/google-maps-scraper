package entreprise

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	inseeBaseURL      = "https://api.insee.fr/api-sirene/3.11"
	inseeSirenEndpoint = "/siren"
	inseeSiretEndpoint = "/siret"
)

type INSEEService struct {
	apiKey string
	client *http.Client
}

var (
	inseeServiceInstance *INSEEService
	inseeServiceOnce     sync.Once
)

type INSEEResponse struct {
	Etablissements []map[string]interface{} `json:"etablissements,omitempty"`
}

type ScoredResult struct {
	Etablissement map[string]interface{}
	Score         float64
	Source        string
}

func NewINSEEService(apiKey string) *INSEEService {
	inseeServiceOnce.Do(func() {
		inseeServiceInstance = &INSEEService{
			apiKey: apiKey,
			client: &http.Client{
				Timeout: 30 * time.Second,
				Transport: &http.Transport{
					MaxIdleConns:        10,
					IdleConnTimeout:     30 * time.Second,
					DisableKeepAlives:   false,
					MaxIdleConnsPerHost: 2,
				},
			},
		}
	})
	return inseeServiceInstance
}

func (s *INSEEService) SearchCompany(companyName, address string) (*SearchResult, error) {
	var addressUpper string
	if address != "" {
		addressUpper = strings.ToUpper(address)
	}
	query := generateSearchQuery(companyName, addressUpper)
	
	log.Printf("INSEE search for '%s' with query: %s", companyName, query)
	
	result, err := s.searchSiret(query)
	if err != nil {
		log.Printf("INSEE search failed: %v", err)
		return &SearchResult{
			Success: false,
			Error:   err.Error(),
		}, err
	}
	
	if result == nil || len(result.Etablissements) == 0 {
		log.Printf("No INSEE results found for company: %s", companyName)
		return &SearchResult{
			Success:      true,
			Data:         []CompanyInfo{},
			TotalResults: 0,
		}, nil
	}
	
	log.Printf("INSEE returned %d establishments", len(result.Etablissements))
	
	var allResults []ScoredResult
	hasAddress := address != ""
	
	for _, etab := range result.Etablissements {
		matchesName := matchesByName(etab, companyName)
		
		source := "nom"
		if matchesName && hasAddress {
			source = "nom+adresse"
		} else if matchesName {
			source = "nom"
		} else {
			source = "adresse"
		}
		
		score := scoreResult(etab, companyName, address)
		allResults = append(allResults, ScoredResult{
			Etablissement: etab,
			Score:         score,
			Source:        source,
		})
	}
	
	if len(allResults) == 0 {
		return &SearchResult{
			Success:      true,
			Data:         []CompanyInfo{},
			TotalResults: 0,
		}, nil
	}
	
	for i := 0; i < len(allResults)-1; i++ {
		for j := i + 1; j < len(allResults); j++ {
			if allResults[j].Score > allResults[i].Score {
				allResults[i], allResults[j] = allResults[j], allResults[i]
			}
		}
	}
	
	if len(allResults) == 0 || allResults[0].Score < MIN_SCORE_THRESHOLD {
		log.Printf("No results above threshold (%.2f) for company: %s", MIN_SCORE_THRESHOLD, companyName)
		return &SearchResult{
			Success:      true,
			Data:         []CompanyInfo{},
			TotalResults: 0,
		}, nil
	}
	
	var results []CompanyInfo
	for _, scored := range allResults {
		companyInfo := s.transformEtablissementToCompanyInfo(scored.Etablissement)
		companyInfo.MatchScore = scored.Score
		results = append(results, companyInfo)
	}
	
	return &SearchResult{
		Success:      true,
		Data:         results,
		TotalResults: len(results),
	}, nil
}

func (s *INSEEService) searchSiret(query string) (*INSEEResponse, error) {
	encodedQuery := url.QueryEscape(query)
	searchURL := fmt.Sprintf("%s%s?q=%s&nombre=200",
		inseeBaseURL, inseeSiretEndpoint, encodedQuery)
	
	req, err := http.NewRequest("GET", searchURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating search request: %w", err)
	}
	
	req.Header.Set("X-INSEE-Api-Key-Integration", s.apiKey)
	req.Header.Set("Accept", "application/json;charset=utf-8")
	
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing search request: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Printf("INSEE search failed: status %d, body: %s", resp.StatusCode, string(bodyBytes))
		return nil, fmt.Errorf("search failed: status %d", resp.StatusCode)
	}
	
	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("error decoding search response: %w", err)
	}
	
	etablissements, ok := data["etablissements"].([]interface{})
	if !ok || len(etablissements) == 0 {
		return &INSEEResponse{
			Etablissements: []map[string]interface{}{},
		}, nil
	}
	
	result := make([]map[string]interface{}, 0, len(etablissements))
	for _, etab := range etablissements {
		if etabMap, ok := etab.(map[string]interface{}); ok {
			result = append(result, etabMap)
		}
	}
	
	return &INSEEResponse{
		Etablissements: result,
	}, nil
}

func (s *INSEEService) transformEtablissementToCompanyInfo(etab map[string]interface{}) CompanyInfo {
	result := CompanyInfo{
		SocieteDirigeants: []string{},
	}
	
	siret, _ := etab["siret"].(string)
	siren, _ := etab["siren"].(string)
	if siren == "" && len(siret) >= 9 {
		siren = siret[:9]
	}
	result.SocieteSiren = siren
	
	ul, ok := etab["uniteLegale"].(map[string]interface{})
	if ok {
		denomination, _ := ul["denominationUniteLegale"].(string)
		result.SocieteNom = denomination
		
		result.SocieteForme, _ = ul["categorieJuridiqueUniteLegale"].(string)
		result.SocieteCreation, _ = ul["dateCreationUniteLegale"].(string)
		result.SocieteCloture, _ = ul["dateDernierTraitementUniteLegale"].(string)
		
		nomUsage, _ := ul["nomUsageUniteLegale"].(string)
		nom, _ := ul["nomUniteLegale"].(string)
		prenom, _ := ul["prenomUsuelUniteLegale"].(string)
		
		dirigeantName := ""
		if nomUsage != "" {
			dirigeantName = nomUsage
		} else if nom != "" {
			dirigeantName = nom
		}
		
		if prenom != "" {
			if len(prenom) > 0 {
				prenomFormatted := strings.ToUpper(string(prenom[0])) + strings.ToLower(prenom[1:])
				if dirigeantName != "" {
					result.SocieteDirigeants = []string{dirigeantName + " " + prenomFormatted}
				} else {
					result.SocieteDirigeants = []string{prenomFormatted}
				}
			}
		} else if dirigeantName != "" {
			result.SocieteDirigeants = []string{dirigeantName}
		}
	}
	
	statutDiffusion, _ := etab["statutDiffusionEtablissement"].(string)
	result.SocieteDiffusion = statutDiffusion == "O"
	
	if result.SocieteSiren != "" && result.SocieteNom != "" {
		result.PappersURL = CreatePappersURL(result.SocieteNom, result.SocieteSiren)
		result.SocieteLink = fmt.Sprintf("https://www.inpi.fr/recherche-entreprise/entreprise/%s", result.SocieteSiren)
	}
	
	return result
}
