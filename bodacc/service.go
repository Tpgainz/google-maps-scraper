package bodacc

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

type BodaccService struct {
	baseURL  string
	dataset  string
	client   *http.Client
}

var (
	bodaccServiceInstance *BodaccService
	bodaccServiceOnce     sync.Once
)

func NewBodaccService() *BodaccService {
	bodaccServiceOnce.Do(func() {
		bodaccServiceInstance = &BodaccService{
			baseURL: "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1",
			dataset: "annonces-commerciales",
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
	return bodaccServiceInstance
}

func (s *BodaccService) SearchCompany(companyName, address string) (*BodaccSearchResult, error) {
	departmentNumber := ExtractDepartmentNumber(address)
	refinedAddress := RefineAddress(address)
	companyNameForSearch := ProcessForSearch(companyName)

	log.Printf("Company name for search: %s, refined address: %s, company name for search: %s", 
		companyName, refinedAddress, companyNameForSearch)

	primaryResult, err := s.executePrimarySearch(companyName, companyNameForSearch, refinedAddress, departmentNumber)
	if err != nil {
		return nil, err
	}

	if s.hasResults(primaryResult) {
		return primaryResult, nil
	}

	log.Println("Aucun résultat trouvé, tentative de recherche avec adresse simplifiée")
	fallbackResult, err := s.executeFallbackSearch(companyNameForSearch, address, departmentNumber)
	if err != nil {
		return nil, err
	}

	return fallbackResult, nil
}

func (s *BodaccService) executePrimarySearch(companyName, companyNameForSearch, refinedAddress, departmentNumber string) (*BodaccSearchResult, error) {
	searchQuery := fmt.Sprintf(`search(listepersonnes, "%s") AND search(commercant, "%s") OR search(listepersonnes, "%s") AND search(commercant, "%s")`,
		refinedAddress, companyName, refinedAddress, companyNameForSearch)

	searchURL := s.buildSearchURL(searchQuery, departmentNumber)

	log.Printf("Recherche BODACC par nom d'entreprise et adresse complète: %s, %s, %s, %s", 
		companyName, refinedAddress, departmentNumber, searchURL)

	return s.executeSearch(searchURL)
}

func (s *BodaccService) executeFallbackSearch(companyNameForSearch, address, departmentNumber string) (*BodaccSearchResult, error) {
	simplifiedAddress := SimplifyAddress(address)
	likeConditions := CreateLikeConditions(companyNameForSearch)
	fallbackSearchQuery := fmt.Sprintf(`search(listepersonnes, "%s") AND (%s)`, simplifiedAddress, likeConditions)

	fallbackURL := s.buildSearchURL(fallbackSearchQuery, departmentNumber)

	log.Printf("Recherche BODACC de fallback avec adresse simplifiée: %s, %s, %s, %s", 
		companyNameForSearch, simplifiedAddress, likeConditions, fallbackURL)

	result, err := s.executeSearch(fallbackURL)
	if err != nil {
		return nil, err
	}

	if s.hasResults(result) {
		return s.filterResultsByCity(result, address), nil
	}

	return result, nil
}

func (s *BodaccService) buildSearchURL(searchQuery, departmentNumber string) string {
	params := url.Values{}
	params.Set("where", searchQuery)
	if departmentNumber != "" {
		params.Set("refine", fmt.Sprintf(`numerodepartement:"%s"`, departmentNumber))
	}
	params.Set("limit", "20")

	return fmt.Sprintf("%s/catalog/datasets/%s/records?%s", s.baseURL, s.dataset, params.Encode())
}

func (s *BodaccService) hasResults(result *BodaccSearchResult) bool {
	return result.Success && result.Data != nil && len(result.Data) > 0
}

func (s *BodaccService) filterResultsByCity(result *BodaccSearchResult, address string) *BodaccSearchResult {
	if result.Data == nil || len(result.Data) == 0 {
		return result
	}

	targetCity := s.extractCityFromAddress(address)
	if targetCity == "" {
		return result
	}

	var filteredResults []BodaccCompanyInfo
	for _, item := range result.Data {
		if strings.ToLower(strings.TrimSpace(item.City)) == targetCity {
			filteredResults = append(filteredResults, item)
		}
	}

	if len(filteredResults) > 0 {
		return &BodaccSearchResult{
			Success:      result.Success,
			Data:         []BodaccCompanyInfo{filteredResults[0]},
			TotalResults: result.TotalResults,
		}
	}

	return &BodaccSearchResult{
		Success:      result.Success,
		Data:         []BodaccCompanyInfo{},
		TotalResults: result.TotalResults,
	}
}

func (s *BodaccService) extractCityFromAddress(address string) string {
	addressParts := strings.Split(address, ",")
	if len(addressParts) < 2 {
		return ""
	}

	cityParts := strings.Fields(addressParts[1])
	if len(cityParts) < 3 {
		return ""
	}

	targetCity := strings.ToLower(strings.TrimSpace(cityParts[2]))
	return targetCity
}

func (s *BodaccService) executeSearch(url string) (*BodaccSearchResult, error) {
	response, err := s.makeAPIRequest(url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("erreur lecture réponse: %w", err)
	}

	var data BodaccAPIResponse
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("erreur parsing JSON: %w", err)
	}

	log.Printf("Réponse BODACC reçue: total_count=%d, results_length=%d", 
		data.TotalCount, len(data.Results))

	if data.Results == nil {
		return &BodaccSearchResult{
			Success:      true,
			Data:         []BodaccCompanyInfo{},
			TotalResults: 0,
		}, nil
	}

	results := s.processAPIResults(data.Results)

	log.Printf("Résultats BODACC: %+v", results)

	return &BodaccSearchResult{
		Success:      true,
		Data:         results,
		TotalResults: data.TotalCount,
	}, nil
}

func (s *BodaccService) makeAPIRequest(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("erreur création requête: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "LeadExpress/1.0")

	response, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("erreur requête HTTP: %w", err)
	}

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		
		log.Printf("Erreur API BODACC: status=%d, statusText=%s, errorBody=%s, url=%s", 
			response.StatusCode, response.Status, string(body), url)
		
		return nil, fmt.Errorf("erreur API BODACC: %d %s - %s", 
			response.StatusCode, response.Status, string(body))
	}

	return response, nil
}

func (s *BodaccService) processAPIResults(results []BodaccRawResult) []BodaccCompanyInfo {
	if len(results) == 0 {
		return []BodaccCompanyInfo{}
	}

	var dpcRecords []BodaccRawResult
	var nonDpcRecords []BodaccRawResult

	for _, result := range results {
		if len(result.Registre) == 0 {
			continue
		}
		if result.Familleavis == "dpc" {
			dpcRecords = append(dpcRecords, result)
		} else {
			nonDpcRecords = append(nonDpcRecords, result)
		}
	}

	dpcClosureDates := s.extractDpcClosureDates(dpcRecords)

	if len(nonDpcRecords) > 0 {
		transformedResults := make([]BodaccCompanyInfo, 0, len(nonDpcRecords))
		for _, result := range nonDpcRecords {
			if len(result.Registre) > 0 && result.Registre[0] != "" {
				transformedResults = append(transformedResults, TransformResult(result, dpcClosureDates))
			}
		}
		if len(transformedResults) > 0 {
			return transformedResults
		}
	}

	if len(dpcRecords) > 0 {
		for _, dpcRecord := range dpcRecords {
			if len(dpcRecord.Registre) > 0 && dpcRecord.Registre[0] != "" {
				return []BodaccCompanyInfo{TransformResult(dpcRecord, dpcClosureDates)}
			}
		}
	}

	return []BodaccCompanyInfo{}
}

func (s *BodaccService) extractDpcClosureDates(dpcRecords []BodaccRawResult) map[string]string {
	closureDates := make(map[string]string)

	for _, result := range dpcRecords {
		if len(result.Registre) == 0 || result.Registre[0] == "" {
			continue
		}
		siren := strings.ReplaceAll(result.Registre[0], " ", "")
		if siren == "" {
			continue
		}
		dateCloture := ParseDepot(result.Depot)
		if dateCloture != "" {
			closureDates[siren] = dateCloture
		}
	}

	return closureDates
}
