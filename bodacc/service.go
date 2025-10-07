package bodacc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gosom/scrapemate"
	"github.com/gosom/scrapemate/scrapemateapp"
)

type BodaccService struct {
	baseURL  string
	dataset  string
	client   *http.Client
}

func NewBodaccService() *BodaccService {
	return &BodaccService{
		baseURL: "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1",
		dataset: "annonces-commerciales",
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
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
		return s.enrichWithDirectors(primaryResult), nil
	}

	log.Println("Aucun résultat trouvé, tentative de recherche avec adresse simplifiée")
	fallbackResult, err := s.executeFallbackSearch(companyNameForSearch, address, departmentNumber)
	if err != nil {
		return nil, err
	}

	return s.enrichWithDirectors(fallbackResult), nil
}

func (s *BodaccService) executePrimarySearch(companyName, companyNameForSearch, refinedAddress, departmentNumber string) (*BodaccSearchResult, error) {
	searchQuery := fmt.Sprintf(`search(listepersonnes, "%s") AND search(commercant, "%s") or search(listepersonnes, "%s") AND search(commercant, "%s")`,
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
	params.Add("where", searchQuery)
	if departmentNumber != "" {
		params.Add("refine", fmt.Sprintf(`numerodepartement:"%s"`, departmentNumber))
	}
	params.Add("limit", "20")

	return fmt.Sprintf("%s/catalog/datasets/%s/records?%s", s.baseURL, s.dataset, params.Encode())
}

func (s *BodaccService) hasResults(result *BodaccSearchResult) bool {
	return result.Success && result.Data != nil && len(result.Data) > 0
}

func (s *BodaccService) filterResultsByCity(result *BodaccSearchResult, address string) *BodaccSearchResult {
	if result.Data == nil {
		return result
	}

	addressParts := strings.Split(address, ",")
	if len(addressParts) < 2 {
		return result
	}

	cityParts := strings.Fields(addressParts[1])
	if len(cityParts) < 3 {
		return result
	}

	targetCity := strings.ToLower(strings.TrimSpace(cityParts[2]))
	if targetCity == "" {
		return result
	}

	var filteredResults []BodaccCompanyInfo
	for _, item := range result.Data {
		if strings.ToLower(item.City) == targetCity {
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
	var dpcRecords []BodaccRawResult
	var nonDpcRecords []BodaccRawResult

	for _, result := range results {
		if result.Familleavis == "dpc" {
			dpcRecords = append(dpcRecords, result)
		} else {
			nonDpcRecords = append(nonDpcRecords, result)
		}
	}

	dpcClosureDates := s.extractDpcClosureDates(dpcRecords)

	if len(nonDpcRecords) > 0 {
		var transformedResults []BodaccCompanyInfo
		for _, result := range nonDpcRecords {
			transformedResults = append(transformedResults, TransformResult(result, dpcClosureDates))
		}
		return transformedResults
	}

	if len(dpcRecords) > 0 {
		return []BodaccCompanyInfo{TransformResult(dpcRecords[0], dpcClosureDates)}
	}

	return []BodaccCompanyInfo{}
}

func (s *BodaccService) extractDpcClosureDates(dpcRecords []BodaccRawResult) map[string]string {
	closureDates := make(map[string]string)

	for _, result := range dpcRecords {
		siren := strings.ReplaceAll(result.Registre[0], " ", "")
		dateCloture := ParseDepot(result.Depot)

		if dateCloture != "" {
			closureDates[siren] = dateCloture
		}
	}

	return closureDates
}

func (s *BodaccService) enrichWithDirectors(result *BodaccSearchResult) *BodaccSearchResult {
	if result.Data == nil {
		return result
	}

	enrichedData := make([]BodaccCompanyInfo, 0, len(result.Data))
	
	for _, company := range result.Data {
		if len(company.SocieteDirigeants) == 0 && company.PappersURL != "" {
			directors := s.scrapeDirectorsFromPappers(company.PappersURL)
			company.SocieteDirigeants = directors
		}
		enrichedData = append(enrichedData, company)
	}

	return &BodaccSearchResult{
		Success:      result.Success,
		Data:         enrichedData,
		Error:        result.Error,
		TotalResults: result.TotalResults,
	}
}

func (s *BodaccService) scrapeDirectorsFromPappers(pappersURL string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	directorsWriter := NewDirectorsWriter()
	app, err := s.createScrapemateApp(directorsWriter)
	if err != nil {
		log.Printf("Failed to create scrapemate app: %v", err)
		return []string{}
	}
	defer app.Close()

	job := NewPappersScraperJob(&BodaccCompanyInfo{PappersURL: pappersURL})
	
	err = app.Start(ctx, job)
	if err != nil {
		log.Printf("Failed to execute pappers scraping job: %v", err)
		return []string{}
	}

	return directorsWriter.GetDirectors()
}

func (s *BodaccService) createScrapemateApp(writer scrapemate.ResultWriter) (*scrapemateapp.ScrapemateApp, error) {
	opts := []func(*scrapemateapp.Config) error{
		scrapemateapp.WithConcurrency(1),
		scrapemateapp.WithExitOnInactivity(30 * time.Second),
		scrapemateapp.WithJS(scrapemateapp.DisableImages()),
	}

	writers := []scrapemate.ResultWriter{writer}
	cfg, err := scrapemateapp.NewConfig(writers, opts...)
	if err != nil {
		return nil, err
	}

	return scrapemateapp.NewScrapeMateApp(cfg)
}
