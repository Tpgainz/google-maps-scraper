package entreprise

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	inpiCompaniesEndpoint = "/api/companies"
	inpiSSOLoginEndpoint  = "/api/sso/login"
	inpiMinScoreThreshold = 200.0
)


type INPIService struct {
	baseURL      string
	authURL      string
	username     string
	password     string
	token        string
	tokenExpiry  time.Time
	client       *http.Client
	tokenMutex   sync.RWMutex
	useDemoEnv   bool
}

var (
	inpiServiceInstance *INPIService
	inpiServiceOnce     sync.Once
)

type INPIAuthRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type INPIAuthResponse struct {
	Token string `json:"token"`
	User  struct {
		Roles              []string `json:"roles"`
		ID                 int      `json:"id"`
		Email              string   `json:"email"`
		Firstname          string   `json:"firstname"`
		Lastname           string   `json:"lastname"`
		CivilityCode       string   `json:"civilityCode"`
		Address1           string   `json:"address1"`
		ZipCode            string   `json:"zipCode"`
		City               string   `json:"city"`
		CountryCode        string   `json:"countryCode"`
		HasCompany         bool     `json:"hasCompany"`
		IsManager          bool     `json:"isManager"`
		OfficePhone        string   `json:"officePhone"`
		LastLogin          string   `json:"lastLogin"`
		Active             bool     `json:"active"`
		CorrespondenceEmails []string `json:"correspondenceEmails"`
	} `json:"user"`
}

type INPISearchRequest struct {
	CompanyName string `json:"companyName,omitempty"`
	Address     string `json:"address,omitempty"`
	SIREN       string `json:"siren,omitempty"`
}

type INPIFormality struct {
	Formality struct {
		Content struct {
			PersonneMorale *struct {
				Identite struct {
					Entreprise struct {
						Siren         string `json:"siren"`
						Denomination  string `json:"denomination"`
						FormeJuridique string `json:"formeJuridique"`
						DateImmat     string `json:"dateImmat"`
					} `json:"entreprise"`
				} `json:"identite"`
				AdresseEntreprise struct {
					Adresse struct {
						CodePostal string `json:"codePostal"`
						Commune    string `json:"commune"`
						Voie       string `json:"voie"`
						NumVoie    string `json:"numVoie"`
						TypeVoie   string `json:"typeVoie"`
					} `json:"adresse"`
				} `json:"adresseEntreprise"`
				DetailCessationEntreprise *struct {
					DateRadiation string `json:"dateRadiation"`
				} `json:"detailCessationEntreprise"`
			} `json:"personneMorale"`
			PersonnePhysique *struct {
				Identite struct {
					Entrepreneur struct {
						DescriptionPersonne struct {
							Nom     string   `json:"nom"`
							Prenoms []string `json:"prenoms"`
						} `json:"descriptionPersonne"`
					} `json:"entrepreneur"`
					Entreprise struct {
						Siren         string `json:"siren"`
						FormeJuridique string `json:"formeJuridique"`
						DateImmat     string `json:"dateImmat"`
					} `json:"entreprise"`
				} `json:"identite"`
				AdresseEntreprise struct {
					Adresse struct {
						CodePostal string `json:"codePostal"`
						Commune    string `json:"commune"`
						Voie       string `json:"voie"`
						NumVoie    string `json:"numVoie"`
						TypeVoie   string `json:"typeVoie"`
					} `json:"adresse"`
				} `json:"adresseEntreprise"`
				DetailCessationEntreprise *struct {
					DateRadiation string `json:"dateRadiation"`
				} `json:"detailCessationEntreprise"`
			} `json:"personnePhysique"`
			NatureCreation *struct {
				DateCreation string `json:"dateCreation"`
			} `json:"natureCreation"`
		} `json:"content"`
		Siren          string `json:"siren"`
		FormeJuridique string `json:"formeJuridique"`
		TypePersonne   string `json:"typePersonne"`
	} `json:"formality"`
	Siren string `json:"siren"`
}

type INPICompanyResponse struct {
	SIREN        string
	CompanyName  string
	LegalForm    string
	CreationDate string
	ClosureDate  string
	Directors    []string
	Address      string
	City         string
	PostalCode   string
	Enseignes    []string
}

func NewINPIService(username, password string, useDemoEnv bool) *INPIService {
	inpiServiceOnce.Do(func() {
		baseURL := "https://registre-national-entreprises.inpi.fr"
		authURL := "https://registre-national-entreprises.inpi.fr/api/sso/login"
		
		if useDemoEnv {
			baseURL = "https://registre-national-entreprises-pprod.inpi.fr"
			authURL = "https://registre-national-entreprises-pprod.inpi.fr/api/sso/login"
		}

		inpiServiceInstance = &INPIService{
			baseURL:  baseURL,
			authURL:  authURL,
			username: username,
			password: password,
			useDemoEnv: useDemoEnv,
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
	return inpiServiceInstance
}

func (s *INPIService) authenticate() error {
	s.tokenMutex.Lock()
	defer s.tokenMutex.Unlock()

	if s.token != "" && time.Now().Before(s.tokenExpiry) {
		return nil
	}

	authReq := INPIAuthRequest{
		Username: s.username,
		Password: s.password,
	}

	jsonData, err := json.Marshal(authReq)
	if err != nil {
		return fmt.Errorf("error marshaling auth request: %w", err)
	}

	req, err := http.NewRequest("POST", s.authURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating auth request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("error executing auth request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("authentication failed: status %d, body: %s", resp.StatusCode, string(body))
	}

	var authResp INPIAuthResponse
	if err := json.NewDecoder(resp.Body).Decode(&authResp); err != nil {
		return fmt.Errorf("error decoding auth response: %w", err)
	}

	if authResp.Token == "" {
		return fmt.Errorf("no token received in auth response")
	}

	s.token = authResp.Token
	s.tokenExpiry = time.Now().Add(55 * time.Minute)

	log.Printf("INPI authentication successful, token expires at %v", s.tokenExpiry)
	return nil
}

func (s *INPIService) getAuthToken() (string, error) {
	s.tokenMutex.RLock()
	if s.token != "" && time.Now().Before(s.tokenExpiry) {
		token := s.token
		s.tokenMutex.RUnlock()
		return token, nil
	}
	s.tokenMutex.RUnlock()

	if err := s.authenticate(); err != nil {
		return "", err
	}

	s.tokenMutex.RLock()
	defer s.tokenMutex.RUnlock()
	return s.token, nil
}

func (s *INPIService) SearchCompany(companyName, address string) (*SearchResult, error) {
	if err := s.authenticate(); err != nil {
		return &SearchResult{
			Success: false,
			Error:   fmt.Sprintf("Authentication failed: %v", err),
		}, nil
	}

	token, err := s.getAuthToken()
	if err != nil {
		return &SearchResult{
			Success: false,
			Error:   fmt.Sprintf("Failed to get auth token: %v", err),
		}, nil
	}

	formalities, err := s.searchByCompanyNameAndAddress(companyName, address, token)
	if err != nil {
		log.Printf("INPI search by name/address failed: %v", err)
		return &SearchResult{
			Success: false,
			Error:   fmt.Sprintf("Search failed: %v", err),
		}, nil
	}

	if len(formalities) == 0 {
		log.Printf("No results found for company: %s", companyName)
		return &SearchResult{
			Success:      true,
			Data:         []CompanyInfo{},
			TotalResults: 0,
		}, nil
	}

	log.Printf("INPI found %d formalities for company: %s", len(formalities), companyName)

	var results []CompanyInfo
	processedName := ProcessForSearch(companyName)
	normalizedSearch := normalizeCompanyName(processedName)
	searchNameLower := strings.ToLower(normalizedSearch)
	parsedAddress := parseAddress(address)
	
	for i, formality := range formalities {
		inpiCompany := s.parseFormalityToCompanyResponse(&formality)
		companyInfo := s.transformINPIResponseToCompanyInfo(inpiCompany, address)
		
		matchScore := s.calculateMatchScore(searchNameLower, inpiCompany, address, parsedAddress)
		companyInfo.MatchScore = matchScore
		
		log.Printf("Parsed formality %d: SIREN=%s, CompanyName=%s, PostalCode=%s, Directors=%v, MatchScore=%.2f", 
			i+1, companyInfo.SocieteSiren, companyInfo.SocieteNom, inpiCompany.PostalCode, companyInfo.SocieteDirigeants, matchScore)
		
		results = append(results, companyInfo)
	}

	if len(results) > 0 {
		s.sortResultsByMatchScore(results)
		
		bestMatch := results[0]
		log.Printf("Best match for '%s': SIREN=%s, CompanyName=%s, Score=%.2f", 
			companyName, bestMatch.SocieteSiren, bestMatch.SocieteNom, bestMatch.MatchScore)
		
		if bestMatch.MatchScore < inpiMinScoreThreshold {
			log.Printf("Warning: Low match score (%.2f) for '%s', best match is '%s' (SIREN: %s). Consider filtering out.", 
				bestMatch.MatchScore, companyName, bestMatch.SocieteNom, bestMatch.SocieteSiren)
			return &SearchResult{
				Success:      true,
				Data:         []CompanyInfo{},
				TotalResults: 0,
			}, nil
		}
	}

	return &SearchResult{
		Success:      true,
		Data:         results,
		TotalResults: len(results),
	}, nil
}

func (s *INPIService) searchByCompanyNameAndAddress(companyName, address, token string) ([]INPIFormality, error) {
	searchURL := fmt.Sprintf("%s%s", s.baseURL, inpiCompaniesEndpoint)
	
	params := url.Values{}
	processedName := ProcessForSearch(companyName)
	params.Set("companyName", processedName)
	
	if address != "" {
		departmentNumber := ExtractDepartmentNumber(address)
		if departmentNumber != "" {
			params.Set("departments", departmentNumber)
		}
	}

	fullURL := fmt.Sprintf("%s?%s", searchURL, params.Encode())
	log.Printf("INPI search URL: %s (companyName=%s, departments=%s)", fullURL, processedName, params.Get("departments"))
	log.Printf("INPI request headers: Authorization=Bearer %s... (token length: %d)", token[:min(20, len(token))], len(token))

	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating search request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing search request: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	
	log.Printf("INPI response status: %d, content-length: %d", resp.StatusCode, len(bodyBytes))
	
	if resp.StatusCode == http.StatusNotFound {
		log.Printf("INPI search returned 404 for company: %s", companyName)
		return []INPIFormality{}, nil
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("INPI search failed: status %d, URL: %s, body: %s", resp.StatusCode, fullURL, string(bodyBytes))
		return nil, fmt.Errorf("search failed: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	if len(bodyBytes) > 0 {
		log.Printf("INPI raw response (first 1000 chars): %s", string(bodyBytes[:min(1000, len(bodyBytes))]))
	}

	var searchResults []INPIFormality
	if err := json.Unmarshal(bodyBytes, &searchResults); err != nil {
		log.Printf("INPI JSON decode error: %v, response body (first 1000 chars): %s", err, string(bodyBytes[:min(1000, len(bodyBytes))]))
		return nil, fmt.Errorf("error decoding search response: %w", err)
	}

	log.Printf("INPI search returned %d results for company: %s", len(searchResults), companyName)
	if len(searchResults) > 0 {
		firstCompany := s.parseFormalityToCompanyResponse(&searchResults[0])
		log.Printf("INPI first result SIREN: %s, CompanyName from response: %s", 
			firstCompany.SIREN, 
			firstCompany.CompanyName)
	}
	return searchResults, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (s *INPIService) getCompanyBySIREN(siren, token string) (*INPICompanyResponse, error) {
	params := url.Values{}
	params.Set("siren", siren)
	companyURL := fmt.Sprintf("%s%s?%s", s.baseURL, inpiCompaniesEndpoint, params.Encode())

	req, err := http.NewRequest("GET", companyURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating company request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing company request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get company failed: status %d, body: %s", resp.StatusCode, string(body))
	}

	var formalities []INPIFormality
	if err := json.NewDecoder(resp.Body).Decode(&formalities); err != nil {
		return nil, fmt.Errorf("error decoding company response: %w", err)
	}

	if len(formalities) == 0 {
		return nil, nil
	}

	formality := formalities[0]
	company := s.parseFormalityToCompanyResponse(&formality)
	return company, nil
}

func findEnseignesInFormality(formality *INPIFormality) []string {
	found := make(map[string]bool)
	findEnseignesRecursiveInFormality(formality, found)
	
	var result []string
	for k := range found {
		result = append(result, k)
	}
	return result
}

func findEnseignesRecursiveInFormality(obj interface{}, found map[string]bool) {
	if obj == nil {
		return
	}
	
	switch v := obj.(type) {
	case map[string]interface{}:
		for key, value := range v {
			keyLower := strings.ToLower(key)
			if strings.Contains(keyLower, "enseigne") {
				if str, ok := value.(string); ok && strings.TrimSpace(str) != "" {
					found[strings.TrimSpace(str)] = true
				} else if arr, ok := value.([]interface{}); ok {
					for _, item := range arr {
						if str, ok := item.(string); ok && strings.TrimSpace(str) != "" {
							found[strings.TrimSpace(str)] = true
						}
					}
				}
			}
			findEnseignesRecursiveInFormality(value, found)
		}
	case []interface{}:
		for _, item := range v {
			findEnseignesRecursiveInFormality(item, found)
		}
	}
}

func (s *INPIService) parseFormalityToCompanyResponse(formality *INPIFormality) *INPICompanyResponse {
	company := &INPICompanyResponse{
		SIREN:     formality.Siren,
		Directors: []string{},
		Enseignes: []string{},
	}

	if company.SIREN == "" {
		company.SIREN = formality.Formality.Siren
	}
	
	enseignes := findEnseignesInFormality(formality)
	company.Enseignes = enseignes

	if formality.Formality.Content.PersonneMorale != nil {
		pm := formality.Formality.Content.PersonneMorale
		company.CompanyName = pm.Identite.Entreprise.Denomination
		company.LegalForm = pm.Identite.Entreprise.FormeJuridique
		if company.LegalForm == "" {
			company.LegalForm = formality.Formality.FormeJuridique
		}
		if pm.Identite.Entreprise.DateImmat != "" {
			company.CreationDate = pm.Identite.Entreprise.DateImmat
		}
		if pm.DetailCessationEntreprise != nil {
			company.ClosureDate = pm.DetailCessationEntreprise.DateRadiation
		}
		company.City = pm.AdresseEntreprise.Adresse.Commune
		company.PostalCode = pm.AdresseEntreprise.Adresse.CodePostal
		if pm.AdresseEntreprise.Adresse.NumVoie != "" && pm.AdresseEntreprise.Adresse.Voie != "" {
			company.Address = fmt.Sprintf("%s %s %s",
				pm.AdresseEntreprise.Adresse.NumVoie,
				pm.AdresseEntreprise.Adresse.TypeVoie,
				pm.AdresseEntreprise.Adresse.Voie)
		}
	} else if formality.Formality.Content.PersonnePhysique != nil {
		pp := formality.Formality.Content.PersonnePhysique
		personne := pp.Identite.Entrepreneur.DescriptionPersonne
		var nameParts []string
		if len(personne.Prenoms) > 0 {
			nameParts = append(nameParts, personne.Prenoms...)
		}
		if personne.Nom != "" {
			nameParts = append(nameParts, personne.Nom)
		}
		if len(nameParts) > 0 {
			company.CompanyName = strings.Join(nameParts, " ")
			company.Directors = append(company.Directors, company.CompanyName)
		}
		company.LegalForm = pp.Identite.Entreprise.FormeJuridique
		if company.LegalForm == "" {
			company.LegalForm = formality.Formality.FormeJuridique
		}
		if pp.Identite.Entreprise.DateImmat != "" {
			company.CreationDate = pp.Identite.Entreprise.DateImmat
		}
		if pp.DetailCessationEntreprise != nil {
			company.ClosureDate = pp.DetailCessationEntreprise.DateRadiation
		}
		company.City = pp.AdresseEntreprise.Adresse.Commune
		company.PostalCode = pp.AdresseEntreprise.Adresse.CodePostal
		if pp.AdresseEntreprise.Adresse.NumVoie != "" && pp.AdresseEntreprise.Adresse.Voie != "" {
			company.Address = fmt.Sprintf("%s %s %s",
				pp.AdresseEntreprise.Adresse.NumVoie,
				pp.AdresseEntreprise.Adresse.TypeVoie,
				pp.AdresseEntreprise.Adresse.Voie)
		}
	}

	if company.CreationDate == "" && formality.Formality.Content.NatureCreation != nil {
		company.CreationDate = formality.Formality.Content.NatureCreation.DateCreation
	}

	return company
}

func (s *INPIService) calculateMatchScore(searchNameLower string, company *INPICompanyResponse, searchAddress string, parsedAddress ParsedAddress) float64 {
	score := 0.0
	
	companyNameNormalized := normalizeCompanyName(company.CompanyName)
	companyNameLower := strings.ToLower(companyNameNormalized)
	
	var enseignesLower []string
	for _, enseigne := range company.Enseignes {
		enseigneNorm := normalizeCompanyName(enseigne)
		enseignesLower = append(enseignesLower, strings.ToLower(enseigneNorm))
	}
	
	if companyNameLower == "" && len(enseignesLower) == 0 {
		return 0.0
	}
	
	if searchAddress != "" {
		searchDepartment := ExtractDepartmentNumber(searchAddress)
		if searchDepartment != "" {
			if company.PostalCode == "" {
				return -50.0
			}
			companyDepartment := ""
			if len(company.PostalCode) >= 2 {
				companyDepartment = company.PostalCode[:2]
			}
			if companyDepartment != searchDepartment {
				return -100.0
			}
		}
	}
	
	wordsSearch := strings.Fields(searchNameLower)
	
	if len(wordsSearch) == 0 {
		return 0.0
	}
	
	if companyNameLower == searchNameLower {
		score += 100.0
	} else if strings.Contains(companyNameLower, searchNameLower) {
		wordsCompany := strings.Fields(companyNameLower)
		if len(wordsCompany) <= len(wordsSearch)+2 {
			score += 80.0
		} else {
			score += 40.0
		}
	} else if strings.Contains(searchNameLower, companyNameLower) && len(companyNameLower) > 5 {
		score += 30.0
	}
	
	var enseigneMatch string
	for _, enseigne := range enseignesLower {
		if strings.Contains(enseigne, searchNameLower) {
			enseigneMatch = enseigne
			break
		}
	}
	
	if enseigneMatch != "" {
		if enseigneMatch == searchNameLower {
			score += 90.0
		} else {
			score += 70.0
		}
	} else if len(enseignesLower) == 0 && companyNameLower != "" {
		score -= 10.0
	}
	
	if companyNameLower != "" {
		wordsCompany := strings.Fields(companyNameLower)
		
		matchedWords := 0
		for _, word := range wordsSearch {
			if len(word) > 2 {
				wordMatched := false
				for _, cWord := range wordsCompany {
					if cWord == word {
						matchedWords++
						wordMatched = true
						break
					} else if strings.Contains(cWord, word) || strings.Contains(word, cWord) {
						matchedWords++
						wordMatched = true
						break
					}
				}
				if !wordMatched {
					for _, enseigne := range enseignesLower {
						enseigneWords := strings.Fields(enseigne)
						for _, eWord := range enseigneWords {
							if eWord == word || strings.Contains(eWord, word) || strings.Contains(word, eWord) {
								matchedWords++
								wordMatched = true
								break
							}
						}
						if wordMatched {
							break
						}
					}
				}
			}
		}
		
		wordMatchRatio := float64(matchedWords) / float64(len(wordsSearch))
		if wordMatchRatio >= 0.8 {
			score += 30.0
		} else if wordMatchRatio >= 0.5 {
			score += 15.0
		} else {
			score += wordMatchRatio * 10.0
		}
		
		if len(wordsCompany) > len(wordsSearch)*2 {
			score -= 20.0
		}
	}
	
	if searchAddress != "" {
		cityFromAddress := ""
		if parsedAddress.LibelleCommune != "" {
			cityFromAddress = strings.ToLower(strings.TrimSpace(parsedAddress.LibelleCommune))
		}
		
		if cityFromAddress != "" && company.City != "" {
			companyCityLower := strings.ToLower(strings.TrimSpace(normalizeCompanyName(company.City)))
			if cityFromAddress == companyCityLower {
				score += 20.0
			} else if strings.Contains(cityFromAddress, companyCityLower) || strings.Contains(companyCityLower, cityFromAddress) {
				score += 10.0
			}
		}
		
		if parsedAddress.PostalCode != "" && company.PostalCode == parsedAddress.PostalCode {
			score += 50.0
		}
		
		if parsedAddress.NumVoie != "" && company.Address != "" {
			numVoieRe := regexp.MustCompile(`\b(\d+)`)
			matches := numVoieRe.FindStringSubmatch(company.Address)
			if len(matches) > 1 {
				companyNumVoie := matches[1]
				if parsedAddress.NumVoie == companyNumVoie {
					score += 50.0
				}
			} else {
				score -= 20.0
			}
		}
	}
	
	if company.ClosureDate != "" {
		score -= 10.0
	}
	
	return score
}

func (s *INPIService) sortResultsByMatchScore(results []CompanyInfo) {
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].MatchScore > results[i].MatchScore {
				results[i], results[j] = results[j], results[i]
			}
		}
	}
}


func (s *INPIService) transformINPIResponseToCompanyInfo(inpiCompany *INPICompanyResponse, originalAddress string) CompanyInfo {
	city := inpiCompany.City
	if city == "" && originalAddress != "" {
		parsed := parseAddress(originalAddress)
		city = parsed.LibelleCommune
	}

	pappersURL := ""
	if inpiCompany.SIREN != "" && inpiCompany.CompanyName != "" {
		pappersURL = CreatePappersURL(inpiCompany.CompanyName, inpiCompany.SIREN)
	}

	return CompanyInfo{
		SocieteSiren:      inpiCompany.SIREN,
		SocieteForme:      inpiCompany.LegalForm,
		SocieteNom:        inpiCompany.CompanyName,
		SocieteCreation:   inpiCompany.CreationDate,
		SocieteCloture:    inpiCompany.ClosureDate,
		SocieteDirigeants: inpiCompany.Directors,
		City:              city,
		PappersURL:        pappersURL,
		SocieteLink:       fmt.Sprintf("https://www.inpi.fr/recherche-entreprise/entreprise/%s", inpiCompany.SIREN),
	}
}

