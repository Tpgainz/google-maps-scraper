package entreprise

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	gouvSearchEndpoint     = "/search"
	gouvNearPointEndpoint  = "/near_point"
	gouvBaseURL           = "https://recherche-entreprises.api.gouv.fr"
	gouvMinScoreThreshold = 200.0
	defaultRadius         = 0.01
)

type GOUVService struct {
	client *http.Client
}

type GOUVEntrepriseResult struct {
	Siren                    string   `json:"siren"`
	NomComplet               string   `json:"nom_complet"`
	NomRaisonSociale         string   `json:"nom_raison_sociale"`
	Sigle                    string   `json:"sigle"`
	NombreEtablissements     int      `json:"nombre_etablissements"`
	NombreEtablissementsOuverts int   `json:"nombre_etablissements_ouverts"`
	Siege                    *GOUVSiege `json:"siege"`
	ActivitePrincipale       string   `json:"activite_principale"`
	CategorieEntreprise      string   `json:"categorie_entreprise"`
	DateCreation             string   `json:"date_creation"`
	DateFermeture            string   `json:"date_fermeture"`
	EtatAdministratif        string   `json:"etat_administratif"`
	NatureJuridique          string   `json:"nature_juridique"`
	TrancheEffectifSalarie   string   `json:"tranche_effectif_salarie"`
	StatutDiffusion          string   `json:"statut_diffusion"`
	Dirigeants               []GOUVDirigeant `json:"dirigeants"`
	MatchingEtablissements   []GOUVEtablissement `json:"matching_etablissements"`
}

type GOUVSiege struct {
	ActivitePrincipale       string   `json:"activite_principale"`
	Adresse                  string   `json:"adresse"`
	CodePostal               string   `json:"code_postal"`
	Commune                  string   `json:"commune"`
	LibelleCommune           string   `json:"libelle_commune"`
	LibelleVoie              string   `json:"libelle_voie"`
	NumeroVoie               string   `json:"numero_voie"`
	TypeVoie                 string   `json:"type_voie"`
	Latitude                 string   `json:"latitude"`
	Longitude                string   `json:"longitude"`
	DateCreation             string   `json:"date_creation"`
	DateFermeture            string   `json:"date_fermeture"`
	EstSiege                 bool     `json:"est_siege"`
	EtatAdministratif       string   `json:"etat_administratif"`
	NomCommercial            string   `json:"nom_commercial"`
	ListeEnseignes           []string `json:"liste_enseignes"`
}

type GOUVDirigeant struct {
	Nom       string   `json:"nom"`
	Prenoms   string   `json:"prenoms"`
	Qualite   string   `json:"qualite"`
	TypeDirigeant string `json:"type_dirigeant"`
}

type GOUVEtablissement struct {
	Siret                   string `json:"siret"`
	CodePostal              string `json:"code_postal"`
	Commune                 string `json:"commune"`
	LibelleCommune          string `json:"libelle_commune"`
	LibelleVoie             string `json:"libelle_voie"`
	NumeroVoie              string `json:"numero_voie"`
	TypeVoie                string `json:"type_voie"`
	DateCreation            string `json:"date_creation"`
	DateFermeture           string `json:"date_fermeture"`
	EstSiege                bool   `json:"est_siege"`
	EtatAdministratif       string `json:"etat_administratif"`
}

type GOUVSearchResponse struct {
	Results      []GOUVEntrepriseResult `json:"results"`
	TotalResults int                    `json:"total_results"`
	Page         int                    `json:"page"`
	PerPage      int                    `json:"per_page"`
	TotalPages   int                    `json:"total_pages"`
}

func NewGOUVService() *GOUVService {
	return &GOUVService{
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
}

func (s *GOUVService) SearchCompany(companyName, address string) (*SearchResult, error) {
	parsedAddress := parseAddress(address)
	
	var searchURL string
	params := url.Values{}
	
	if parsedAddress.PostalCode != "" {
		params.Set("q", ProcessForSearch(companyName))
		params.Set("code_postal", parsedAddress.PostalCode)
		params.Set("per_page", "20")
		searchURL = fmt.Sprintf("%s%s?%s", gouvBaseURL, gouvSearchEndpoint, params.Encode())
	} else {
		return &SearchResult{
			Success: false,
			Error:   "Code postal requis pour la recherche GOUV",
		}, nil
	}

	log.Printf("GOUV search URL: %s", searchURL)

	req, err := http.NewRequest("GET", searchURL, nil)
	if err != nil {
		return &SearchResult{
			Success: false,
			Error:   fmt.Sprintf("Error creating request: %v", err),
		}, nil
	}

	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return &SearchResult{
			Success: false,
			Error:   fmt.Sprintf("Error executing request: %v", err),
		}, nil
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		log.Printf("GOUV search failed: status %d, body: %s", resp.StatusCode, string(bodyBytes))
		return &SearchResult{
			Success: false,
			Error:   fmt.Sprintf("Search failed: status %d", resp.StatusCode),
		}, nil
	}

	var searchResponse GOUVSearchResponse
	if err := json.Unmarshal(bodyBytes, &searchResponse); err != nil {
		log.Printf("GOUV JSON decode error: %v, response body: %s", err, string(bodyBytes[:min(1000, len(bodyBytes))]))
		return &SearchResult{
			Success: false,
			Error:   fmt.Sprintf("Error decoding response: %v", err),
		}, nil
	}

	log.Printf("GOUV search returned %d results for company: %s", len(searchResponse.Results), companyName)

	if len(searchResponse.Results) == 0 {
		return &SearchResult{
			Success:      true,
			Data:         []CompanyInfo{},
			TotalResults: 0,
		}, nil
	}

	var results []CompanyInfo
	companyNameLower := strings.ToLower(ProcessForSearch(companyName))
	
	for i, result := range searchResponse.Results {
		companyInfo := s.transformGOUVToCompanyInfo(&result, address)
		
		matchScore := s.calculateGOUVMatchScore(companyNameLower, &result, address, &parsedAddress)
		companyInfo.MatchScore = matchScore
		
		log.Printf("Parsed GOUV result %d: SIREN=%s, CompanyName=%s, PostalCode=%s, Directors=%v, MatchScore=%.2f", 
			i+1, companyInfo.SocieteSiren, companyInfo.SocieteNom, result.Siege.CodePostal, companyInfo.SocieteDirigeants, matchScore)
		
		results = append(results, companyInfo)
	}

	if len(results) > 0 {
		s.sortResultsByMatchScore(results)
		
		bestMatch := results[0]
		log.Printf("Best GOUV match for '%s': SIREN=%s, CompanyName=%s, Score=%.2f", 
			companyName, bestMatch.SocieteSiren, bestMatch.SocieteNom, bestMatch.MatchScore)
		
		if bestMatch.MatchScore < gouvMinScoreThreshold {
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

func (s *GOUVService) calculateGOUVMatchScore(searchNameLower string, result *GOUVEntrepriseResult, address string, parsedAddress *ParsedAddress) float64 {
	score := 0.0
	
	searchDepartment := ExtractDepartmentNumber(address)
	
	if searchDepartment != "" && result.Siege != nil {
		if result.Siege.CodePostal == "" {
			return -50.0
		}
		companyDepartment := ""
		if len(result.Siege.CodePostal) >= 2 {
			companyDepartment = result.Siege.CodePostal[:2]
		}
		if companyDepartment != searchDepartment {
			return -100.0
		}
	}

	nomComplet := strings.ToLower(normalizeCompanyName(result.NomComplet))
	nomRaisonSociale := strings.ToLower(normalizeCompanyName(result.NomRaisonSociale))
	sigle := strings.ToLower(normalizeCompanyName(result.Sigle))
	
	var nomCommercial string
	var enseignes []string
	if result.Siege != nil {
		nomCommercial = strings.ToLower(normalizeCompanyName(result.Siege.NomCommercial))
		enseignes = result.Siege.ListeEnseignes
	}

	nameScore := 0.0

	if nomComplet == searchNameLower {
		nameScore = 100.0
	} else if strings.Contains(nomComplet, searchNameLower) {
		wordsSearch := strings.Fields(searchNameLower)
		wordsCompany := strings.Fields(nomComplet)
		if len(wordsCompany) <= len(wordsSearch)+2 {
			nameScore = 80.0
		} else {
			nameScore = 40.0
		}
	} else if strings.Contains(searchNameLower, nomComplet) && len(nomComplet) > 5 {
		nameScore = 30.0
	}

	if nomRaisonSociale == searchNameLower {
		if nameScore < 100.0 {
			nameScore = 100.0
		}
	} else if strings.Contains(nomRaisonSociale, searchNameLower) {
		wordsSearch := strings.Fields(searchNameLower)
		wordsCompany := strings.Fields(nomRaisonSociale)
		scoreCandidate := 80.0
		if len(wordsCompany) > len(wordsSearch)+2 {
			scoreCandidate = 40.0
		}
		if scoreCandidate > nameScore {
			nameScore = scoreCandidate
		}
	}

	if sigle != "" && sigle == searchNameLower {
		if nameScore < 90.0 {
			nameScore = 90.0
		}
	} else if sigle != "" && strings.Contains(sigle, searchNameLower) {
		if nameScore < 70.0 {
			nameScore = 70.0
		}
	}

	if nomCommercial == searchNameLower {
		if nameScore < 90.0 {
			nameScore = 90.0
		}
	} else if strings.Contains(nomCommercial, searchNameLower) {
		if nameScore < 70.0 {
			nameScore = 70.0
		}
	}

	for _, enseigne := range enseignes {
		enseigneLower := strings.ToLower(normalizeCompanyName(enseigne))
		if enseigneLower == searchNameLower {
			if nameScore < 90.0 {
				nameScore = 90.0
			}
			break
		} else if strings.Contains(enseigneLower, searchNameLower) {
			if nameScore < 70.0 {
				nameScore = 70.0
			}
		}
	}

	score += nameScore

	wordsSearch := strings.Fields(searchNameLower)
	if len(wordsSearch) > 0 && nameScore < 80.0 {
		allNames := []string{nomComplet, nomRaisonSociale, nomCommercial}
		for _, e := range enseignes {
			allNames = append(allNames, strings.ToLower(normalizeCompanyName(e)))
		}

		matchedWords := 0
		for _, word := range wordsSearch {
			if len(word) > 2 {
				for _, name := range allNames {
					nameWords := strings.Fields(name)
					for _, nameWord := range nameWords {
						if nameWord == word {
							matchedWords++
							goto nextWord
						} else if strings.Contains(nameWord, word) || strings.Contains(word, nameWord) {
							matchedWords++
							goto nextWord
						}
					}
				}
			nextWord:
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

		longestName := ""
		for _, name := range allNames {
			if len(strings.Fields(name)) > len(strings.Fields(longestName)) {
				longestName = name
			}
		}
		longestNameWords := strings.Fields(longestName)
		if len(longestNameWords) > len(wordsSearch)*2 {
			score -= 20.0
		}
	}

	if address != "" && result.Siege != nil {
		siege := result.Siege
		
		if parsedAddress.PostalCode != "" && siege.CodePostal == parsedAddress.PostalCode {
			score += 50.0
		}

		if parsedAddress.NumVoie != "" && siege.NumeroVoie != "" {
			if parsedAddress.NumVoie == siege.NumeroVoie {
				score += 50.0
			} else {
				searchNum, err1 := strconv.Atoi(parsedAddress.NumVoie)
				siegeNum, err2 := strconv.Atoi(siege.NumeroVoie)
				if err1 == nil && err2 == nil {
					diff := searchNum - siegeNum
					if diff < 0 {
						diff = -diff
					}
					if diff <= 2 {
						score -= float64(diff) * 5.0
					} else {
						score -= 15.0
					}
				}
			}
		} else if parsedAddress.NumVoie != "" && siege.NumeroVoie == "" {
			score -= 20.0
		}

		if parsedAddress.TypeVoie != "" && siege.TypeVoie != "" {
			typeVoieNormalized := normalizeCompanyName(siege.TypeVoie)
			searchTypeVoieNormalized := normalizeCompanyName(parsedAddress.TypeVoie)
			if typeVoieNormalized == searchTypeVoieNormalized {
				score += 20.0
			}
		}

		if parsedAddress.LibelleVoie != "" && siege.LibelleVoie != "" {
			libelleVoieNormalized := normalizeCompanyName(siege.LibelleVoie)
			searchLibelleVoieNormalized := normalizeCompanyName(parsedAddress.LibelleVoie)
			if libelleVoieNormalized == searchLibelleVoieNormalized {
				score += 40.0
			} else if strings.Contains(libelleVoieNormalized, searchLibelleVoieNormalized) {
				score += 20.0
			}
		}

		if parsedAddress.LibelleCommune != "" && siege.LibelleCommune != "" {
			cityFromAddress := strings.ToLower(strings.TrimSpace(parsedAddress.LibelleCommune))
			siegeCommune := strings.ToLower(strings.TrimSpace(siege.LibelleCommune))
			if cityFromAddress == siegeCommune {
				score += 20.0
			} else if strings.Contains(cityFromAddress, siegeCommune) || strings.Contains(siegeCommune, cityFromAddress) {
				score += 10.0
			}
		}
	}

	if result.EtatAdministratif == "A" {
		score += 10.0
	} else if result.EtatAdministratif == "C" || result.EtatAdministratif == "F" {
		score -= 30.0
	}

	if result.Siege != nil && result.Siege.DateFermeture != "" {
		score -= 10.0
	}

	if result.Siege != nil && result.Siege.EstSiege {
		score += 10.0
	}

	return score
}

func (s *GOUVService) transformGOUVToCompanyInfo(result *GOUVEntrepriseResult, originalAddress string) CompanyInfo {
	var denominationCommerciale string
	if result.Siege != nil && len(result.Siege.ListeEnseignes) > 0 {
		for _, enseigne := range result.Siege.ListeEnseignes {
			if strings.Contains(result.NomComplet, enseigne) {
				denominationCommerciale = enseigne
				break
			}
		}
		if denominationCommerciale == "" {
			denominationCommerciale = result.Siege.ListeEnseignes[0]
		}
	}
	if denominationCommerciale == "" {
		denominationCommerciale = result.NomComplet
		if denominationCommerciale == "" {
			denominationCommerciale = result.NomRaisonSociale
		}
	}

	var directors []string
	for _, dir := range result.Dirigeants {
		if dir.Nom != "" {
			fullName := dir.Nom
			if dir.Prenoms != "" {
				fullName = dir.Prenoms + " " + fullName
			}
			directors = append(directors, fullName)
		}
	}

	city := ""
	if result.Siege != nil {
		city = result.Siege.LibelleCommune
	}
	if city == "" && originalAddress != "" {
		parsed := parseAddress(originalAddress)
		city = parsed.LibelleCommune
	}

	pappersURL := ""
	if result.Siren != "" && denominationCommerciale != "" {
		pappersURL = CreatePappersURL(denominationCommerciale, result.Siren)
	}

	diffusionCommerciale := false
	if result.StatutDiffusion == "O" {
		diffusionCommerciale = true
	}

	return CompanyInfo{
		SocieteSiren:      result.Siren,
		SocieteForme:      result.NatureJuridique,
		SocieteNom:        denominationCommerciale,
		SocieteCreation:   result.DateCreation,
		SocieteCloture:    result.DateFermeture,
		SocieteDirigeants: directors,
		City:              city,
		PappersURL:        pappersURL,
		SocieteLink:       fmt.Sprintf("https://recherche-entreprises.api.gouv.fr/search?q=%s", url.QueryEscape(result.Siren)),
		SocieteDiffusion:  diffusionCommerciale,
	}
}

func (s *GOUVService) sortResultsByMatchScore(results []CompanyInfo) {
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].MatchScore > results[i].MatchScore {
				results[i], results[j] = results[j], results[i]
			}
		}
	}
}

func calculateDistance(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusKm = 6371.0
	
	lat1Rad := lat1 * math.Pi / 180.0
	lat2Rad := lat2 * math.Pi / 180.0
	deltaLat := (lat2 - lat1) * math.Pi / 180.0
	deltaLon := (lon2 - lon1) * math.Pi / 180.0
	
	a := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
			math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	
	return earthRadiusKm * c
}

func scoreEntrepriseResult(result *GOUVEntrepriseResult, query string, address string) float64 {
	score := 0.0
	
	if query == "" && address == "" {
		return score
	}
	
	var parsedAddress *ParsedAddress
	if address != "" {
		parsed := parseAddress(address)
		parsedAddress = &parsed
	}
	
	queryLower := strings.ToLower(normalizeCompanyName(query))
	
	nomComplet := strings.ToLower(normalizeCompanyName(result.NomComplet))
	nomRaisonSociale := strings.ToLower(normalizeCompanyName(result.NomRaisonSociale))
	sigle := strings.ToLower(normalizeCompanyName(result.Sigle))
	
	var nomCommercial string
	var enseignes []string
	if result.Siege != nil {
		nomCommercial = strings.ToLower(normalizeCompanyName(result.Siege.NomCommercial))
		enseignes = result.Siege.ListeEnseignes
	}
	
	nameScore := 0.0
	
	if nomComplet == queryLower {
		nameScore = 100.0
	} else if strings.Contains(nomComplet, queryLower) {
		wordsSearch := strings.Fields(queryLower)
		wordsCompany := strings.Fields(nomComplet)
		if len(wordsCompany) <= len(wordsSearch)+2 {
			nameScore = 80.0
		} else {
			nameScore = 40.0
		}
	} else if strings.Contains(queryLower, nomComplet) && len(nomComplet) > 5 {
		nameScore = 30.0
	}
	
	if nomRaisonSociale == queryLower {
		if nameScore < 100.0 {
			nameScore = 100.0
		}
	} else if strings.Contains(nomRaisonSociale, queryLower) {
		wordsSearch := strings.Fields(queryLower)
		wordsCompany := strings.Fields(nomRaisonSociale)
		scoreCandidate := 80.0
		if len(wordsCompany) > len(wordsSearch)+2 {
			scoreCandidate = 40.0
		}
		if scoreCandidate > nameScore {
			nameScore = scoreCandidate
		}
	}
	
	if sigle != "" && sigle == queryLower {
		if nameScore < 90.0 {
			nameScore = 90.0
		}
	} else if sigle != "" && strings.Contains(sigle, queryLower) {
		if nameScore < 70.0 {
			nameScore = 70.0
		}
	}
	
	if nomCommercial == queryLower {
		if nameScore < 90.0 {
			nameScore = 90.0
		}
	} else if strings.Contains(nomCommercial, queryLower) {
		if nameScore < 70.0 {
			nameScore = 70.0
		}
	}
	
	for _, enseigne := range enseignes {
		enseigneLower := strings.ToLower(normalizeCompanyName(enseigne))
		if enseigneLower == queryLower {
			if nameScore < 90.0 {
				nameScore = 90.0
			}
			break
		} else if strings.Contains(enseigneLower, queryLower) {
			if nameScore < 70.0 {
				nameScore = 70.0
			}
		}
	}
	
	score += nameScore
	
	wordsSearch := strings.Fields(queryLower)
	if len(wordsSearch) > 0 && nameScore < 80.0 {
		allNames := []string{nomComplet, nomRaisonSociale, nomCommercial}
		for _, e := range enseignes {
			allNames = append(allNames, strings.ToLower(normalizeCompanyName(e)))
		}
		
		matchedWords := 0
		for _, word := range wordsSearch {
			if len(word) > 2 {
				for _, name := range allNames {
					nameWords := strings.Fields(name)
					for _, nameWord := range nameWords {
						if nameWord == word {
							matchedWords++
							goto nextWord
						} else if strings.Contains(nameWord, word) || strings.Contains(word, nameWord) {
							matchedWords++
							goto nextWord
						}
					}
				}
			nextWord:
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
		
		longestName := ""
		for _, name := range allNames {
			if len(strings.Fields(name)) > len(strings.Fields(longestName)) {
				longestName = name
			}
		}
		longestNameWords := strings.Fields(longestName)
		if len(longestNameWords) > len(wordsSearch)*2 {
			score -= 20.0
		}
	}
	
	if address != "" && result.Siege != nil && parsedAddress != nil {
		siege := result.Siege
		
		if parsedAddress.PostalCode != "" && siege.CodePostal == parsedAddress.PostalCode {
			score += 50.0
		}
		
		if parsedAddress.NumVoie != "" && siege.NumeroVoie != "" {
			if parsedAddress.NumVoie == siege.NumeroVoie {
				score += 50.0
			} else {
				searchNum, err1 := strconv.Atoi(parsedAddress.NumVoie)
				siegeNum, err2 := strconv.Atoi(siege.NumeroVoie)
				if err1 == nil && err2 == nil {
					diff := searchNum - siegeNum
					if diff < 0 {
						diff = -diff
					}
					if diff <= 2 {
						score -= float64(diff) * 5.0
					} else {
						if parsedAddress.AdresseBis == "" {
							score -= 15.0
						}
					}
				}
			}
		} else if parsedAddress.NumVoie != "" && siege.NumeroVoie == "" {
			if parsedAddress.AdresseBis == "" {
				score -= 20.0
			}
		}
		
		if parsedAddress.TypeVoie != "" && siege.TypeVoie != "" {
			typeVoieNormalized := normalizeCompanyName(siege.TypeVoie)
			searchTypeVoieNormalized := normalizeCompanyName(parsedAddress.TypeVoie)
			if typeVoieNormalized == searchTypeVoieNormalized {
				score += 20.0
			}
		}
		
		if parsedAddress.LibelleVoie != "" && siege.LibelleVoie != "" {
			libelleVoieNormalized := normalizeCompanyName(siege.LibelleVoie)
			searchLibelleVoieNormalized := normalizeCompanyName(parsedAddress.LibelleVoie)
			if libelleVoieNormalized == searchLibelleVoieNormalized {
				score += 40.0
			} else if strings.Contains(libelleVoieNormalized, searchLibelleVoieNormalized) {
				score += 20.0
			}
		}
		
		if parsedAddress.AdresseBis != "" {
			libelleVoieNormalized := normalizeCompanyName(siege.LibelleVoie)
			normalizedAdresseBis := normalizeCompanyName(parsedAddress.AdresseBis)
			
			if libelleVoieNormalized == normalizedAdresseBis {
				score += 60.0
			} else if strings.Contains(libelleVoieNormalized, normalizedAdresseBis) {
				score += 40.0
			} else if strings.Contains(normalizedAdresseBis, libelleVoieNormalized) {
				score += 30.0
			}
		}
	}
	
	if result.EtatAdministratif == "A" {
		score += 10.0
	} else if result.EtatAdministratif == "C" || result.EtatAdministratif == "F" {
		score -= 30.0
	}
	
	if result.Siege != nil && result.Siege.DateFermeture != "" {
		score -= 10.0
	}
	
	if result.Siege != nil && result.Siege.EstSiege {
		score += 10.0
	}
	
	return score
}

type GeographicSearchParams struct {
	Query                      string
	Address                    string
	Lat                        *float64
	Long                       *float64
	Radius                     float64
	CodePostal                 string
	CodeCommune                string
	ActivitePrincipale         string
	SectionActivitePrincipale  string
	Page                       *int
	PerPage                    *int
	LimiteMatchingEtablissements *int
	Minimal                    *bool
	Include                    string
	PageEtablissements         *int
	SortBySize                *bool
}

func (s *GOUVService) SearchByGeographicLocation(params GeographicSearchParams) (*SearchResult, error) {
	hasTextSearch := params.Query != ""
	hasGeographicFilters := params.CodePostal != "" || params.CodeCommune != "" || (params.Lat != nil && params.Long != nil)
	
	if !hasTextSearch && !hasGeographicFilters {
		return &SearchResult{
			Success: false,
			Error:   "Au moins un paramètre de recherche (query, lat/long, ou code_postal) est requis",
		}, nil
	}
	
	var searchURL string
	useNearPoint := false
	
	radius := params.Radius
	if radius == 0 {
		radius = defaultRadius
	}
	
	if params.Lat != nil && params.Long != nil {
		useNearPoint = true
		urlParams := url.Values{}
		urlParams.Set("lat", fmt.Sprintf("%f", *params.Lat))
		urlParams.Set("long", fmt.Sprintf("%f", *params.Long))
		
		radiusKm := radius
		if radiusKm > 50 {
			log.Printf("Radius supérieur à 50km, utilisation de 50km maximum, requestedRadius: %f", radius)
			radiusKm = 50
		}
		urlParams.Set("radius", fmt.Sprintf("%f", radiusKm))
		
		if params.ActivitePrincipale != "" {
			urlParams.Set("activite_principale", params.ActivitePrincipale)
		}
		
		if params.SectionActivitePrincipale != "" {
			urlParams.Set("section_activite_principale", params.SectionActivitePrincipale)
		}
		
		if params.PerPage != nil {
			perPage := *params.PerPage
			if perPage > 100 {
				perPage = 100
			}
			urlParams.Set("per_page", strconv.Itoa(perPage))
		} else {
			urlParams.Set("per_page", "100")
		}
		
		if params.LimiteMatchingEtablissements != nil {
			limite := *params.LimiteMatchingEtablissements
			if limite < 1 {
				limite = 1
			}
			if limite > 100 {
				limite = 100
			}
			urlParams.Set("limite_matching_etablissements", strconv.Itoa(limite))
		}
		
		if params.Minimal != nil {
			urlParams.Set("minimal", strconv.FormatBool(*params.Minimal))
		}
		
		if params.Include != "" {
			urlParams.Set("include", params.Include)
		}
		
		if params.PageEtablissements != nil {
			urlParams.Set("page_etablissements", strconv.Itoa(*params.PageEtablissements))
		}
		
		if params.SortBySize != nil {
			urlParams.Set("sort_by_size", strconv.FormatBool(*params.SortBySize))
		}
		
		searchURL = fmt.Sprintf("%s%s?%s", gouvBaseURL, gouvNearPointEndpoint, urlParams.Encode())
	} else {
		searchParams := url.Values{}
		
		if hasTextSearch {
			searchParams.Set("q", params.Query)
		}
		
		postalCode := params.CodePostal
		if postalCode == "" && params.Address != "" {
			parsed := parseAddress(params.Address)
			postalCode = parsed.PostalCode
		}
		
		if postalCode != "" {
			searchParams.Set("code_postal", postalCode)
		}
		
		if params.CodeCommune != "" {
			searchParams.Set("code_commune", params.CodeCommune)
		}
		
		if params.ActivitePrincipale != "" {
			searchParams.Set("activite_principale", params.ActivitePrincipale)
		}
		
		if params.SectionActivitePrincipale != "" {
			searchParams.Set("section_activite_principale", params.SectionActivitePrincipale)
		}
		
		if params.Page != nil {
			searchParams.Set("page", strconv.Itoa(*params.Page))
		}
		
		if params.PerPage != nil {
			perPage := *params.PerPage
			if perPage > 25 {
				perPage = 25
			}
			searchParams.Set("per_page", strconv.Itoa(perPage))
		}
		
		if params.LimiteMatchingEtablissements != nil {
			limite := *params.LimiteMatchingEtablissements
			if limite < 1 {
				limite = 1
			}
			if limite > 100 {
				limite = 100
			}
			searchParams.Set("limite_matching_etablissements", strconv.Itoa(limite))
		}
		
		if params.Minimal != nil {
			searchParams.Set("minimal", strconv.FormatBool(*params.Minimal))
		}
		
		if params.Include != "" {
			searchParams.Set("include", params.Include)
		}
		
		if params.PageEtablissements != nil {
			searchParams.Set("page_etablissements", strconv.Itoa(*params.PageEtablissements))
		}
		
		if params.SortBySize != nil {
			searchParams.Set("sort_by_size", strconv.FormatBool(*params.SortBySize))
		}
		
		searchURL = fmt.Sprintf("%s%s?%s", gouvBaseURL, gouvSearchEndpoint, searchParams.Encode())
	}
	
	log.Printf("GOUV geographic search URL: %s", searchURL)
	
	req, err := http.NewRequest("GET", searchURL, nil)
	if err != nil {
		return &SearchResult{
			Success: false,
			Error:   fmt.Sprintf("Error creating request: %v", err),
		}, nil
	}
	
	req.Header.Set("Accept", "application/json")
	
	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("GOUV geographic search error: %v, url: %s, query: %s, address: %s, lat: %v, long: %v, radius: %f",
			err, searchURL, params.Query, params.Address, params.Lat, params.Long, radius)
		return &SearchResult{
			Success: false,
			Error:   fmt.Sprintf("Error executing request: %v", err),
		}, nil
	}
	defer resp.Body.Close()
	
	bodyBytes, _ := io.ReadAll(resp.Body)
	
	if resp.StatusCode != http.StatusOK {
		log.Printf("GOUV geographic search failed: status %d, statusText: %s, url: %s, query: %s, address: %s, lat: %v, long: %v, radius: %f, body: %s",
			resp.StatusCode, resp.Status, searchURL, params.Query, params.Address, params.Lat, params.Long, radius, string(bodyBytes))
		return &SearchResult{
			Success: false,
			Error:   fmt.Sprintf("Erreur HTTP %d: %s", resp.StatusCode, resp.Status),
		}, nil
	}
	
	var searchResponse GOUVSearchResponse
	if err := json.Unmarshal(bodyBytes, &searchResponse); err != nil {
		log.Printf("GOUV geographic search JSON decode error: %v, response body: %s", err, string(bodyBytes[:min(1000, len(bodyBytes))]))
		return &SearchResult{
			Success: false,
			Error:   fmt.Sprintf("Error decoding response: %v", err),
		}, nil
	}
	
	results := searchResponse.Results
	
	if params.Lat != nil && params.Long != nil && radius > 0 && !useNearPoint {
		radiusKm := radius
		if radiusKm > 50 {
			radiusKm = 50
		}
		
		var filteredResults []GOUVEntrepriseResult
		for _, result := range results {
			if result.Siege == nil {
				continue
			}
			if result.Siege.Latitude == "" || result.Siege.Longitude == "" {
				continue
			}
			
			resultLat, err1 := strconv.ParseFloat(result.Siege.Latitude, 64)
			resultLong, err2 := strconv.ParseFloat(result.Siege.Longitude, 64)
			if err1 != nil || err2 != nil {
				continue
			}
			
			distance := calculateDistance(*params.Lat, *params.Long, resultLat, resultLong)
			if distance <= radiusKm {
				filteredResults = append(filteredResults, result)
			}
		}
		results = filteredResults
	}
	
	type ScoredResult struct {
		Result GOUVEntrepriseResult
		Score  float64
	}
	
	var scoredResults []ScoredResult
	
	if params.Query != "" || params.Address != "" {
		for _, result := range results {
			score := scoreEntrepriseResult(&result, params.Query, params.Address)
			scoredResults = append(scoredResults, ScoredResult{
				Result: result,
				Score:  score,
			})
		}
		
		for i := 0; i < len(scoredResults)-1; i++ {
			for j := i + 1; j < len(scoredResults); j++ {
				if scoredResults[j].Score > scoredResults[i].Score {
					scoredResults[i], scoredResults[j] = scoredResults[j], scoredResults[i]
				}
			}
		}
		
		if useNearPoint {
			var filteredScoredResults []ScoredResult
			for _, item := range scoredResults {
				if item.Score >= gouvMinScoreThreshold {
					filteredScoredResults = append(filteredScoredResults, item)
				}
			}
			scoredResults = filteredScoredResults
		}
	} else {
		for _, result := range results {
			scoredResults = append(scoredResults, ScoredResult{
				Result: result,
				Score:  0,
			})
		}
	}
	
	var companyInfos []CompanyInfo
	for _, item := range scoredResults {
		companyInfo := s.transformGOUVToCompanyInfo(&item.Result, params.Address)
		companyInfo.MatchScore = item.Score
		companyInfos = append(companyInfos, companyInfo)
	}
	
	return &SearchResult{
		Success:      true,
		Data:         companyInfos,
		TotalResults: len(companyInfos),
	}, nil
}


