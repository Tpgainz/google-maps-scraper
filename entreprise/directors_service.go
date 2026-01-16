package entreprise

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type DirectorInfo struct {
	Nom    string
	Prenom string
}

type DirectorsService struct {
	client *http.Client
}

func NewDirectorsService() *DirectorsService {
	return &DirectorsService{
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

func (s *DirectorsService) GetDirectors(siren string, siret string) *DirectorInfo {
	if siret != "" {
		directors := s.getDirectorsFromInpiBySiret(siret)
		if directors != nil && directors.Nom != "" && directors.Prenom != "" {
			return directors
		}
	}

	directors := s.getDirectorsFromAnnuaireEntreprises(siren)
	if directors != nil && directors.Nom != "" && directors.Prenom != "" {
		return directors
	}

	directors = s.getDirectorsFromInpiSearch(siren)
	if directors != nil && directors.Nom != "" && directors.Prenom != "" {
		return directors
	}

	directors = s.getDirectorsFromBodacc(siren)
	if directors != nil && directors.Nom != "" && directors.Prenom != "" {
		return directors
	}

	directors = s.getDirectorsFromPappers(siren)
	if directors != nil && directors.Nom != "" && directors.Prenom != "" {
		return directors
	}

	return nil
}

func (s *DirectorsService) getDirectorsFromAnnuaireEntreprises(siren string) *DirectorInfo {
	url := fmt.Sprintf("https://recherche-entreprises.api.gouv.fr/entreprises/%s", siren)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil
	}

	dirigeants, ok := data["dirigeants"].([]interface{})
	if !ok || len(dirigeants) == 0 {
		return nil
	}

	dirigeant, ok := dirigeants[0].(map[string]interface{})
	if !ok {
		return nil
	}

	var nom, prenom string

	if n, ok := dirigeant["nom"].(string); ok && n != "" {
		nom = n
	} else if n, ok := dirigeant["nomUsage"].(string); ok && n != "" {
		nom = n
	}

	if p, ok := dirigeant["prenoms"].([]interface{}); ok && len(p) > 0 {
		var prenoms []string
		for _, pr := range p {
			if str, ok := pr.(string); ok {
				prenoms = append(prenoms, str)
			}
		}
		prenom = strings.Join(prenoms, " ")
	} else if p, ok := dirigeant["prenom"].(string); ok && p != "" {
		prenom = p
	}

	if nom != "" && prenom != "" {
		return &DirectorInfo{Nom: nom, Prenom: prenom}
	}

	return nil
}

func (s *DirectorsService) getDirectorsFromBodacc(siren string) *DirectorInfo {
	baseURL := "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1"
	dataset := "annonces-commerciales"

	searchQuery := fmt.Sprintf(`registre:"%s"`, siren)

	params := url.Values{}
	params.Set("where", searchQuery)
	params.Set("limit", "5")

	searchURL := fmt.Sprintf("%s/catalog/datasets/%s/records?%s", baseURL, dataset, params.Encode())

	req, err := http.NewRequest("GET", searchURL, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "LeadExpress/1.0")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	var data struct {
		Results []struct {
			Record struct {
				Fields struct {
					Listepersonnes string `json:"listepersonnes"`
				} `json:"fields"`
			} `json:"record"`
		} `json:"results"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil
	}

	if data.Results == nil || len(data.Results) == 0 {
		return nil
	}

	for _, result := range data.Results {
		if result.Record.Fields.Listepersonnes == "" {
			continue
		}

		var personnesData map[string]interface{}
		if err := json.Unmarshal([]byte(result.Record.Fields.Listepersonnes), &personnesData); err != nil {
			continue
		}

		personne, ok := personnesData["personne"].(map[string]interface{})
		if !ok {
			continue
		}

		administration, ok := personne["administration"]
		if !ok {
			continue
		}

		switch admin := administration.(type) {
		case []interface{}:
			if len(admin) > 0 {
				if dirigeant, ok := admin[0].(string); ok && dirigeant != "" {
					parts := strings.Fields(strings.TrimSpace(dirigeant))
					if len(parts) >= 2 {
						return &DirectorInfo{
							Nom:    parts[len(parts)-1],
							Prenom: strings.Join(parts[:len(parts)-1], " "),
						}
					}
				}
			}
		case string:
			if admin != "" {
				parts := strings.Fields(strings.TrimSpace(admin))
				if len(parts) >= 2 {
					return &DirectorInfo{
						Nom:    parts[len(parts)-1],
						Prenom: strings.Join(parts[:len(parts)-1], " "),
					}
				}
			}
		}
	}

	return nil
}

func (s *DirectorsService) getDirectorsFromInpiBySiret(siret string) *DirectorInfo {
	const retries = 3
	const inpiRNEBaseURL = "https://registre-national-entreprises.inpi.fr/api"

	var jwt string
	var err error

	for attempt := 0; attempt < retries; attempt++ {
		if jwt == "" {
			jwt, err = getINPIJWTToken()
			if err != nil {
				log.Printf("getDirectorsFromInpiBySiret: Failed to get INPI JWT token: %v", err)
				if attempt < retries-1 {
					time.Sleep(time.Duration(1<<uint(attempt)) * time.Second)
					continue
				}
				return nil
			}
		}

		url := fmt.Sprintf("%s/companies?siret=%s", inpiRNEBaseURL, siret)

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Printf("getDirectorsFromInpiBySiret: Error creating request: %v", err)
			if attempt < retries-1 {
				time.Sleep(time.Duration(1<<uint(attempt)) * time.Second)
				continue
			}
			return nil
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", jwt))

		resp, err := s.client.Do(req)
		if err != nil {
			log.Printf("getDirectorsFromInpiBySiret: Error executing request: %v", err)
			if attempt < retries-1 {
				time.Sleep(time.Duration(1<<uint(attempt)) * time.Second)
				continue
			}
			return nil
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			retryAfter := resp.Header.Get("Retry-After")
			waitTime := time.Duration(1<<uint(attempt)) * time.Second
			if retryAfter != "" {
				if seconds, err := strconv.Atoi(retryAfter); err == nil {
					waitTime = time.Duration(seconds) * time.Second
				}
			}

			log.Printf("getDirectorsFromInpiBySiret: Rate limited (429), retrying in %v for SIRET %s, attempt %d/%d",
				waitTime, siret, attempt+1, retries)

			if attempt < retries-1 {
				time.Sleep(waitTime)
				jwt = ""
				continue
			}
			log.Printf("getDirectorsFromInpiBySiret: Rate limited, max retries reached for SIRET %s", siret)
			return nil
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("getDirectorsFromInpiBySiret: HTTP %d for SIRET %s, attempt %d/%d",
				resp.StatusCode, siret, attempt+1, retries)
			if attempt < retries-1 {
				time.Sleep(time.Duration(1<<uint(attempt)) * time.Second)
				continue
			}
			return nil
		}

		var inpiData []map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&inpiData); err != nil {
			log.Printf("getDirectorsFromInpiBySiret: Error decoding response: %v", err)
			if attempt < retries-1 {
				time.Sleep(time.Duration(1<<uint(attempt)) * time.Second)
				continue
			}
			return nil
		}

		return extractDirectorsFromInpiData(inpiData)
	}

	return nil
}

func getINPIJWTToken() (string, error) {
	username := os.Getenv("INPI_USERNAME")
	password := os.Getenv("INPI_PASSWORD")
	useDemoEnv := os.Getenv("INPI_USE_DEMO") == "true"

	if username == "" || password == "" {
		return "", fmt.Errorf("INPI_USERNAME and INPI_PASSWORD environment variables are required")
	}

	authURL := "https://registre-national-entreprises.inpi.fr/api/sso/login"
	if useDemoEnv {
		authURL = "https://registre-national-entreprises-pprod.inpi.fr/api/sso/login"
	}

	authReq := map[string]string{
		"username": username,
		"password": password,
	}

	jsonData, err := json.Marshal(authReq)
	if err != nil {
		return "", fmt.Errorf("error marshaling auth request: %w", err)
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	req, err := http.NewRequest("POST", authURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("error creating auth request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error executing auth request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("authentication failed: status %d, body: %s", resp.StatusCode, string(body))
	}

	var authResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&authResp); err != nil {
		return "", fmt.Errorf("error decoding auth response: %w", err)
	}

	token, ok := authResp["token"].(string)
	if !ok || token == "" {
		return "", fmt.Errorf("no token received in auth response")
	}

	return token, nil
}

func extractDirectorsFromInpiData(inpiData []map[string]interface{}) *DirectorInfo {
	if len(inpiData) == 0 {
		return nil
	}

	companyData := inpiData[0]
	formality, ok := companyData["formality"].(map[string]interface{})
	if !ok {
		return nil
	}

	content, ok := formality["content"].(map[string]interface{})
	if !ok {
		return nil
	}

	personneMorale, ok := content["personneMorale"].(map[string]interface{})
	if !ok {
		return nil
	}

	composition, ok := personneMorale["composition"].(map[string]interface{})
	if !ok {
		return nil
	}

	pouvoirs, ok := composition["pouvoirs"].([]interface{})
	if !ok {
		return nil
	}

	for _, pouvoirInterface := range pouvoirs {
		pouvoir, ok := pouvoirInterface.(map[string]interface{})
		if !ok {
			continue
		}

		if representant, ok := pouvoir["representant"].(map[string]interface{}); ok {
			if descriptionPersonne, ok := representant["descriptionPersonne"].(map[string]interface{}); ok {
				nom, _ := descriptionPersonne["nom"].(string)
				prenomsInterface, ok := descriptionPersonne["prenoms"].([]interface{})
				var prenoms []string
				if ok {
					for _, p := range prenomsInterface {
						if pStr, ok := p.(string); ok {
							prenoms = append(prenoms, pStr)
						}
					}
				}
				prenom := strings.Join(prenoms, " ")

				if nom != "" && prenom != "" {
					return &DirectorInfo{Nom: nom, Prenom: prenom}
				}
			}
		}

		if individu, ok := pouvoir["individu"].(map[string]interface{}); ok {
			if descriptionPersonne, ok := individu["descriptionPersonne"].(map[string]interface{}); ok {
				nom, _ := descriptionPersonne["nom"].(string)
				if nom == "" {
					if nomUsage, ok := descriptionPersonne["nomUsage"].(string); ok {
						nom = nomUsage
					}
				}
				if nom == "" {
					if nomPatronymique, ok := descriptionPersonne["nomPatronymique"].(string); ok {
						nom = nomPatronymique
					}
				}

				var prenom string
				if prenomsInterface, ok := descriptionPersonne["prenoms"].([]interface{}); ok {
					var prenoms []string
					for _, p := range prenomsInterface {
						if pStr, ok := p.(string); ok {
							prenoms = append(prenoms, pStr)
						}
					}
					prenom = strings.Join(prenoms, " ")
				} else if prenomStr, ok := descriptionPersonne["prenom"].(string); ok {
					prenom = prenomStr
				}

				if nom != "" && prenom != "" {
					return &DirectorInfo{Nom: nom, Prenom: prenom}
				}
			}
		}
	}

	return nil
}

func (s *DirectorsService) getDirectorsFromInpiSearch(siren string) *DirectorInfo {
	requestBody := map[string]interface{}{
		"query": map[string]interface{}{
			"type":              "companies",
			"selectedIds":       []interface{}{},
			"sort":              "relevance",
			"order":              "asc",
			"nbResultsPerPage":  "1",
			"page":              "1",
			"filter":            map[string]interface{}{},
			"q":                 siren,
			"advancedSearch":    map[string]interface{}{},
		},
		"aggregations": []string{
			"idt_cp_short",
			"idt_pm_code_form_jur",
			"formality.content.formeExerciceActivitePrincipale",
			"code_ape",
		},
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil
	}

	req, err := http.NewRequest("POST", "https://data.inpi.fr/search", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil
	}
	req.Header.Set("Content-Type", "text/plain;charset=UTF-8")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil
	}

	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		return nil
	}

	hitsArray, ok := hits["hits"].([]interface{})
	if !ok || len(hitsArray) == 0 {
		return nil
	}

	hit, ok := hitsArray[0].(map[string]interface{})
	if !ok {
		return nil
	}

	source, ok := hit["_source"].(map[string]interface{})
	if !ok {
		return nil
	}

	formality, ok := source["formality"].(map[string]interface{})
	if !ok {
		return nil
	}

	content, ok := formality["content"].(map[string]interface{})
	if !ok {
		return nil
	}

	personneMorale, ok := content["personneMorale"].(map[string]interface{})
	if !ok {
		return nil
	}

	composition, ok := personneMorale["composition"].(map[string]interface{})
	if !ok {
		return nil
	}

	pouvoirs, ok := composition["pouvoirs"].([]interface{})
	if !ok {
		return nil
	}

	for _, pouvoir := range pouvoirs {
		pouvoirMap, ok := pouvoir.(map[string]interface{})
		if !ok {
			continue
		}

		representant, ok := pouvoirMap["representant"].(map[string]interface{})
		if !ok {
			continue
		}

		descriptionPersonne, ok := representant["descriptionPersonne"].(map[string]interface{})
		if !ok {
			continue
		}

		nom, _ := descriptionPersonne["nom"].(string)
		prenoms, ok := descriptionPersonne["prenoms"].([]interface{})

		if nom != "" && ok && len(prenoms) > 0 {
			var prenomParts []string
			for _, p := range prenoms {
				if str, ok := p.(string); ok {
					prenomParts = append(prenomParts, str)
				}
			}
			if len(prenomParts) > 0 {
				return &DirectorInfo{
					Nom:    nom,
					Prenom: strings.Join(prenomParts, " "),
				}
			}
		}
	}

	return nil
}

func (s *DirectorsService) getDirectorsFromPappers(siren string) *DirectorInfo {
	url := fmt.Sprintf("https://www.pappers.fr/entreprise/%s", siren)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	body := string(bodyBytes)

	re := regexp.MustCompile(`(?i)Dirigeant[^<]*<[^>]*>([^<]+)</[^>]*>`)
	matches := re.FindStringSubmatch(body)
	if len(matches) > 1 {
		fullName := strings.TrimSpace(matches[1])
		parts := strings.Fields(fullName)
		if len(parts) >= 2 {
			return &DirectorInfo{
				Nom:    parts[len(parts)-1],
				Prenom: strings.Join(parts[:len(parts)-1], " "),
			}
		}
	}

	return nil
}
