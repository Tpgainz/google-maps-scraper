package entreprise

import (
	"log"
	"os"
)

var _ CompanySearchService = (*Service)(nil)

type Service struct {
	inseeService    *INSEEService
	inpiService    *INPIService
	gouvService    *GOUVService
	directorsService *DirectorsService
}

func NewService() *Service {
	service := &Service{}

	inseeApiKey := getEnvOrDefault("INSEE_API_KEY", "")
	if inseeApiKey != "" {
		service.inseeService = NewINSEEService(inseeApiKey)
		log.Println("Service: INSEE service initialized")
	}

	inpiUsername := getEnvOrDefault("INPI_USERNAME", "")
	inpiPassword := getEnvOrDefault("INPI_PASSWORD", "")
	useDemoEnv := getEnvOrDefault("INPI_USE_DEMO", "false") == "true"
	if inpiUsername != "" && inpiPassword != "" {
		service.inpiService = NewINPIService(inpiUsername, inpiPassword, useDemoEnv)
		log.Println("Service: INPI service initialized")
	}

	service.gouvService = NewGOUVService()
	log.Println("Service: GOUV service initialized")

	service.directorsService = NewDirectorsService()
	log.Println("Service: Directors service initialized")

	return service
}

func (s *Service) SearchCompany(companyName, address string) (*SearchResult, error) {
	log.Printf("Service: Starting search for '%s' at '%s'", companyName, address)

	if s.inseeService != nil {
		log.Println("Service: Trying INSEE service...")
		result, err := s.inseeService.SearchCompany(companyName, address)
		if err != nil {
			log.Printf("Service: INSEE service error: %v", err)
		} else if result != nil && result.Success && len(result.Data) > 0 {
			log.Printf("Service: INSEE service found %d results", len(result.Data))
			return result, nil
		} else if result != nil {
			log.Printf("Service: INSEE service returned no results (Success=%v, Data length=%d)", 
				result.Success, len(result.Data))
		}
	}

	if s.inpiService != nil {
		log.Println("Service: Trying INPI service...")
		result, err := s.inpiService.SearchCompany(companyName, address)
		if err != nil {
			log.Printf("Service: INPI service error: %v", err)
		} else if result != nil && result.Success && len(result.Data) > 0 {
			log.Printf("Service: INPI service found %d results", len(result.Data))
			return result, nil
		} else if result != nil {
			log.Printf("Service: INPI service returned no results (Success=%v, Data length=%d)", 
				result.Success, len(result.Data))
		}
	}

	if s.gouvService != nil {
		log.Println("Service: Trying GOUV service...")
		result, err := s.gouvService.SearchCompany(companyName, address)
		if err != nil {
			log.Printf("Service: GOUV service error: %v", err)
		} else if result != nil && result.Success && len(result.Data) > 0 {
			log.Printf("Service: GOUV service found %d results", len(result.Data))
			return result, nil
		} else if result != nil {
			log.Printf("Service: GOUV service returned no results (Success=%v, Data length=%d)", 
				result.Success, len(result.Data))
		}
	}

	log.Println("Service: No results found from INSEE, INPI or GOUV services")
	return &SearchResult{
		Success:      true,
		Data:         []CompanyInfo{},
		TotalResults: 0,
	}, nil
}

func (s *Service) GetDirectors(siren string, siret string) *DirectorInfo {
	if s.directorsService != nil {
		return s.directorsService.GetDirectors(siren, siret)
	}
	return nil
}

func getEnvOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

