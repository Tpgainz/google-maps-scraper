package entreprise

import (
	"log"
	"os"
	"sync"
)

var _ CompanySearchService = (*Service)(nil)

type Service struct {
	inseeService     *INSEEService
	inpiService      *INPIService
	gouvService      *GOUVService
	directorsService *DirectorsService
}

var (
	serviceInstance *Service
	serviceOnce     sync.Once
)

func NewService() *Service {
	serviceOnce.Do(func() {
		serviceInstance = &Service{}

		inseeApiKey := getEnvOrDefault("INSEE_API_KEY", "")
		if inseeApiKey != "" {
			serviceInstance.inseeService = NewINSEEService(inseeApiKey)
		}

		inpiUsername := getEnvOrDefault("INPI_USERNAME", "")
		inpiPassword := getEnvOrDefault("INPI_PASSWORD", "")
		useDemoEnv := getEnvOrDefault("INPI_USE_DEMO", "false") == "true"
		if inpiUsername != "" && inpiPassword != "" {
			serviceInstance.inpiService = NewINPIService(inpiUsername, inpiPassword, useDemoEnv)
		}

		serviceInstance.gouvService = NewGOUVService()
		serviceInstance.directorsService = NewDirectorsService()

		log.Println("Service: all enterprise services initialized")
	})

	return serviceInstance
}

func (s *Service) SearchCompany(companyName, address string) (*SearchResult, error) {
	if s.inseeService != nil {
		result, err := s.inseeService.SearchCompany(companyName, address)
		if err != nil {
			log.Printf("Service: INSEE error for '%s': %v", companyName, err)
		} else if result != nil && result.Success && len(result.Data) > 0 {
			return result, nil
		}
	}

	if s.inpiService != nil {
		result, err := s.inpiService.SearchCompany(companyName, address)
		if err != nil {
			log.Printf("Service: INPI error for '%s': %v", companyName, err)
		} else if result != nil && result.Success && len(result.Data) > 0 {
			return result, nil
		}
	}

	if s.gouvService != nil {
		result, err := s.gouvService.SearchCompany(companyName, address)
		if err != nil {
			log.Printf("Service: GOUV error for '%s': %v", companyName, err)
		} else if result != nil && result.Success && len(result.Data) > 0 {
			return result, nil
		}
	}

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
