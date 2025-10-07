package bodacc

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

func ParsePersonnes(listepersonnes *string) ([]string, string) {
	if listepersonnes == nil {
		return []string{}, ""
	}

	var personnesData ParsedPersonnes
	if err := json.Unmarshal([]byte(*listepersonnes), &personnesData); err != nil {
		log.Printf("Erreur parsing listepersonnes: %v, data: %s", err, *listepersonnes)
		return []string{}, ""
	}

	if personnesData.Personne == nil {
		return []string{}, ""
	}

	var societeDirigeants []string
	administration := personnesData.Personne.Administration

	log.Printf("Parsing personnes data: %s, administration type: %T", *listepersonnes, administration)

	switch admin := administration.(type) {
	case []interface{}:
		for _, item := range admin {
			if str, ok := item.(string); ok && strings.TrimSpace(str) != "" {
				societeDirigeants = append(societeDirigeants, str)
			}
		}
	case string:
		cleanString := strings.TrimSpace(admin)
		if cleanString != "" {
			societeDirigeants = append(societeDirigeants, cleanString)
		}
	}

	societeForme := ""
	if personnesData.Personne.FormeJuridique != nil {
		societeForme = *personnesData.Personne.FormeJuridique
	}

	return societeDirigeants, societeForme
}

func ParseDepot(depot *string) string {
	if depot == nil {
		return ""
	}

	var depotData ParsedDepot
	if err := json.Unmarshal([]byte(*depot), &depotData); err != nil {
		log.Printf("Erreur parsing depot: %v, data: %s", err, *depot)
		return ""
	}

	if depotData.DateCloture != nil {
		return *depotData.DateCloture
	}

	return ""
}

func CreatePappersURL(commercant, siren string) string {
	cleanCommercant := strings.ToLower(commercant)
	cleanCommercant = strings.ReplaceAll(cleanCommercant, " ", "-")
	return fmt.Sprintf("https://www.pappers.fr/entreprise/%s-%s", cleanCommercant, siren)
}

func TransformResult(result BodaccRawResult, dpcClosureDates map[string]string) BodaccCompanyInfo {
	siren := strings.ReplaceAll(result.Registre[0], " ", "")
	societeDirigeants, societeForme := ParsePersonnes(result.Listepersonnes)
	
	dateCloture := ParseDepot(result.Depot)
	if dateCloture == "" {
		if closureDate, exists := dpcClosureDates[siren]; exists {
			dateCloture = closureDate
		}
	}

	return BodaccCompanyInfo{
		City:              result.Ville,
		SocieteDirigeants: societeDirigeants,
		SocieteForme:      societeForme,
		SocieteCreation:   result.Dateparution,
		SocieteCloture:    dateCloture,
		SocieteLink:       result.URLComplete,
		SocieteSiren:      siren,
		PappersURL:        CreatePappersURL(result.Commercant, siren),
	}
}
