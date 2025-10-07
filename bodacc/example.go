package bodacc

import (
	"fmt"
	"log"
)

func ExampleUsage() {
	service := NewBodaccService()
	
	companyName := "Example Company"
	address := "123 Rue de la Paix, 75001 Paris"
	
	result, err := service.SearchCompany(companyName, address)
	if err != nil {
		log.Fatalf("Erreur lors de la recherche: %v", err)
	}
	
	if result.Success {
		fmt.Printf("Recherche réussie! %d résultats trouvés\n", result.TotalResults)
		for i, company := range result.Data {
			fmt.Printf("Résultat %d:\n", i+1)
			fmt.Printf("  SIREN: %s\n", company.SocieteSiren)
			fmt.Printf("  Nom: %s\n", company.SocieteForme)
			fmt.Printf("  Dirigeants: %v\n", company.SocieteDirigeants)
			fmt.Printf("  Ville: %s\n", company.City)
			fmt.Printf("  Lien: %s\n", company.SocieteLink)
			fmt.Printf("  Pappers: %s\n", company.PappersURL)
			
			if len(company.SocieteDirigeants) == 0 {
				fmt.Println("  Note: Aucun dirigeant trouvé dans BODACC, tentative de scraping Pappers...")
			}
		}
	} else {
		fmt.Printf("Erreur: %s\n", result.Error)
	}
}
