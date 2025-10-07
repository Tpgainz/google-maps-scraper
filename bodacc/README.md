# BODACC Service

A Go implementation of the BODACC (Bulletin Officiel Des Annonces Civiles et Commerciales) service for searching French company information.

## Features

- Search companies by name and address
- Primary search with full address matching
- Fallback search with simplified address matching
- Automatic department number extraction
- Company name processing for better search results
- Address refinement and simplification
- JSON data parsing and transformation
- Pappers.fr URL generation
- **Automatic director scraping**: When SocieteDirigeants is null, automatically scrapes Pappers.fr to extract director information using scrapemate

## Usage

```go
package main

import (
    "fmt"
    "log"
    "your-module/bodacc"
)

func main() {
    service := bodacc.NewBodaccService()

    companyName := "Example Company"
    address := "123 Rue de la Paix, 75001 Paris"

    result, err := service.SearchCompany(companyName, address)
    if err != nil {
        log.Fatalf("Erreur lors de la recherche: %v", err)
    }

    if result.Success {
        fmt.Printf("Recherche réussie! %d résultats trouvés\n", result.TotalResults)
        for _, company := range result.Data {
            fmt.Printf("SIREN: %s\n", company.SocieteSiren)
            fmt.Printf("Nom: %s\n", company.SocieteForme)
            fmt.Printf("Dirigeants: %v\n", company.SocieteDirigeants)
            fmt.Printf("Ville: %s\n", company.City)
        }
    }
}
```

## Data Structures

### BodaccCompanyInfo

Contains company information including:

- `SocieteDirigeants`: List of company directors
- `SocieteForme`: Legal form of the company
- `SocieteCreation`: Creation date
- `SocieteCloture`: Closure date
- `SocieteSiren`: SIREN number
- `SocieteLink`: Complete URL
- `PappersURL`: Pappers.fr URL
- `City`: Company city

### BodaccSearchResult

Contains search results:

- `Success`: Boolean indicating if search was successful
- `Data`: Array of company information
- `Error`: Error message if search failed
- `TotalResults`: Total number of results

## API

### NewBodaccService()

Creates a new BODACC service instance.

### SearchCompany(companyName, address string) (\*BodaccSearchResult, error)

Searches for companies by name and address. Returns search results or an error.

## Search Strategy

1. **Primary Search**: Uses full company name and refined address
2. **Fallback Search**: If no results, uses simplified address and LIKE conditions
3. **City Filtering**: Filters results by city if available

## Address Processing

- Extracts department number from postal code
- Refines addresses (expands abbreviations)
- Simplifies addresses for fallback search

## Company Name Processing

- Handles uppercase short names by adding dots
- Truncates long names to first two words
- Creates LIKE conditions for flexible matching
