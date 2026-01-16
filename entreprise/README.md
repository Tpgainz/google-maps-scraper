# Entreprise Service

A Go implementation for searching French company information. Supports INSEE and INPI (Institut National de la Propriété Industrielle) RNE APIs, with automatic director enrichment from multiple sources.

## Features

- Search companies by name and address using INSEE API
- Support for INPI RNE API as fallback
- Advanced scoring system with minimum threshold (200 points)
- Automatic director enrichment from multiple sources:
  - INPI RNE API (by SIRET)
  - Annuaire des Entreprises (public API)
  - INPI Search API
  - BODACC (for director information only)
  - Pappers.fr (scraping, last resort)
- Address parsing and normalization
- Company name normalization with accent removal
- Pappers.fr URL generation

## Usage

```go
package main

import (
    "fmt"
    "log"
    "your-module/entreprise"
)

func main() {
    service := entreprise.NewService()

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

### CompanyInfo

Contains company information including:

- `SocieteDirigeants`: List of company directors
- `SocieteForme`: Legal form of the company
- `SocieteCreation`: Creation date
- `SocieteCloture`: Closure date
- `SocieteSiren`: SIREN number
- `SocieteLink`: Complete URL
- `PappersURL`: Pappers.fr URL
- `City`: Company city

### SearchResult

Contains search results:

- `Success`: Boolean indicating if search was successful
- `Data`: Array of company information
- `Error`: Error message if search failed
- `TotalResults`: Total number of results

## API

### Service

#### NewService()

Creates a new service instance that chains INSEE and INPI services. Automatically initializes available services based on environment variables.

#### SearchCompany(companyName, address string) (\*SearchResult, error)

Searches for companies by name and address using a chain of services (INSEE → INPI). Returns search results from the first successful service. Results are scored and filtered by minimum threshold (200 points).

### INPI Service

#### NewINPIService(username, password string, useDemoEnv bool) \*INPIService

Creates a new INPI service instance. Requires INPI e-procedures account credentials.

- `username`: INPI e-procedures username
- `password`: INPI e-procedures password
- `useDemoEnv`: Set to `true` to use demo environment

#### SearchCompany(companyName, address string) (\*SearchResult, error)

Searches for companies by name and address using INPI RNE API. Returns search results or an error.

**Note**: This implementation uses the INPI RNE (Registre National des Entreprises) API v4.0. The API endpoints are:

- Authentication: `/api/sso/login` (POST)
- Company search: `/api/companies` (GET) with query parameters `denomination` (company name) or `siren`, and optional `departments` filter

The implementation parses the complex JSON response structure to extract company information including directors from the `composition.pouvoirs` array.

## Configuration

The service automatically detects available credentials and chains services in order:

- `INSEE_API_KEY=<your-key>` - INSEE API key (tried first)
- `INPI_USERNAME=<your-username>` - INPI e-procedures username
- `INPI_PASSWORD=<your-password>` - INPI e-procedures password
- `INPI_USE_DEMO=true` - Use demo environment (optional, defaults to production)

## Search Strategy

1. **INSEE Search**: Primary search using INSEE SIRET API with advanced query generation
2. **Scoring**: Results are scored based on:
   - Exact/partial name matches (100/80 points)
   - Enseigne (commercial name) matches (90/70 points)
   - Address matching (postal code, street number, type, name)
   - Active status bonus (+10 points)
   - Head office bonus (+5 points)
   - Closure penalty (-30 points)
3. **Threshold Filtering**: Only results above 200 points are returned
4. **Director Enrichment**: Automatically fetches directors from multiple sources if missing
5. **INPI Fallback**: If INSEE returns no results, tries INPI RNE API

## Address Processing

- Parses addresses to extract:
  - Postal code (with prefix matching support)
  - Street number (with BIS/TER/QUATER support)
  - Street type (normalized abbreviations)
  - Street name
  - City name
- Normalizes addresses for matching

## Company Name Processing

- Normalizes company names:
  - Removes accents and special characters
  - Converts to uppercase
  - Handles multiple spaces
- Generates search queries combining name and address conditions
