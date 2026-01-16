package entreprise

type CompanySearchService interface {
	SearchCompany(companyName, address string) (*SearchResult, error)
}
