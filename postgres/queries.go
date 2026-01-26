package postgres

// CompanyDataQuery builds a query for checking existing company data.
type CompanyDataQuery struct {
	title          string
	address        string
	ownerID        string
	organizationID string
}

// NewCompanyDataQuery creates a new CompanyDataQuery builder.
func NewCompanyDataQuery(title, address, ownerID, organizationID string) *CompanyDataQuery {
	return &CompanyDataQuery{
		title:          title,
		address:        address,
		ownerID:        ownerID,
		organizationID: organizationID,
	}
}

// Build returns the SQL query string and arguments for company data lookup.
func (q *CompanyDataQuery) Build() (string, []interface{}, bool) {
	if q.title == "" || q.address == "" {
		return "", nil, false
	}

	baseSelect := `SELECT
		societe_dirigeants, societe_siren, societe_forme,
		societe_creation, societe_cloture, societe_link, societe_diffusion
		FROM results
		WHERE LOWER(TRIM(title)) = LOWER(TRIM($1))
		AND LOWER(TRIM(address)) = LOWER(TRIM($2))`

	companyCond := `AND (societe_dirigeants IS NOT NULL AND societe_dirigeants != ''
		OR societe_siren IS NOT NULL AND societe_siren != '')
		LIMIT 1`

	if q.ownerID != "" && q.organizationID != "" {
		query := baseSelect + `
			AND (user_id = $3 OR organization_id = $4)
			` + companyCond
		return query, []interface{}{q.title, q.address, q.ownerID, q.organizationID}, true
	}

	if q.ownerID != "" {
		query := baseSelect + `
			AND user_id = $3
			` + companyCond
		return query, []interface{}{q.title, q.address, q.ownerID}, true
	}

	if q.organizationID != "" {
		query := baseSelect + `
			AND organization_id = $3
			` + companyCond
		return query, []interface{}{q.title, q.address, q.organizationID}, true
	}

	return "", nil, false
}

// DuplicateURLQuery builds a query for checking duplicate URLs.
type DuplicateURLQuery struct {
	url            string
	userID         string
	organizationID string
}

// NewDuplicateURLQuery creates a new DuplicateURLQuery builder.
func NewDuplicateURLQuery(url, userID, organizationID string) *DuplicateURLQuery {
	return &DuplicateURLQuery{
		url:            url,
		userID:         userID,
		organizationID: organizationID,
	}
}

// Build returns the SQL query string and arguments for duplicate URL check.
func (q *DuplicateURLQuery) Build() (string, []interface{}, bool) {
	if q.url == "" {
		return "", nil, false
	}

	if q.userID != "" && q.organizationID != "" {
		query := `SELECT COUNT(*) FROM results
			WHERE link = $1 AND (user_id = $2 OR organization_id = $3)`
		return query, []interface{}{q.url, q.userID, q.organizationID}, true
	}

	if q.userID != "" {
		query := `SELECT COUNT(*) FROM results
			WHERE link = $1 AND user_id = $2`
		return query, []interface{}{q.url, q.userID}, true
	}

	if q.organizationID != "" {
		query := `SELECT COUNT(*) FROM results
			WHERE link = $1 AND organization_id = $2`
		return query, []interface{}{q.url, q.organizationID}, true
	}

	return "", nil, false
}
