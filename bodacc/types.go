package bodacc

type BodaccCompanyInfo struct {
	SocieteDirigeants []string `json:"societeDirigeants"`
	SocieteForme      string   `json:"societeForme"`
	SocieteCreation   string   `json:"societeCreation"`
	SocieteCloture    string   `json:"societeCloture"`
	SocieteSiren      string   `json:"societeSiren"`
	SocieteLink       string   `json:"societeLink"`
	PappersURL        string   `json:"pappersURL"`
	City              string   `json:"city"`
}

type BodaccSearchResult struct {
	Success      bool                `json:"success"`
	Data         []BodaccCompanyInfo `json:"data,omitempty"`
	Error        string              `json:"error,omitempty"`
	TotalResults int                 `json:"totalResults,omitempty"`
}

type BodaccRawResult struct {
	Familleavis    string   `json:"familleavis"`
	Registre       []string `json:"registre"`
	Depot          *string  `json:"depot,omitempty"`
	Listepersonnes *string  `json:"listepersonnes,omitempty"`
	Dateparution   string   `json:"dateparution"`
	URLComplete    string   `json:"url_complete"`
	Commercant     string   `json:"commercant"`
	Ville          string   `json:"ville"`
}

type ParsedPersonnes struct {
	Personne *struct {
		Administration interface{} `json:"administration,omitempty"`
		FormeJuridique *string     `json:"formeJuridique,omitempty"`
	} `json:"personne,omitempty"`
}

type ParsedDepot struct {
	DateCloture *string `json:"dateCloture,omitempty"`
}

type BodaccAPIResponse struct {
	TotalCount int               `json:"total_count"`
	Results    []BodaccRawResult `json:"results"`
}
