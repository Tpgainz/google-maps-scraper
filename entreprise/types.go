package entreprise

type CompanyInfo struct {
	SocieteDirigeants []string `json:"societeDirigeants"`
	SocieteForme      string   `json:"societeForme"`
	SocieteNom        string   `json:"societeNom,omitempty"`
	SocieteCreation   string   `json:"societeCreation"`
	SocieteCloture    string   `json:"societeCloture"`
	SocieteSiren      string   `json:"societeSiren"`
	SocieteLink       string   `json:"societeLink"`
	PappersURL        string   `json:"pappersURL"`
	City              string   `json:"city"`
	MatchScore        float64  `json:"matchScore,omitempty"`
	SocieteDiffusion  bool     `json:"societeDiffusion"`
}

type SearchResult struct {
	Success      bool          `json:"success"`
	Data         []CompanyInfo `json:"data,omitempty"`
	Error        string        `json:"error,omitempty"`
	TotalResults int           `json:"totalResults,omitempty"`
}

