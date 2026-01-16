package entreprise

import (
	"strings"
)

func ProcessForSearch(companyName string) string {
	trimmed := strings.TrimSpace(companyName)
	
	if len(trimmed) > 50 {
		words := strings.Fields(trimmed)
		if len(words) >= 3 {
			return strings.Join(words[:3], " ")
		} else if len(words) >= 2 {
			return strings.Join(words[:2], " ")
		}
	}
	
	return trimmed
}

func CreateLikeConditions(companyName string) string {
	words := strings.Fields(strings.TrimSpace(companyName))
	var conditions []string
	
	for _, word := range words {
		if len(word) > 0 {
			conditions = append(conditions, `commercant like "%`+word+`%"`)
		}
	}
	
	return strings.Join(conditions, " OR ")
}
