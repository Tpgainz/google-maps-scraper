package bodacc

import (
	"strings"
)

func ProcessForSearch(companyName string) string {
	if companyName == strings.ToUpper(companyName) && len(companyName) < 10 {
		chars := strings.Split(companyName, "")
		return strings.Join(chars, ".")
	}
	
	if len(companyName) > 30 {
		words := strings.Fields(companyName)
		if len(words) >= 2 {
			return strings.Join(words[:2], " ")
		}
	}
	
	return companyName
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
