package entreprise

import (
	"regexp"
	"strings"
)

func ExtractDepartmentNumber(address string) string {
	re := regexp.MustCompile(`(\d{5})`)
	matches := re.FindStringSubmatch(address)
	if len(matches) > 1 {
		return matches[1][:2]
	}
	return ""
}

func RefineAddress(address string) string {
	refined := address
	refined = strings.ReplaceAll(refined, "Imp.", "Impasse")
	refined = strings.ReplaceAll(refined, "Av.", "Avenue")
	refined = strings.ReplaceAll(refined, "Pl.", "Place")
	refined = strings.ReplaceAll(refined, "Bd", "Boulevard")
	refined = strings.ReplaceAll(refined, "Sq.", "Square")
	refined = strings.ReplaceAll(refined, "Rte", "Route")
	refined = strings.ReplaceAll(refined, "C.Cial", "Centre Commercial")
	
	re := regexp.MustCompile(`\d+-\d+`)
	refined = re.ReplaceAllString(refined, "")
	
	return strings.TrimSpace(refined)
}

func SimplifyAddress(address string) string {
	simplified := address
	simplified = strings.ReplaceAll(simplified, "Imp.", "Impasse")
	simplified = strings.ReplaceAll(simplified, "Av.", "Avenue")
	simplified = strings.ReplaceAll(simplified, "Pl.", "Place")
	simplified = strings.ReplaceAll(simplified, "Bd", "Boulevard")
	simplified = strings.ReplaceAll(simplified, "Sq.", "Square")
	simplified = strings.ReplaceAll(simplified, "C.Cial", "Centre Commercial")
	
	re := regexp.MustCompile(`\d+-\d+`)
	simplified = re.ReplaceAllString(simplified, "")
	
	re = regexp.MustCompile(`\d+`)
	simplified = re.ReplaceAllString(simplified, "")
	
	parts := strings.Split(simplified, ",")
	if len(parts) > 0 {
		simplified = parts[0]
	}
	
	return strings.TrimSpace(simplified)
}
