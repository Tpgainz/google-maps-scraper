package entreprise

import (
	"regexp"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

const MIN_SCORE_THRESHOLD = 200.0

var typeVoieAbbreviations = map[string]string{
	"RUE":      "RUE",
	"AV":       "AVENUE",
	"AVENUE":   "AVENUE",
	"BD":       "BOULEVARD",
	"BOULEVARD": "BOULEVARD",
	"BLVD":     "BOULEVARD",
	"PL":       "PLACE",
	"PLACE":    "PLACE",
	"CH":       "CHEMIN",
	"CHEMIN":   "CHEMIN",
	"IMP":      "IMPASSE",
	"IMPASSE":  "IMPASSE",
	"AL":       "ALLEE",
	"ALLEE":    "ALLEE",
	"CRS":      "COURS",
	"COURS":    "COURS",
	"PASS":     "PASSAGE",
	"PASSAGE":  "PASSAGE",
	"SQ":       "SQUARE",
	"SQUARE":   "SQUARE",
	"QT":       "QUAI",
	"QUAI":     "QUAI",
	"RTE":      "ROUTE",
	"ROUTE":    "ROUTE",
	"VOIE":     "VOIE",
	"VILLA":    "VILLA",
	"RES":      "RESIDENCE",
	"RESIDENCE": "RESIDENCE",
	"DOM":      "DOMAINE",
	"DOMAINE":  "DOMAINE",
	"LOT":      "LOTISSEMENT",
	"LOTISSEMENT": "LOTISSEMENT",
	"ZA":       "ZONE",
	"ZONE":     "ZONE",
}

var legalForms = []string{
	"SARL", "SA", "SAS", "SASU", "SNC", "SCS", "SCA", "SCE", "SCIC",
	"SELARL", "SELAS", "SELAFA", "SELCA", "EURL", "EIRL", "SCI", "SCM", "SEL",
}

type ParsedAddress struct {
	PostalCode          string
	NumVoie             string
	ComplementNumeroVoie string
	TypeVoie            string
	LibelleVoie         string
	LibelleCommune      string
	AdresseBis          string
}

func normalizeCompanyName(name string) string {
	normalized := strings.TrimSpace(name)
	normalized = strings.ReplaceAll(normalized, "&", "ET")
	normalized = strings.ToUpper(normalized)
	
	normalized = norm.NFD.String(normalized)
	
	var builder strings.Builder
	for _, r := range normalized {
		if unicode.IsMark(r) {
			continue
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) {
			builder.WriteRune(r)
		} else {
			builder.WriteRune(' ')
		}
	}
	normalized = builder.String()
	
	normalized = regexp.MustCompile(`[^\w\s]`).ReplaceAllString(normalized, " ")
	normalized = regexp.MustCompile(`\s+`).ReplaceAllString(normalized, " ")
	normalized = strings.TrimSpace(normalized)
	normalized = strings.ToUpper(normalized)
	return normalized
}

func removeLegalForm(name string) string {
	cleaned := name
	for _, form := range legalForms {
		re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(form) + `\b`)
		cleaned = re.ReplaceAllString(cleaned, "")
	}
	cleaned = regexp.MustCompile(`\s+`).ReplaceAllString(cleaned, " ")
	return strings.TrimSpace(cleaned)
}

func normalizeTypeVoie(abbrev string) string {
	cleaned := strings.ReplaceAll(abbrev, ".", "")
	cleaned = strings.ToUpper(cleaned)
	if normalized, ok := typeVoieAbbreviations[cleaned]; ok {
		return normalized
	}
	return cleaned
}

func parseAddress(address string) ParsedAddress {
	result := ParsedAddress{}
	cleaned := normalizeCompanyName(address)
	
	postalCodeRe := regexp.MustCompile(`(\d{5})`)
	postalCodeMatch := postalCodeRe.FindStringSubmatch(cleaned)
	if len(postalCodeMatch) > 1 {
		result.PostalCode = postalCodeMatch[1]
	}
	
	parts := regexp.MustCompile(`[, ]+`).Split(cleaned, -1)
	var filteredParts []string
	for _, p := range parts {
		if len(p) > 0 {
			filteredParts = append(filteredParts, p)
		}
	}
	
	postalCodeIndex := -1
	for i, p := range filteredParts {
		if regexp.MustCompile(`^\d{5}$`).MatchString(p) {
			postalCodeIndex = i
			break
		}
	}
	
	if postalCodeIndex > 0 {
		result.LibelleCommune = strings.Join(filteredParts[postalCodeIndex+1:], " ")
		
		addressPart := strings.Join(filteredParts[:postalCodeIndex], " ")
		
		typeVoiePatterns := []*regexp.Regexp{
			regexp.MustCompile(`(?i)\b(RUE|AVENUE|BOULEVARD|PLACE|CHEMIN|IMPASSE|ALLEE|COURS|PASSAGE|SQUARE|QUAI|VOIE|ROUTE|VILLA|RESIDENCE|DOMAINE|LOTISSEMENT|ZONE)\s+`),
			regexp.MustCompile(`(?i)\b(PL|AV|BD|BLVD|CH|IMP|AL|CRS|PASS|SQ|QT|RTE|RES|DOM|LOT|ZA)\s+`),
		}
		
		typeVoieIndex := -1
		for _, pattern := range typeVoiePatterns {
			match := pattern.FindStringSubmatch(addressPart)
			if len(match) > 1 {
				abbrev := strings.ToUpper(match[1])
				result.TypeVoie = normalizeTypeVoie(abbrev)
				typeVoieIndex = pattern.FindStringIndex(addressPart)[0]
				afterTypeVoie := addressPart[pattern.FindStringIndex(addressPart)[1]:]
				result.LibelleVoie = strings.TrimSpace(afterTypeVoie)
				break
			}
		}
		
		if typeVoieIndex >= 0 {
			beforeTypeVoie := strings.TrimSpace(addressPart[:typeVoieIndex])
			numVoieRe := regexp.MustCompile(`(?i)\b(\d+)(BIS|TER|QUATER|QUINQUIES)?\s*$`)
			numVoieMatch := numVoieRe.FindStringSubmatch(beforeTypeVoie)
			if len(numVoieMatch) > 1 {
				result.NumVoie = numVoieMatch[1]
				if len(numVoieMatch) > 2 && numVoieMatch[2] != "" {
					result.ComplementNumeroVoie = strings.ToUpper(numVoieMatch[2])
				}
				numIndex := numVoieRe.FindStringIndex(beforeTypeVoie)
				if numIndex != nil {
					beforeNum := strings.TrimSpace(beforeTypeVoie[:numIndex[0]])
					if beforeNum != "" {
						result.AdresseBis = beforeNum
					}
				}
			} else {
				numVoieWithComplementRe := regexp.MustCompile(`(?i)\b(\d+)\s+(BIS|TER|QUATER|QUINQUIES)\s*$`)
				numVoieWithComplementMatch := numVoieWithComplementRe.FindStringSubmatch(beforeTypeVoie)
				if len(numVoieWithComplementMatch) > 1 {
					result.NumVoie = numVoieWithComplementMatch[1]
					result.ComplementNumeroVoie = strings.ToUpper(numVoieWithComplementMatch[2])
					numIndex := numVoieWithComplementRe.FindStringIndex(beforeTypeVoie)
					if numIndex != nil {
						beforeNum := strings.TrimSpace(beforeTypeVoie[:numIndex[0]])
						if beforeNum != "" {
							result.AdresseBis = beforeNum
						}
					}
				} else {
					if beforeTypeVoie != "" && !regexp.MustCompile(`^\d`).MatchString(beforeTypeVoie) {
						result.AdresseBis = beforeTypeVoie
					}
					if result.LibelleVoie != "" {
						afterTypeVoieRe := regexp.MustCompile(`(?i)^(\d+)(BIS|TER|QUATER|QUINQUIES)?\s+`)
						afterTypeVoieMatch := afterTypeVoieRe.FindStringSubmatch(result.LibelleVoie)
						if len(afterTypeVoieMatch) > 1 {
							result.NumVoie = afterTypeVoieMatch[1]
							if len(afterTypeVoieMatch) > 2 && afterTypeVoieMatch[2] != "" {
								result.ComplementNumeroVoie = strings.ToUpper(afterTypeVoieMatch[2])
							}
							afterIndex := afterTypeVoieRe.FindStringIndex(result.LibelleVoie)
							if afterIndex != nil {
								result.LibelleVoie = strings.TrimSpace(result.LibelleVoie[afterIndex[1]:])
							}
						}
					}
				}
			}
		} else {
			numVoieRe := regexp.MustCompile(`(?i)\b(\d+)(BIS|TER|QUATER|QUINQUIES)?\b`)
			numVoieMatch := numVoieRe.FindStringSubmatch(addressPart)
			if len(numVoieMatch) > 1 {
				result.NumVoie = numVoieMatch[1]
				if len(numVoieMatch) > 2 && numVoieMatch[2] != "" {
					result.ComplementNumeroVoie = strings.ToUpper(numVoieMatch[2])
				}
				numIndex := numVoieRe.FindStringIndex(addressPart)
				if numIndex != nil {
					beforeNum := strings.TrimSpace(addressPart[:numIndex[0]])
					if beforeNum != "" {
						result.AdresseBis = beforeNum
					}
					afterNum := strings.TrimSpace(addressPart[numIndex[1]:])
					typeMatch := regexp.MustCompile(`(?i)^([A-Z]{2,})\s+(.+)$`).FindStringSubmatch(afterNum)
					if len(typeMatch) > 2 {
						abbrev := strings.ToUpper(typeMatch[1])
						result.TypeVoie = normalizeTypeVoie(abbrev)
						result.LibelleVoie = typeMatch[2]
					}
				}
			} else {
				numVoieWithComplementRe := regexp.MustCompile(`(?i)\b(\d+)\s+(BIS|TER|QUATER|QUINQUIES)\b`)
				numVoieWithComplementMatch := numVoieWithComplementRe.FindStringSubmatch(addressPart)
				if len(numVoieWithComplementMatch) > 1 {
					result.NumVoie = numVoieWithComplementMatch[1]
					result.ComplementNumeroVoie = strings.ToUpper(numVoieWithComplementMatch[2])
					numIndex := numVoieWithComplementRe.FindStringIndex(addressPart)
					if numIndex != nil {
						beforeNum := strings.TrimSpace(addressPart[:numIndex[0]])
						if beforeNum != "" {
							result.AdresseBis = beforeNum
						}
						afterNum := strings.TrimSpace(addressPart[numIndex[1]:])
						typeMatch := regexp.MustCompile(`(?i)^([A-Z]{2,})\s+(.+)$`).FindStringSubmatch(afterNum)
						if len(typeMatch) > 2 {
							abbrev := strings.ToUpper(typeMatch[1])
							result.TypeVoie = normalizeTypeVoie(abbrev)
							result.LibelleVoie = typeMatch[2]
						}
					}
				} else {
					result.AdresseBis = addressPart
				}
			}
		}
	}
	
	return result
}

func generateSearchQuery(name string, address string) string {
	normalized := normalizeCompanyName(name)
	nameQuery := `denominationUniteLegale:"` + normalized + `"`
	var addressQuery string
	var adresseBisQuery string
	
	if address != "" {
		parsed := parseAddress(address)
		
		if parsed.PostalCode != "" {
			postalCodePrefix := parsed.PostalCode[:2]
			postalCodeCondition := `codePostalEtablissement:(` + parsed.PostalCode + ` OR ` + postalCodePrefix + `*)`
			
			nameQuery += ` AND ` + postalCodeCondition
			addressQuery = postalCodeCondition
			
			if parsed.AdresseBis != "" {
				adresseBisQuery = postalCodeCondition
			}
			
			if parsed.NumVoie != "" {
				addressQuery += ` AND numeroVoieEtablissement:` + parsed.NumVoie
			}
			
			if parsed.TypeVoie != "" {
				addressQuery += ` AND typeVoieEtablissement:` + parsed.TypeVoie
			}
			
			if parsed.LibelleVoie != "" {
				addressQuery += ` AND libelleVoieEtablissement:"` + normalizeCompanyName(parsed.LibelleVoie) + `"`
			}
			
			if parsed.AdresseBis != "" {
				adresseBisQuery += ` AND libelleVoieEtablissement:"` + normalizeCompanyName(parsed.AdresseBis) + `"`
			}
			
			if parsed.LibelleCommune != "" {
				addressQuery += ` AND libelleCommuneEtablissement:"` + normalizeCompanyName(parsed.LibelleCommune) + `"`
				if parsed.AdresseBis != "" {
					adresseBisQuery += ` AND libelleCommuneEtablissement:"` + normalizeCompanyName(parsed.LibelleCommune) + `"`
				}
			}
		} else {
			if parsed.NumVoie != "" {
				addressQuery += `numeroVoieEtablissement:` + parsed.NumVoie
			}
			
			if parsed.TypeVoie != "" {
				if addressQuery != "" {
					addressQuery += ` AND `
				}
				addressQuery += `typeVoieEtablissement:` + parsed.TypeVoie
			}
			
			if parsed.LibelleVoie != "" {
				if addressQuery != "" {
					addressQuery += ` AND `
				}
				addressQuery += `libelleVoieEtablissement:"` + normalizeCompanyName(parsed.LibelleVoie) + `"`
			}
			
			if parsed.LibelleCommune != "" {
				if addressQuery != "" {
					addressQuery += ` AND `
				}
				addressQuery += `libelleCommuneEtablissement:"` + normalizeCompanyName(parsed.LibelleCommune) + `"`
				if parsed.AdresseBis != "" {
					adresseBisQuery += `libelleCommuneEtablissement:"` + normalizeCompanyName(parsed.LibelleCommune) + `"`
				}
			}
		}
	} else {
		nameQuery = `denominationUniteLegale:"` + normalized + `"`
		nameQuery += ` OR denominationUniteLegale:"` + normalized + `"~1`
	}
	
	if addressQuery != "" {
		result := `(` + nameQuery + `) OR (` + addressQuery + `)`
		if adresseBisQuery != "" {
			result += ` OR (` + adresseBisQuery + `)`
		}
		return result
	}
	return nameQuery
}

func findEnseignes(obj interface{}) []string {
	found := make(map[string]bool)
	findEnseignesRecursive(obj, found)
	
	var result []string
	for k := range found {
		result = append(result, k)
	}
	return result
}

func findEnseignesRecursive(obj interface{}, found map[string]bool) {
	if obj == nil {
		return
	}
	
	switch v := obj.(type) {
	case map[string]interface{}:
		for key, value := range v {
			keyLower := strings.ToLower(key)
			if strings.Contains(keyLower, "enseigne") || strings.Contains(keyLower, "denominationusuelle") {
				if str, ok := value.(string); ok && strings.TrimSpace(str) != "" {
					found[strings.TrimSpace(str)] = true
				} else if arr, ok := value.([]interface{}); ok {
					for _, item := range arr {
						if str, ok := item.(string); ok && strings.TrimSpace(str) != "" {
							found[strings.TrimSpace(str)] = true
						}
					}
				}
			}
			findEnseignesRecursive(value, found)
		}
	case []interface{}:
		for _, item := range v {
			findEnseignesRecursive(item, found)
		}
	}
}

func matchesByName(etab map[string]interface{}, searchName string) bool {
	normalizedSearch := normalizeCompanyName(searchName)
	
	ul, ok := etab["uniteLegale"].(map[string]interface{})
	if !ok {
		return false
	}
	
	denomination, _ := ul["denominationUniteLegale"].(string)
	denominationNorm := normalizeCompanyName(denomination)
	if strings.Contains(denominationNorm, normalizedSearch) {
		return true
	}
	
	enseignes := findEnseignes(etab)
	for _, enseigne := range enseignes {
		if strings.Contains(normalizeCompanyName(enseigne), normalizedSearch) {
			return true
		}
	}
	
	return false
}

func scoreResult(etab map[string]interface{}, searchName string, searchAddress string) float64 {
	score := 0.0
	normalizedSearch := normalizeCompanyName(searchName)
	
	ul, ok := etab["uniteLegale"].(map[string]interface{})
	if !ok {
		return score
	}
	
	denomination, _ := ul["denominationUniteLegale"].(string)
	denominationNorm := normalizeCompanyName(denomination)
	
	if denominationNorm == normalizedSearch {
		score += 100.0
	} else if strings.Contains(denominationNorm, normalizedSearch) {
		score += 80.0
	}
	
	enseignes := findEnseignes(etab)
	var enseigneMatch string
	for _, enseigne := range enseignes {
		enseigneNorm := normalizeCompanyName(enseigne)
		if strings.Contains(enseigneNorm, normalizedSearch) {
			enseigneMatch = enseigne
			break
		}
	}
	
	if enseigneMatch != "" {
		enseigneNorm := normalizeCompanyName(enseigneMatch)
		if enseigneNorm == normalizedSearch {
			score += 90.0
		} else {
			score += 70.0
		}
	} else {
		searchWords := strings.Fields(normalizedSearch)
		var filteredWords []string
		for _, word := range searchWords {
			if len(word) > 2 {
				filteredWords = append(filteredWords, word)
			}
		}
		searchWordsCount := len(filteredWords)
		
		if searchWordsCount > 0 {
			matchedWords := 0
			for _, word := range filteredWords {
				wordMatched := strings.Contains(denominationNorm, word)
				if !wordMatched {
					for _, enseigne := range enseignes {
						enseigneNorm := normalizeCompanyName(enseigne)
						if strings.Contains(enseigneNorm, word) {
							wordMatched = true
							break
						}
					}
				}
				if wordMatched {
					matchedWords++
				}
			}
			
			wordMatchRatio := float64(matchedWords) / float64(searchWordsCount)
			if wordMatchRatio >= 0.6 {
				score += 60.0
			} else if wordMatchRatio >= 0.4 {
				score += 40.0
			}
		}
		
		var reverseMatch string
		for _, enseigne := range enseignes {
			enseigneNorm := normalizeCompanyName(enseigne)
			if strings.Contains(normalizedSearch, enseigneNorm) && len(enseigneNorm) > 5 {
				reverseMatch = enseigne
				break
			}
		}
		
		if reverseMatch != "" {
			score += 60.0
		} else if len(enseignes) == 0 {
			score -= 10.0
		}
	}
	
	if searchAddress != "" {
		parsed := parseAddress(searchAddress)
		adresse, _ := etab["adresseEtablissement"].(map[string]interface{})
		
		if parsed.PostalCode != "" {
			codePostal, _ := adresse["codePostalEtablissement"].(string)
			if parsed.PostalCode == codePostal {
				score += 50.0
			}
		}
		
		if parsed.NumVoie != "" {
			numVoie, _ := adresse["numeroVoieEtablissement"].(string)
			if numVoie != "" {
				if parsed.NumVoie == numVoie {
					score += 50.0
				} else {
					searchNum := parseInt(parsed.NumVoie)
					etabNum := parseInt(numVoie)
					if searchNum > 0 && etabNum > 0 {
						diff := abs(searchNum - etabNum)
						if diff <= 2 {
							score -= float64(diff) * 5.0
						} else {
							if parsed.AdresseBis == "" {
								score -= 15.0
							}
						}
					}
				}
			} else {
				if parsed.AdresseBis == "" {
					score -= 20.0
				}
			}
		}
		
		if parsed.TypeVoie != "" {
			typeVoie, _ := adresse["typeVoieEtablissement"].(string)
			if parsed.TypeVoie == typeVoie {
				score += 20.0
			}
		}
		
		if parsed.LibelleVoie != "" {
			libelleVoie, _ := adresse["libelleVoieEtablissement"].(string)
			libelleVoieNorm := normalizeCompanyName(libelleVoie)
			parsedLibelleNorm := normalizeCompanyName(parsed.LibelleVoie)
			
			if parsedLibelleNorm == libelleVoieNorm {
				score += 40.0
			} else if strings.Contains(libelleVoieNorm, parsedLibelleNorm) {
				score += 20.0
			}
		}
		
		if parsed.AdresseBis != "" {
			libelleVoie, _ := adresse["libelleVoieEtablissement"].(string)
			libelleVoieNorm := normalizeCompanyName(libelleVoie)
			normalizedAdresseBis := normalizeCompanyName(parsed.AdresseBis)
			
			if libelleVoieNorm == normalizedAdresseBis {
				score += 60.0
			} else if strings.Contains(libelleVoieNorm, normalizedAdresseBis) {
				score += 40.0
			} else if strings.Contains(normalizedAdresseBis, libelleVoieNorm) {
				score += 30.0
			}
		}
	}
	
	etatAdmin, _ := etab["etatAdministratifEtablissement"].(string)
	etatAdminUL, _ := ul["etatAdministratifUniteLegale"].(string)
	
	periodes, ok := etab["periodesEtablissement"].([]interface{})
	var periodeEtat string
	var dateFin interface{}
	if ok && len(periodes) > 0 {
		periode, ok := periodes[0].(map[string]interface{})
		if ok {
			periodeEtat, _ = periode["etatAdministratifEtablissement"].(string)
			dateFin, _ = periode["dateFin"]
		}
	}
	
	isActive := etatAdmin == "A" && etatAdminUL == "A" && periodeEtat == "A" && (dateFin == nil || dateFin == "")
	if isActive {
		score += 10.0
	}
	
	etablissementSiege, _ := etab["etablissementSiege"].(bool)
	if etablissementSiege {
		score += 10.0
	}
	
	isClosed := etatAdmin == "F" || etatAdminUL == "F" || (dateFin != nil && dateFin != "") || periodeEtat == "F"
	if isClosed {
		score -= 30.0
	}
	
	return score
}

func parseInt(s string) int {
	var result int
	for _, r := range s {
		if r >= '0' && r <= '9' {
			result = result*10 + int(r-'0')
		} else {
			break
		}
	}
	return result
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
