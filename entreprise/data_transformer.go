package entreprise

import (
	"fmt"
	"strings"
)

func CreatePappersURL(commercant, siren string) string {
	cleanCommercant := strings.ToLower(commercant)
	cleanCommercant = strings.ReplaceAll(cleanCommercant, " ", "-")
	return fmt.Sprintf("https://www.pappers.fr/entreprise/%s-%s", cleanCommercant, siren)
}
