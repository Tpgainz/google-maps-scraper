package bodacc

import (
	"testing"
)

func TestExtractDepartmentNumber(t *testing.T) {
	tests := []struct {
		address  string
		expected string
	}{
		{"123 Rue de la Paix, 75001 Paris", "75"},
		{"456 Avenue des Champs, 69000 Lyon", "69"},
		{"789 Boulevard Saint-Germain, 13000 Marseille", "13"},
		{"No postal code", ""},
	}

	for _, test := range tests {
		result := ExtractDepartmentNumber(test.address)
		if result != test.expected {
			t.Errorf("ExtractDepartmentNumber(%s) = %s, expected %s", test.address, result, test.expected)
		}
	}
}

func TestRefineAddress(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"123 Imp. de la Paix", "123 Impasse de la Paix"},
		{"456 Av. des Champs", "456 Avenue des Champs"},
		{"789 Bd Saint-Germain", "789 Boulevard Saint-Germain"},
	}

	for _, test := range tests {
		result := RefineAddress(test.input)
		if result != test.expected {
			t.Errorf("RefineAddress(%s) = %s, expected %s", test.input, result, test.expected)
		}
	}
}

func TestProcessForSearch(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"ABC", "A.B.C"},
		{"Very Long Company Name That Exceeds Thirty Characters", "Very Long"},
		{"Normal Company", "Normal Company"},
	}

	for _, test := range tests {
		result := ProcessForSearch(test.input)
		if result != test.expected {
			t.Errorf("ProcessForSearch(%s) = %s, expected %s", test.input, result, test.expected)
		}
	}
}

func TestCreateLikeConditions(t *testing.T) {
	result := CreateLikeConditions("Test Company Name")
	expected := `commercant like "%Test%" OR commercant like "%Company%" OR commercant like "%Name%"`
	
	if result != expected {
		t.Errorf("CreateLikeConditions() = %s, expected %s", result, expected)
	}
}

func TestCreatePappersURL(t *testing.T) {
	result := CreatePappersURL("Test Company", "123456789")
	expected := "https://www.pappers.fr/entreprise/test-company-123456789"
	
	if result != expected {
		t.Errorf("CreatePappersURL() = %s, expected %s", result, expected)
	}
}
