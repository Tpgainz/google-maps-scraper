package gmaps

import (
	"testing"

	"github.com/gosom/scrapemate"
)

func TestPlaceJobWithBodaccExtraction(t *testing.T) {
	// Create a PlaceJob with BODACC extraction enabled
	job := NewPlaceJob(
		"parent123",
		"en",
		"https://maps.google.com/place/test",
		"user123",
		"org456",
		true,  // extractEmail
		false, // extractExtraReviews
		WithBodaccExtraction(), // Enable BODACC extraction
	)

	// Verify the job has BODACC extraction enabled
	if !job.ExtractBodacc {
		t.Error("BODACC extraction should be enabled")
	}

	if !job.ExtractEmail {
		t.Error("Email extraction should be enabled")
	}

	// Test job properties
	if job.OwnerID != "user123" {
		t.Error("OwnerID should match")
	}

	if job.OrganizationID != "org456" {
		t.Error("OrganizationID should match")
	}
}

func TestPlaceJobWithoutBodaccExtraction(t *testing.T) {
	// Create a PlaceJob without BODACC extraction
	job := NewPlaceJob(
		"parent123",
		"en",
		"https://maps.google.com/place/test",
		"user123",
		"org456",
		true,  // extractEmail
		false, // extractExtraReviews
		// No BODACC extraction option
	)

	// Verify the job does not have BODACC extraction enabled
	if job.ExtractBodacc {
		t.Error("BODACC extraction should be disabled")
	}

	if !job.ExtractEmail {
		t.Error("Email extraction should be enabled")
	}
}

func TestBodaccJobCreationFromPlaceJob(t *testing.T) {
	// This test would require a mock response, but we can test the job creation logic
	job := NewPlaceJob(
		"parent123",
		"en", 
		"https://maps.google.com/place/test",
		"user123",
		"org456",
		true,  // extractEmail
		false, // extractExtraReviews
		WithBodaccExtraction(),
	)

	// Verify job configuration
	if job.GetID() == "" {
		t.Error("Job should have an ID")
	}

	if job.ParentID != "parent123" {
		t.Error("ParentID should match")
	}

	// Test that the job implements the correct interface
	var _ scrapemate.IJob = job
}
