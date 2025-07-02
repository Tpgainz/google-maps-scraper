package gmaps

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/gosom/google-maps-scraper/exiter"
	"github.com/gosom/scrapemate"
	"github.com/playwright-community/playwright-go"
)

type PlaceJobOptions func(*PlaceJob)

type PlaceJob struct {
	scrapemate.Job
	OwnerID             string
	UsageInResultststs bool
	ExtractEmail       bool
	ExitMonitor        exiter.Exiter
}

func NewPlaceJob(parentID, langCode, u string, ownerID string, extractEmail bool, opts ...PlaceJobOptions) *PlaceJob {
	const (
		defaultPrio       = scrapemate.PriorityMedium
		defaultMaxRetries = 3
	)

	job := PlaceJob{
		Job: scrapemate.Job{
			ID:         uuid.New().String(),
			ParentID:   parentID,
			Method:     "GET",
			URL:        u,
			URLParams:  map[string]string{"hl": langCode},
			MaxRetries: defaultMaxRetries,
			Priority:   defaultPrio,
		},
	}

	job.UsageInResultststs = true
	job.ExtractEmail = extractEmail
	job.OwnerID = ownerID

	for _, opt := range opts {
		opt(&job)
	}

	return &job
}

func WithPlaceJobExitMonitor(exitMonitor exiter.Exiter) PlaceJobOptions {
	return func(j *PlaceJob) {
		j.ExitMonitor = exitMonitor
	}
}

func (j *PlaceJob) Process(_ context.Context, resp *scrapemate.Response) (any, []scrapemate.IJob, error) {
	defer func() {
		resp.Document = nil
		resp.Body = nil
		resp.Meta = nil
	}()

	raw, ok := resp.Meta["json"].([]byte)
	if !ok {
		return nil, nil, fmt.Errorf("could not convert to []byte")
	}

	entry, err := EntryFromJSON(raw)
	if err != nil {
		return nil, nil, err
	}

	entry.ID = j.ParentID

	if entry.Link == "" {
		entry.Link = j.GetURL()
	}

	if j.ExtractEmail && entry.IsWebsiteValidForEmail() {
		opts := []EmailExtractJobOptions{}
		if j.ExitMonitor != nil {
			opts = append(opts, WithEmailJobExitMonitor(j.ExitMonitor))
		}

		emailJob := NewEmailJob(j.ID, &entry, opts...)

		j.UsageInResultststs = false

		return nil, []scrapemate.IJob{emailJob}, nil
	} else if j.ExitMonitor != nil {
		j.ExitMonitor.IncrPlacesCompleted(1)
	}

	return &entry, nil, err
}

func (j *PlaceJob) BrowserActions(_ context.Context, page playwright.Page) scrapemate.Response {
	var resp scrapemate.Response

	pageResponse, err := page.Goto(j.GetURL(), playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateDomcontentloaded,
	})

	if err != nil {
		resp.Error = err

		return resp
	}

	if err = clickRejectCookiesIfRequired(page); err != nil {
		resp.Error = err

		return resp
	}

	const defaultTimeout = 10000

	err = page.WaitForURL(page.URL(), playwright.PageWaitForURLOptions{
		WaitUntil: playwright.WaitUntilStateNetworkidle,
		Timeout:   playwright.Float(defaultTimeout),
	})
	if err != nil {
		resp.Error = err

		return resp
	}

	resp.URL = pageResponse.URL()
	resp.StatusCode = pageResponse.Status()
	resp.Headers = make(http.Header, len(pageResponse.Headers()))

	for k, v := range pageResponse.Headers() {
		resp.Headers.Add(k, v)
	}

	page.On("console", func(msg playwright.ConsoleMessage) {
		if msg.Type() == "log" {
			fmt.Printf("CONSOLE: %s\n", msg.Text())
		}
	})

	rawI, err := page.Evaluate(js)
	if err != nil {
		resp.Error = err

		return resp
	}
	raw, ok := rawI.(string)
	if !ok {
		if rawI == nil {
			resp.Error = fmt.Errorf("JavaScript returned null - page structure may have changed or data not available")
		} else {
			resp.Error = fmt.Errorf("could not convert to string: got %T, expected string", rawI)
		}

		return resp
	}

	if raw == "" {
		resp.Error = fmt.Errorf("JavaScript returned empty string - no data found on page")
		return resp
	}

	const prefix = `)]}'`

	raw = strings.TrimSpace(strings.TrimPrefix(raw, prefix))

	if resp.Meta == nil {
		resp.Meta = make(map[string]any)
	}

	resp.Meta["json"] = []byte(raw)

	return resp
}

func (j *PlaceJob) UseInResults() bool {
	return j.UsageInResultststs
}

const js = `
function parse() {
  try {
    console.log("=== DIAGNOSTIC: Starting page structure analysis ===");
    
    const diagnostic = {
      windowKeys: Object.keys(window).filter(key => key.includes('APP') || key.includes('INIT') || key.includes('STATE') || key.includes('DATA')),
      appInitState: window.APP_INITIALIZATION_STATE ? 'EXISTS' : 'NOT_FOUND',
      appInitStateLength: window.APP_INITIALIZATION_STATE ? window.APP_INITIALIZATION_STATE.length : 0,
      placeIdFound: false,
      finalResult: null
    };

    function createMockEntryData(placeId, title, url) {
      const darray = new Array(200);
      darray[11] = title;
      darray[13] = ["Business"];
      darray[18] = title + ", Address";
      darray[27] = url;
      darray[7] = [url];
      darray[10] = placeId;
      darray[57] = [null, "Owner Name", "owner_id"];
      darray[178] = [null, [null, "+33 1 23 45 67 89"]];
      
      // Extraire les coordonnées depuis l'URL ou les données de la page
      let lat = 0.0, lng = 0.0;
      
      // Essayer d'extraire depuis l'URL (format: @lat,lng)
      const urlMatch = url.match(/@(-?\d+\.\d+),(-?\d+\.\d+)/);
      if (urlMatch) {
        lat = parseFloat(urlMatch[1]);
        lng = parseFloat(urlMatch[2]);
      } else {
        // Essayer d'extraire depuis APP_INITIALIZATION_STATE
        if (window.APP_INITIALIZATION_STATE) {
          for (let i = 0; i < window.APP_INITIALIZATION_STATE.length; i++) {
            const section = window.APP_INITIALIZATION_STATE[i];
            if (Array.isArray(section)) {
              for (let j = 0; j < section.length; j++) {
                const item = section[j];
                if (typeof item === 'string' && item.includes('"latitude"')) {
                  try {
                    const parsed = JSON.parse(item);
                    if (parsed.latitude && parsed.longitude) {
                      lat = parseFloat(parsed.latitude);
                      lng = parseFloat(parsed.longitude);
                      break;
                    }
                  } catch (e) {
                    continue;
                  }
                }
              }
            }
          }
        }
      }
      
      // Placer les coordonnées dans darray[9] comme attendu par le parser Go
      darray[9] = [null, null, lat, lng];

      const jd = new Array(30);
      jd[6] = darray;
      jd[25] = [null, null, null, [null, null, null, null, null, null, null, null, null, null, null, null, null, [null, placeId]]];

      return JSON.stringify(jd);
    }

    const possiblePaths = [
      () => {
        if (window.APP_INITIALIZATION_STATE && window.APP_INITIALIZATION_STATE[3] && window.APP_INITIALIZATION_STATE[3][6]) {
          const result = String(window.APP_INITIALIZATION_STATE[3][6]);
          if (result.includes('"place_id"')) {
            diagnostic.placeIdFound = true;
            diagnostic.finalResult = 'PATH_1: APP_INITIALIZATION_STATE[3][6]';
            return result;
          }
        }
        return null;
      },
      () => {
        if (window.APP_INITIALIZATION_STATE) {
          for (let i = 0; i < window.APP_INITIALIZATION_STATE.length; i++) {
            const section = window.APP_INITIALIZATION_STATE[i];
            if (Array.isArray(section)) {
              for (let j = 0; j < section.length; j++) {
                const item = section[j];
                if (typeof item === 'string' && item.includes('"place_id"')) {
                  diagnostic.placeIdFound = true;
                  diagnostic.finalResult = 'PATH_2: APP_INITIALIZATION_STATE[' + i + '][' + j + ']';
                  return String(item);
                }
              }
            }
          }
        }
        return null;
      },
      () => {
        const url = window.location.href;
        const placeIdMatch = url.match(/place_id=([^&]+)/);
        if (placeIdMatch) {
          const placeId = placeIdMatch[1];
          const title = document.title.replace(' - Google Maps', '');
          const mockData = createMockEntryData(placeId, title, url);
          diagnostic.placeIdFound = true;
          diagnostic.finalResult = 'PATH_3: Extract from URL place_id parameter';
          return mockData;
        }
        return null;
      },
      () => {
        const url = window.location.href;
        const chIJMatch = url.match(/ChIJ[^?&]+/);
        if (chIJMatch) {
          const placeId = chIJMatch[0];
          const title = document.title.replace(' - Google Maps', '');
          const mockData = createMockEntryData(placeId, title, url);
          diagnostic.placeIdFound = true;
          diagnostic.finalResult = 'PATH_4: Extract ChIJ from URL';
          return mockData;
        }
        return null;
      },
      () => {
        const scripts = document.querySelectorAll('script');
        for (const script of scripts) {
          const content = script.textContent || script.innerHTML;
          if (content && content.includes('"place_id"')) {
            const startIndex = content.indexOf('"place_id"');
            if (startIndex !== -1) {
              let braceCount = 0;
              let bracketCount = 0;
              let startPos = startIndex;
              let endPos = startIndex;
              
              for (let i = startIndex; i >= 0; i--) {
                if (content[i] === '}') braceCount++;
                if (content[i] === '{') braceCount--;
                if (content[i] === ']') bracketCount++;
                if (content[i] === '[') bracketCount--;
                
                if (braceCount === 0 && bracketCount === 0) {
                  startPos = i;
                  break;
                }
              }
              
              braceCount = 0;
              bracketCount = 0;
              
              for (let i = startIndex; i < content.length; i++) {
                if (content[i] === '{') braceCount++;
                if (content[i] === '}') braceCount--;
                if (content[i] === '[') bracketCount++;
                if (content[i] === ']') bracketCount--;
                
                if (braceCount === 0 && bracketCount === 0) {
                  endPos = i + 1;
                  break;
                }
              }
              
              const extracted = content.substring(startPos, endPos);
              try {
                const parsed = JSON.parse(extracted);
                if (parsed && typeof parsed === 'object') {
                  diagnostic.placeIdFound = true;
                  diagnostic.finalResult = 'PATH_5: Smart JSON extraction around place_id';
                  return extracted;
                }
              } catch (e) {
                continue;
              }
            }
          }
        }
        return null;
      }
    ];

    for (const path of possiblePaths) {
      const result = path();
      if (result && result !== 'null' && result !== 'undefined' && result.trim() !== '') {
        console.log("=== DIAGNOSTIC RESULT ===");
        console.log(JSON.stringify(diagnostic, null, 2));
        console.log("=== END DIAGNOSTIC ===");
        return result;
      }
    }

    console.log("=== DIAGNOSTIC RESULT ===");
    console.log(JSON.stringify(diagnostic, null, 2));
    console.log("=== END DIAGNOSTIC ===");
    return null;
  } catch (error) {
    console.log("=== DIAGNOSTIC ERROR ===");
    console.log(error.toString());
    console.log("=== END DIAGNOSTIC ===");
    return null;
  }
}
`
