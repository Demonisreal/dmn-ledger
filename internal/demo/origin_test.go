package demo

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestPostNeedsTheOrigin(t *testing.T) {
	for _, c := range []struct {
		name    string
		headers []string
		want    int
	}{
		{"no origin", nil, http.StatusForbidden},
		{"a foreign origin", []string{"Origin", "https://evil.example"}, http.StatusForbidden},
		{"a neighbouring subdomain", []string{"Origin", "https://blog.example.org"}, http.StatusForbidden},
		{"a subdomain of the origin", []string{"Origin", "https://x.demo.example.org"}, http.StatusForbidden},
		{"the origin as a prefix", []string{"Origin", "https://demo.example.org.evil.example"}, http.StatusForbidden},
		{"another port", []string{"Origin", "https://demo.example.org:8443"}, http.StatusForbidden},
		{"another scheme", []string{"Origin", "http://demo.example.org"}, http.StatusForbidden},
		{"null", []string{"Origin", "null"}, http.StatusForbidden},
		{"upper case host", []string{"Origin", "https://DEMO.example.org"}, http.StatusForbidden},
		{"upper case scheme", []string{"Origin", "HTTPS://demo.example.org"}, http.StatusForbidden},
		{"a trailing slash", []string{"Origin", testOrigin + "/"}, http.StatusForbidden},
		{"twice", []string{"Origin", testOrigin, "Origin", testOrigin}, http.StatusForbidden},
		{"cross-site", []string{"Origin", testOrigin, "Sec-Fetch-Site", "cross-site"}, http.StatusForbidden},
		{"same-site", []string{"Origin", testOrigin, "Sec-Fetch-Site", "same-site"}, http.StatusForbidden},
		{"none", []string{"Origin", testOrigin, "Sec-Fetch-Site", "none"}, http.StatusForbidden},
		{"same-origin twice", []string{"Origin", testOrigin, "Sec-Fetch-Site", "same-origin", "Sec-Fetch-Site", "same-site"}, http.StatusForbidden},
		{"right", []string{"Origin", testOrigin}, http.StatusAccepted},
		{"right and same-origin", []string{"Origin", testOrigin, "Sec-Fetch-Site", "same-origin"}, http.StatusAccepted},
	} {
		show := newFake()
		w := hit(NewWeb(show, testOrigin, nil), "POST", "/act/kill/1", c.headers...)
		if w.Code != c.want {
			t.Errorf("%s: status %d, want %d", c.name, w.Code, c.want)
		}
		acts := len(show.calls())
		if c.want == http.StatusForbidden {
			var why refusal
			if err := json.Unmarshal(w.Body.Bytes(), &why); err != nil || why.Error != "origin" {
				t.Errorf("%s: body %q, want the error origin", c.name, w.Body)
			}
			if acts != 0 {
				t.Errorf("%s: Act was called %d times", c.name, acts)
			}
		} else if acts != 1 {
			t.Errorf("%s: Act was called %d times, want once", c.name, acts)
		}
	}
}

func TestPreflightFromAForeignSiteIsAWrongMethod(t *testing.T) {
	show := newFake()
	web := NewWeb(show, testOrigin, nil)
	for _, c := range []struct{ target, method, allow string }{
		{"/act/kill/leader", "POST", "POST"},
		{"/events", "GET", "GET, HEAD"},
		{"/proof", "GET", "GET, HEAD"},
	} {
		w := hit(web, "OPTIONS", c.target, "Origin", "https://evil.example", "Access-Control-Request-Method", c.method,
			"Sec-Fetch-Mode", "cors", "Sec-Fetch-Site", "cross-site")
		if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != c.allow {
			t.Errorf("OPTIONS %s: status %d, Allow %q; want 405 with Allow %s", c.target, w.Code, w.Header().Get("Allow"), c.allow)
		}
		for key := range w.Header() {
			if strings.HasPrefix(key, "Access-Control-") {
				t.Errorf("OPTIONS %s: CORS header %s", c.target, key)
			}
		}
	}
	if acts := show.calls(); len(acts) != 0 {
		t.Errorf("Act was called: %v", acts)
	}
	if n := len(web.hub.viewers); n != 0 {
		t.Errorf("%d viewers joined through a preflight", n)
	}
}

func TestReadsRefuseAForeignOrigin(t *testing.T) {
	web := NewWeb(newFake(), testOrigin, nil)
	for _, target := range []string{"/proof", "/events"} {
		for _, origin := range []string{"https://evil.example", "null", "https://demo.example.org:8443"} {
			if w := hit(web, "GET", target, "Origin", origin); w.Code != http.StatusForbidden {
				t.Errorf("GET %s from %s: status %d, want 403", target, origin, w.Code)
			}
		}
	}
	for _, target := range []string{"/proof", "/events"} {
		for _, site := range []string{"cross-site", "same-site"} {
			if w := hit(web, "GET", target, "Sec-Fetch-Site", site); w.Code != http.StatusForbidden {
				t.Errorf("GET %s without an origin, Sec-Fetch-Site %s: status %d, want 403", target, site, w.Code)
			}
		}
	}
	for _, headers := range [][]string{nil, {"Origin", testOrigin}, {"Sec-Fetch-Site", "same-origin"}, {"Sec-Fetch-Site", "none"}} {
		if w := hit(web, "GET", "/proof", headers...); w.Code != http.StatusOK {
			t.Errorf("GET /proof with %q: status %d, want 200", headers, w.Code)
		}
	}
}
