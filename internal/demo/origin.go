package demo

import "net/http"

func sameOrigin(r *http.Request, origin string) bool {
	origins := r.Header.Values("Origin")
	sites := r.Header.Values("Sec-Fetch-Site")
	return len(origins) == 1 && origins[0] == origin &&
		(len(sites) == 0 || len(sites) == 1 && sites[0] == "same-origin")
}

func foreign(r *http.Request, origin string) bool {
	origins := r.Header.Values("Origin")
	return len(origins) > 0 && (len(origins) > 1 || origins[0] != origin)
}
