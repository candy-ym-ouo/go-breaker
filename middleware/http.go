package middleware

import (
	"bytes"
	"net/http"
)

type responseCapture struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func newResponseCapture() *responseCapture {
	return &responseCapture{header: make(http.Header)}
}

func (r *responseCapture) Header() http.Header {
	return r.header
}

func (r *responseCapture) WriteHeader(status int) {
	if r.status != 0 {
		return
	}
	r.status = status
}

func (r *responseCapture) Write(data []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.body.Write(data)
}

func (r *responseCapture) Status() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

func (r *responseCapture) Successful() bool {
	return r.Status() < http.StatusInternalServerError
}

func copyResponse(target http.ResponseWriter, source *responseCapture) {
	for key, values := range source.Header() {
		for _, value := range values {
			target.Header().Add(key, value)
		}
	}
	target.WriteHeader(source.Status())
	_, _ = target.Write(source.body.Bytes())
}

func normalizedResource(request *http.Request) string {
	value := request.URL.Path
	for len(value) > 0 && value[0] == '/' {
		value = value[1:]
	}
	if value == "" {
		return "root"
	}
	return value
}
