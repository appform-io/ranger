package rangerdns

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSearchServiceInvalidQuery(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	client, err := NewRangerClient(RangerConfig{Endpoint: server.URL})
	client.Init()

	// Close the server when test finishes
	defer server.Close()

	assert.Nil(t, err)

	assert.Nil(t, client.SearchService("test"))
	assert.Nil(t, client.SearchService("test."))
	assert.Nil(t, client.SearchService("test_"))
	assert.Nil(t, client.SearchService("test_namespace."))
	assert.Nil(t, client.SearchService("test.namespace.test"))
}

func TestSearchServiceWithError(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	client, err := NewRangerClient(RangerConfig{Endpoint: server.URL})
	client.Init()
	assert.Nil(t, err)

	// Close the server when test finishes
	defer server.Close()

	mux.HandleFunc("/ranger/nodes/v1/namespace/test", func(rw http.ResponseWriter, req *http.Request) {
		// Test request parameters
		// Send response to be tested
		// rw.Write([]byte(`OK`))
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusBadGateway)
	})

	servicesNodes := client.SearchService("test.namespace.")
	assert.Nil(t, servicesNodes)
}

func TestSearchServiceWhenServiceFound(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	mux.HandleFunc("/ranger/nodes/v1/testNamespace/testService", func(rw http.ResponseWriter, req *http.Request) {
		// Test request parameters
		// Send response to be tested
		// rw.Write([]byte(`OK`))
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusOK)

		fmt.Fprint(rw, `{"status": "ok", "message": "ok", "data": [{
							"host": "host", "port": 1234, "portScheme": "HTTP"
					}]}`)
	})

	// Close the server when test finishes
	defer server.Close()

	// Use Client & URL from our local test server
	client, err := NewRangerClient(RangerConfig{Endpoint: server.URL})
	client.Init()
	assert.Nil(t, err)

	servicesNodes := client.SearchService("testService.testNamespace.")
	assert.Equal(t, len(servicesNodes), 1)
	assert.Equal(t, servicesNodes[0].Host, "host")
	assert.Equal(t, servicesNodes[0].Port, int32(1234))
	assert.Equal(t, servicesNodes[0].PortScheme, "HTTP")

}
