package rangerdns

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServiceFetch(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	mux.HandleFunc("/ranger/services/nodes/v1", func(rw http.ResponseWriter, req *http.Request) {
		// Test request parameters
		// Send response to be tested
		// rw.Write([]byte(`OK`))
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusOK)

		fmt.Fprint(rw, `{"status": "ok", "message": "ok", "data": [{
					"service": {"namespace": "testNamespace", "serviceName": "testService"},
                    "nodes": [{
							"host": "host", "port": 1234, "portScheme": "HTTP"
					}]
			}]}`)
	})
	// Close the server when test finishes
	defer server.Close()

	// Use Client & URL from our local test server
	client, err := NewRangerClient(RangerConfig{Endpoint: server.URL})
	client.Init()

	services, err := client.FetchServices()
	assert.Nil(t, err)
	assert.Equal(t, len(services.Services), 1)
	assert.Equal(t, services.Services[0].RangerService.ServiceName, "testService")
	assert.Equal(t, services.Services[0].RangerService.Namespace, "testNamespace")
	assert.Equal(t, len(services.Services[0].ServiceNodes), 1)
	assert.Equal(t, services.Services[0].ServiceNodes[0].Host, "host")
	assert.Equal(t, services.Services[0].ServiceNodes[0].Port, int32(1234))
	assert.Equal(t, services.Services[0].ServiceNodes[0].PortScheme, "HTTP")
}
