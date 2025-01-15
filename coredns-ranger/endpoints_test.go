package rangerdns

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSetServices(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)

	// Close the server when test finishes
	defer server.Close()

	// Use Client & URL from our local test server
	client, err := NewRangerClient(RangerConfig{Endpoint: server.URL})
	assert.Nil(t, err)

	rangerServices := newRangerServices(&client)

	serviceInfoResponse := &RangerServiceInfoResponse{
		Status: "200",
		Services: []RangerServiceInfo{{
			RangerService: RangerService{
				ServiceName: "test",
				Namespace:   "testN",
			},
			ServiceNodes: []ServiceNode{
				{
					Host:       "host",
					Port:       2344,
					PortScheme: "HTTP",
				},
			},
		}},
	}

	rangerServices.setServices(serviceInfoResponse)

	serviceInfoDB := rangerServices.getServices()
	assert.NotNil(t, serviceInfoDB)
	assert.Equal(t, 1, len(serviceInfoDB.Services))
}

func TestSearchService(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)

	// Close the server when test finishes
	defer server.Close()

	// Use Client & URL from our local test server
	client, err := NewRangerClient(RangerConfig{Endpoint: server.URL})
	assert.Nil(t, err)

	rangerServices := newRangerServices(&client)

	serviceInfoResponse := &RangerServiceInfoResponse{
		Status: "200",
		Services: []RangerServiceInfo{{
			RangerService: RangerService{
				ServiceName: "test",
				Namespace:   "testN",
			},
			ServiceNodes: []ServiceNode{
				{
					Host:       "host",
					Port:       2344,
					PortScheme: "HTTP",
				},
			},
		}},
	}

	rangerServices.setServices(serviceInfoResponse)

	assert.Nil(t, rangerServices.searchService("test1.testN."))
	assert.Nil(t, rangerServices.searchService("test.testN"))

	rangerServiceInfo := rangerServices.searchService("test.testN.")
	assert.NotNil(t, rangerServiceInfo)
	assert.Equal(t, "test", rangerServiceInfo.RangerService.ServiceName)
	assert.Equal(t, "testN", rangerServiceInfo.RangerService.Namespace)
	assert.Equal(t, 1, len(rangerServiceInfo.ServiceNodes))
	assert.Equal(t, "host", rangerServiceInfo.ServiceNodes[0].Host)
	assert.Equal(t, int32(2344), rangerServiceInfo.ServiceNodes[0].Port)
	assert.Equal(t, "HTTP", rangerServiceInfo.ServiceNodes[0].PortScheme)

}
