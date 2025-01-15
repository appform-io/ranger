package rangerdns

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"net/url"

	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	FetchServicesTimeout = time.Duration(5) * time.Second
)

type IRangerClient interface {
	FetchServices() (*RangerServiceInfoResponse, error)
}
type RangerClient struct {
	EndpointMutex sync.RWMutex
	Endpoint      EndpointInfo
	client        *http.Client
}

func NewRangerClient(config RangerConfig) (RangerClient, error) {
	endpoint := config.Endpoint

	parsedUrl, err := url.Parse(endpoint)
	if err != nil {
		return RangerClient{}, err
	}

	host, port, splitErr := net.SplitHostPort(parsedUrl.Host)
	if splitErr != nil {
		return RangerClient{}, err
	}

	iPort, _ := strconv.Atoi(port)

	tr := &http.Transport{MaxIdleConnsPerHost: 10, TLSClientConfig: &tls.Config{}}
	httpClient := &http.Client{
		Timeout:   0 * time.Second,
		Transport: tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return RangerClient{Endpoint: EndpointInfo{
		Endpoint: endpoint,
		Host:     host,
		Port:     int32(iPort),
	}, client: httpClient}, nil
}

func (c *RangerClient) Init() {
	// Nothing to be done here?
}

func (c *RangerClient) executeRequest(path string, timeout time.Duration, obj any) error {
	host := c.Endpoint.Endpoint
	endpoint := host + path
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return err
	}

	setHeaders(req)
	resp, err := c.client.Do(req)
	if err != nil {
		RangerApiRequests.WithLabelValues("err", "GET", host).Inc()
		return err
	}

	RangerApiRequests.WithLabelValues(strconv.Itoa(resp.StatusCode), "GET", host).Inc()
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)

	err = decoder.Decode(obj)
	if err != nil {
		return err
	}
	return nil
}

func (c *RangerClient) FetchServices() (*RangerServiceInfoResponse, error) {
	services := &RangerServiceInfoResponse{}
	err := c.executeRequest("/ranger/services/nodes/v1", FetchServicesTimeout, services)
	return services, err

}

func setHeaders(req *http.Request) {
	req.Header.Set("Accept", "application/json")
}
