package rangerdns

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"net/url"
	"strings"

	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	FetchServiceNodesTimeout = time.Duration(5) * time.Second
)

type IRangerClient interface {
	SearchService(question string) []ServiceNode
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
	// Nothing to be done here
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

func (c *RangerClient) SearchService(question string) []ServiceNode {
	RangerQueryTotal.Inc()
	questionSplit := strings.Split(strings.TrimSuffix(question, "."), ".")

	if len(questionSplit) != 2 {
		return nil
	}
	nodes, err := c.FetchServiceNodes(questionSplit[0], questionSplit[1])
	if err != nil {
		RangerQueryFailure.Inc()
		log.Errorf("Error fetching nodes")
		return nil
	}
	return nodes.ServiceNodes
}

func (c *RangerClient) FetchServiceNodes(serviceName string, namespace string) (*RangerServiceNodeResponse, error) {
	services := &RangerServiceNodeResponse{}
	err := c.executeRequest("/ranger/nodes/v1/"+namespace+"/"+serviceName, FetchServiceNodesTimeout, services)
	return services, err

}

func setHeaders(req *http.Request) {
	req.Header.Set("Accept", "application/json")
}
