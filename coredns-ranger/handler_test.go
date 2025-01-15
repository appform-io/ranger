package rangerdns

import (
	"context"
	"encoding/json"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
	"time"
)

type MockRangerClient struct{}

func (*MockRangerClient) FetchServices() (*RangerServiceInfoResponse, error) {
	services := &RangerServiceInfoResponse{}
	json.Unmarshal([]byte(`{"status": "ok", "message": "ok", "data": [{
					"service": {"namespace": "testNamespace", "serviceName": "testService"},
                    "nodes": [{
							"host": "host", "port": 1234, "portScheme": "HTTP"
					}]
			}]}`), services)
	return services, nil
}

type MockResponseWriter struct {
	dns.ResponseWriter
	validator   func(ms *dns.Msg)
	callCounter int
}

func (w *MockResponseWriter) WriteMsg(res *dns.Msg) error {
	w.callCounter += 1
	w.validator(res)
	return nil

}

func TestServeDNSNotReady(t *testing.T) {
	handler := RangerHandler{RangerServices: newRangerServices(&MockRangerClient{})}
	writer := &MockResponseWriter{
		validator: func(res *dns.Msg) {
			assert.Equal(t, 1, len(res.Answer), "One Answer should be returned")
			assert.Equal(t, 0, len(res.Extra), "Additional should be empty")
			assert.Equal(t, "host.", res.Answer[0].(*dns.SRV).Target, "'host' should be the target")
			assert.Equal(t, uint16(1234), res.Answer[0].(*dns.SRV).Port, "1234 should be the port")
		}}
	code, err := handler.ServeDNS(context.Background(), writer, &dns.Msg{Question: []dns.Question{dns.Question{Name: "testService.testNamespace.", Qtype: dns.TypeSRV, Qclass: dns.ClassINET}}})
	assert.NotNil(t, err, "Error should be returned")
	assert.Equal(t, dns.RcodeServerFailure, code, "Failure error code should be returned")
	assert.Equal(t, 0, writer.callCounter, "Message would not be written")

}

func TestServeDNSWhenServiceFound(t *testing.T) {
	handler := RangerHandler{RangerServices: newRangerServices(&MockRangerClient{})}
	for !handler.Ready() {
		time.Sleep(1)
	}
	writer := &MockResponseWriter{
		validator: func(res *dns.Msg) {
			assert.Equal(t, 1, len(res.Answer), "One Answer should be returned")
			assert.Equal(t, 0, len(res.Extra), "Additional should be empty")
			assert.Equal(t, "host.", res.Answer[0].(*dns.SRV).Target, "'host' should be the target")
			assert.Equal(t, uint16(1234), res.Answer[0].(*dns.SRV).Port, "1234 should be the port")
		}}
	code, err := handler.ServeDNS(context.Background(), writer, &dns.Msg{Question: []dns.Question{dns.Question{Name: "testService.testNamespace.", Qtype: dns.TypeSRV, Qclass: dns.ClassINET}}})
	assert.Nil(t, err, "Error should be nil")
	assert.Equal(t, dns.RcodeSuccess, code, "SuccessCode should be returned")
	assert.Equal(t, 1, writer.callCounter, "Message should be written")

}

func TestServeDNSQueryTypeAWhenServiceFound(t *testing.T) {
	handler := RangerHandler{RangerServices: newRangerServices(&MockRangerClient{})}
	for !handler.Ready() {
		time.Sleep(1)
	}
	writer := &MockResponseWriter{
		validator: func(res *dns.Msg) {
			assert.Equal(t, 0, len(res.Answer))
			assert.Equal(t, 1, len(res.Extra))
			assert.Equal(t, "host.", res.Extra[0].(*dns.SRV).Target)
			assert.Equal(t, uint16(1234), res.Extra[0].(*dns.SRV).Port)
		}}
	code, err := handler.ServeDNS(context.Background(), writer, &dns.Msg{Question: []dns.Question{dns.Question{Name: "testService.testNamespace.", Qtype: dns.TypeA, Qclass: dns.ClassINET}}})
	assert.Nil(t, err, "Error should be nil")
	assert.Equal(t, dns.RcodeSuccess, code, "SuccessCode should be returned")
	assert.Equal(t, 1, writer.callCounter, "Message should be written")

}

func TestServeDNSNoMatchingService(t *testing.T) {
	handler := RangerHandler{RangerServices: newRangerServices(&MockRangerClient{})}
	for !handler.Ready() {
		time.Sleep(1)
	}
	writer := &MockResponseWriter{
		validator: func(res *dns.Msg) {
			// Should not come here
			t.Fail()
		}}
	code, err := handler.ServeDNS(context.Background(), writer, &dns.Msg{Question: []dns.Question{dns.Question{Name: "testService.testNamespace1.", Qtype: dns.TypeA, Qclass: dns.ClassINET}}})
	assert.NotNil(t, err, "Error should be returned")
	assert.Equal(t, dns.RcodeServerFailure, code, "Failure error code should be returned")
	assert.Equal(t, 0, writer.callCounter, "Message should not be Written")

}

func TestServeDNSForwarding(t *testing.T) {
	handler := RangerHandler{RangerServices: newRangerServices(&MockRangerClient{})}
	mockNextHandler := MockHandler{}
	handler.Next = &mockNextHandler
	for !handler.Ready() {
		time.Sleep(1)
	}
	writer := &MockResponseWriter{
		validator: func(res *dns.Msg) {
			assert.Equal(t, 0, len(res.Answer))
			assert.Equal(t, 1, len(res.Extra))
			assert.Equal(t, "host.", res.Extra[0].(*dns.SRV).Target)
			assert.Equal(t, uint16(1234), res.Extra[0].(*dns.SRV).Port)
		}}
	handler.ServeDNS(context.Background(), writer, &dns.Msg{Question: []dns.Question{dns.Question{Name: "testService.testNamespace.", Qtype: dns.TypeA, Qclass: dns.ClassINET}}})
	assert.Equal(t, 1, mockNextHandler.callCounter, "Next handler should be called")
}

type MockHandler struct {
	callCounter int
}

func (h *MockHandler) ServeDNS(c context.Context, rw dns.ResponseWriter, r *dns.Msg) (int, error) {
	h.callCounter += 1
	r.Answer = append(r.Answer, &dns.A{
		A: net.IPv4(1, 1, 1, 1),
	})
	return 0, nil
}
func (h *MockHandler) Name() string {
	return "MOCK"
}
