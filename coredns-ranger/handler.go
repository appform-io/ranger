package rangerdns

import (
	"context"
	"fmt"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
)

type RangerHandler struct {
	RangerServices *RangerServices
	Next           plugin.Handler
}

func NewRangerHandler(rangerClient IRangerClient) *RangerHandler {
	return &RangerHandler{RangerServices: newRangerServices(rangerClient)}

}
func (e *RangerHandler) Name() string { return "ranger" }

func (e *RangerHandler) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {

	a := new(dns.Msg)
	if e.RangerServices.getServices() == nil {
		return dns.RcodeServerFailure, fmt.Errorf("no services found with ranger DNS")
	}
	if len(r.Question) == 0 {
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}
	serviceInfo := e.RangerServices.searchService(r.Question[0].Name)
	if serviceInfo != nil {

		a.SetReply(r)
		a.Authoritative = true

		state := request.Request{W: w, Req: r}

		srv := make([]dns.RR, len(serviceInfo.ServiceNodes))

		for i, h := range serviceInfo.ServiceNodes {
			srv[i] = &dns.SRV{Hdr: dns.RR_Header{Name: state.QName(), Rrtype: dns.TypeSRV, Class: state.QClass(), Ttl: 30},
				Port:     uint16(h.Port),
				Target:   h.Host + ".",
				Weight:   1,
				Priority: 1,
			}
		}

		if state.QType() == dns.TypeSRV {
			a.Answer = srv
		} else {
			a.Extra = srv
		}
	}

	if len(a.Answer) > 0 || len(a.Extra) > 0 {
		if e.Next != nil {
			return plugin.NextOrFailure(e.Name(), e.Next, ctx, &CombiningResponseWriter{w, a}, r)
		}
		w.WriteMsg(a)
		return dns.RcodeSuccess, nil

	}

	// Call next plugin (if any).
	return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
}

// Name implements the Handler interface.
type CombiningResponseWriter struct {
	dns.ResponseWriter
	answer *dns.Msg
}

func (w *CombiningResponseWriter) WriteMsg(res *dns.Msg) error {

	res.Answer = append(res.Answer, w.answer.Answer...)
	res.Extra = append(res.Extra, w.answer.Extra...)
	return w.ResponseWriter.WriteMsg(res)

}
