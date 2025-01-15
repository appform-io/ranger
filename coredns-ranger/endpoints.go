package rangerdns

import (
	"sync"
	"time"
)

type RangerServices struct {
	appsMutex              *sync.RWMutex
	ServiceInfoDB          *RangerServiceInfoResponse
	RangerClient           IRangerClient
	ServiceByNameNamespace map[string]RangerServiceInfo
}

func (dr *RangerServices) setServices(serviceInfoDB *RangerServiceInfoResponse) {
	var servicesByNameNamespace = make(map[string]RangerServiceInfo)
	if serviceInfoDB != nil {
		for _, serviceInfo := range serviceInfoDB.Services {
			servicesByNameNamespace[serviceInfo.RangerService.ServiceName+"."+serviceInfo.RangerService.Namespace+"."] = serviceInfo
		}
	}
	dr.appsMutex.Lock()
	dr.ServiceInfoDB = serviceInfoDB
	dr.ServiceByNameNamespace = servicesByNameNamespace
	dr.appsMutex.Unlock()
}

func (dr *RangerServices) getServices() *RangerServiceInfoResponse {
	dr.appsMutex.RLock()
	defer dr.appsMutex.RUnlock()
	if dr.ServiceInfoDB == nil {
		return nil
	}
	return dr.ServiceInfoDB
}

func (dr *RangerServices) searchService(question string) *RangerServiceInfo {
	dr.appsMutex.RLock()
	defer dr.appsMutex.RUnlock()
	if dr.ServiceByNameNamespace == nil {
		return nil
	}
	if serviceInfo, ok := dr.ServiceByNameNamespace[question]; ok {
		return &serviceInfo
	}
	return nil
}

func newRangerServices(client IRangerClient) *RangerServices {
	endpoints := RangerServices{RangerClient: client, appsMutex: &sync.RWMutex{}}
	ticker := time.NewTicker(10 * time.Second)
	done := make(chan bool)
	go func() {
		var syncServices = func() {
			RangerQueryTotal.Inc()
			services, err := endpoints.RangerClient.FetchServices()
			if err != nil {
				RangerQueryFailure.Inc()
				log.Errorf("Error fetching services")
				return
			}

			endpoints.setServices(services)
		}
		syncServices()
		for {
			select {
			case <-done:
				return
			case _ = <-ticker.C:
				log.Debug("Refreshing services data from ranger")
				syncServices()
			}
		}
	}()
	return &endpoints
}
