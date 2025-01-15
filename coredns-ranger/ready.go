package rangerdns

func (e *RangerHandler) Ready() bool {
	return e.RangerServices.getServices() != nil
}
