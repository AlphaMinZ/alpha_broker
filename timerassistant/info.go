package timerassistant

import "time"

type CallInfo struct {
	Category     CallCategory
	Fn           func()
	ResumeCallCh chan func()
}

func (cb *CallInfo) NotifyOwner() {
	if cb.Category.ShouldCall() {
		cb.ResumeCallCh <- cb.Fn
		cb.Category.SetLastCallTime(time.Now().UnixNano())
	}
}
