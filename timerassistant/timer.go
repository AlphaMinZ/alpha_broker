package timerassistant

type TimerAssistant interface {
	AddCallBack(*CallInfo)
	DelCallBack(*CallInfo)
	Loop()
	AssertOwner(owner Owner)
}
