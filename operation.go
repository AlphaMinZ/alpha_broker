package alphaBroker

type CallBack func()

type Operation struct {
	IsAsynchronous bool
	Cb             CallBack
	Ret            chan interface{}
}
