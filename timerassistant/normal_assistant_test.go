package timerassistant

import (
	"fmt"
	"testing"
	"time"
)

type TOwner struct {
	resumeCh chan func()
	o        TimerAssistant
}

func (o *TOwner) Execute(func()) {
	fmt.Println("Execute")
}

func (o TOwner) loop() {
	for {
		select {
		case fn := <-o.resumeCh:
			o.Execute(fn)
		}
	}
}

func TestNormalAssistant(t *testing.T) {
	tn := &TOwner{
		resumeCh: make(chan func(), 1),
		o:        NewTimerNormalAssistant(time.Second),
	}
	tn.o.AddCallBack(&CallInfo{
		Category: &Once{
			Hour: 22,
			Min:  35,
			Sec:  0,
		},
		Fn: func() {
			fmt.Println("fn inner")
		},
		ResumeCallCh: tn.resumeCh,
	})
	go tn.loop()
	go tn.o.Loop()
	select {}

}
