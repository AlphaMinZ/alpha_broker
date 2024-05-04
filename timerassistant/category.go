package timerassistant

import "time"

type CallCategory interface {
	ShouldCall() bool
	SetLastCallTime(int642 int64)
}

type Interval struct {
	Duration     time.Duration
	LastCallTime int64
}

func NewInterval(duration time.Duration, ShouldNowExecute bool) *Interval {
	interval := &Interval{
		Duration:     duration,
		LastCallTime: 0,
	}
	if !ShouldNowExecute {
		interval.LastCallTime = time.Now().UnixNano()
	}
	return interval
}

func (i *Interval) ShouldCall() bool {
	curTime := time.Now()
	curUnixNano := curTime.UnixNano()
	if curUnixNano-i.LastCallTime < int64(i.Duration) {
		return false
	}
	return true
}

func (i *Interval) SetLastCallTime(timeStamp int64) {
	i.LastCallTime = timeStamp
}

type Once struct {
	Hour         int
	Min          int
	Sec          int
	lastCallTime int64
}

func NewOnce(hour, min, sec int) *Once {
	return &Once{
		Hour: hour,
		Min:  min,
		Sec:  sec,
	}
}

func (o *Once) ShouldCall() bool {
	if o.lastCallTime != 0 {
		return false
	}
	now := time.Now()
	hour, min, sec := now.Local().Clock()
	if (hour*3600 + min*60 + sec) >= (o.Hour*3600 + o.Min*60 + o.Sec) {
		return true
	}
	return false
}
func (o *Once) SetLastCallTime(lastCallTime int64) {
	o.lastCallTime = lastCallTime
}

type Daily struct {
	Hour         int
	Min          int
	Sec          int
	lastCallTime int64
}

func NewDaily(hour, min, sec int) *Daily {
	return &Daily{
		Hour: hour,
		Min:  min,
		Sec:  sec,
	}
}

func (d *Daily) ShouldCall() bool {
	now := time.Now().Local()
	hour, min, sec := now.Clock()
	nowSec := hour*3660 + min*60 + sec

	if d.lastCallTime == 0 {
		if nowSec >= (d.Hour*3600 + d.Hour*60 + d.Sec) {
			return true
		}
	} else {
		if nowSec >= (d.Hour*3600+d.Hour*60+d.Sec) && int(nowSec-int(d.lastCallTime)) >= 86400 {
			return true
		}
	}

	return false
}

func (d *Daily) SetLastCallTime(lastCallTime int64) {
	d.lastCallTime = lastCallTime

}

type Weekly struct {
	WeekDay int
	Hour    int
	Min     int
	S       int
}
