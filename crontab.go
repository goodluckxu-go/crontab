package crontab

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type cType int

const (
	second = cType(iota)
	minute
	hour
	day
	month
	week

	errorStr        = "规则错误"
	errorNull       = "不存在可执行时间"
	errorNullYear   = "%v年以前不存在可执行时间"
	errorStrRange   = "%v的规则错误，范围在%v-%v之间"
	errorStrCompare = "%v的规则错误，%v-%v开始比结束小"
)

var (
	maxYearLen = 1 // 最长年限多久，今年年份+参数为最大年，0表示限制为今年，-1为不限制(如果周月日不一致可能导致查询几十年以后的)
)

func (c cType) text() string {
	m := map[cType]string{
		second: "秒",
		minute: "分",
		hour:   "时",
		day:    "日",
		month:  "月",
		week:   "周",
	}
	return m[c]
}

type Context struct {
	context.Context
	c         *Cron
	beginTime time.Time
}

// Stop 结束定时器
func (c *Context) Stop() {
	c.c.die <- struct{}{}
}

// BeginTime 定时器开始时间
func (c *Context) BeginTime() time.Time {
	return c.beginTime
}

// NewCron 创建定时器
func NewCron(rules string) *Cron {
	return &Cron{rules: rules}
}

// SetMaxYearLen 设置最长年限多久，今年年份+参数为最大年，0表示限制为今年，-1为不限制(如果周月日不一致可能导致查询几十年以后的)
func SetMaxYearLen(l int) {
	maxYearLen = l
}

type Cron struct {
	rules         string             // 任务规则
	task          func(res *Context) // 任务
	beforeTimePtr *time.Time         // 上次执行时间
	die           chan struct{}      // 关闭定时器
	t             *time.Timer        // 定时器
	year          int                // 执行年
	times         [6][]int           // 可执行时间
	timesLen      [6]int             // 可执行的数组长度
	timesIdx      [6]int             // 执行时间索引
}

// SetFun 设置任务方法
func (c *Cron) SetFun(task func(ctx *Context)) *Cron {
	c.task = task
	return c
}

// SetBeforeTime 设置上一次执行时间，没有则不设置
func (c *Cron) SetBeforeTime(beforeTime time.Time) *Cron {
	c.beforeTimePtr = &beforeTime
	return c
}

func (c *Cron) Run() {
	// 解析规则
	c.parseRules()
	// 验证月份和日期是否可组成正确的时间(月份闰月，大月小月问题)
	c.validMonthDay()
	// 初始化第一次可执行时间
	c.init()
	// 判断当前时间是否时已经执行
	if c.beforeTimePtr != nil && c.beforeTimePtr.Year() == c.year &&
		int(c.beforeTimePtr.Month()) == c.times[month][c.timesIdx[month]] &&
		c.beforeTimePtr.Day() == c.times[day][c.timesIdx[day]] &&
		c.beforeTimePtr.Hour() == c.times[hour][c.timesIdx[hour]] &&
		c.beforeTimePtr.Minute() == c.times[minute][c.timesIdx[minute]] &&
		c.beforeTimePtr.Second() == c.times[second][c.timesIdx[second]] {
		// 已经执行则跳到下一个时间
		c.nextTime()
	}
	c.t = time.AfterFunc(time.Duration(c.getNextTime().Unix()-time.Now().Unix())*time.Second, func() {
		c.run()
	})
	defer c.t.Stop()
	// 阻塞
	select {
	case <-c.die:
		c.t.Stop()
	}
}

func (c *Cron) run() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				c.die <- struct{}{}
			}
		}()
		c.task(&Context{
			c:         c,
			beginTime: c.getNextTime(),
		})
	}()
	// 下一次执行时间
	c.nextTime()
	c.t = time.AfterFunc(time.Duration(c.getNextTime().Unix()-time.Now().Unix())*time.Second, func() {
		c.run()
	})
}

func (c *Cron) validMonthDay() {
	isExists := false
	for _, v := range c.times[month] {
		switch v {
		case 1, 3, 5, 7, 8, 10, 12:
			isExists = true
		// 不验证，天可为31天
		case 2:
			// 2月最大可为29
			if c.times[day][0] <= 29 {
				isExists = true
			}
		default:
			// 其他月份都为30天
			if c.times[day][0] <= 30 {
				isExists = true
			}
		}
	}
	if !isExists {
		panic(errorNull)
	}
}

func (c *Cron) init() {
	c.die = make(chan struct{})
	nowTime := time.Now()
	c.year = nowTime.Year()
	nowTimes := []int{nowTime.Second(), nowTime.Minute(), nowTime.Hour(), nowTime.Day(), int(nowTime.Month()),
		nowTime.Year()}
	// 秒到月可进制
	isNext := false
	for i := second; i <= month; i++ {
		c.timesIdx[i] = inSliceByRun(nowTimes[i], c.times[i])
		if isNext {
			c.timesIdx[i] = (c.timesIdx[i] + 1) % c.timesLen[i]
			isNext = false
		}
		// 小于证明数字经过最大，高位进1
		if c.times[i][c.timesIdx[i]] < nowTimes[i] {
			isNext = true
		}
		// 不等于证明变化，变化后低位至0
		if c.times[i][c.timesIdx[i]] != nowTimes[i] {
			for j := second; j < i; j++ {
				c.timesIdx[j] = 0
			}
		}
	}
	if isNext {
		c.year++
		for j := second; j < month; j++ {
			c.timesIdx[j] = 0
		}
	}
	if !c.isTrueNextTime() {
		c.nextTime()
	}
}

func (c *Cron) nextTime() {
	isNext := true
	for i := second; i <= month; i++ {
		if isNext {
			c.timesIdx[i] = (c.timesIdx[i] + 1) % c.timesLen[i]
		}
		if c.timesIdx[i] != 0 {
			isNext = false
			break
		}
	}
	if isNext {
		c.year++
	}
	if c.isTrueNextTime() {
		return
	}
	// 不满足为日月周不匹配，循环日，将时分秒置零
	c.timesIdx[second] = 0
	c.timesIdx[minute] = 0
	c.timesIdx[hour] = 0
	for {
		isNext = true
		for i := day; i <= month; i++ {
			if isNext {
				c.timesIdx[i] = (c.timesIdx[i] + 1) % c.timesLen[i]
			}
			if c.timesIdx[i] != 0 {
				isNext = false
				break
			}
		}
		if isNext {
			c.year++
		}
		if c.isTrueNextTime() {
			return
		}
	}
}

func (c *Cron) getNextTime() time.Time {
	nextTime := time.Date(c.year, time.Month(c.times[month][c.timesIdx[month]]), c.times[day][c.timesIdx[day]],
		c.times[hour][c.timesIdx[hour]], c.times[minute][c.timesIdx[minute]], c.times[second][c.timesIdx[second]], 0, time.Local)
	return nextTime
}

func (c *Cron) isTrueNextTime() bool {
	nextTime := c.getNextTime()
	// 两年内时间为正确时间
	if maxYearLen != -1 && time.Now().Year()+1+maxYearLen <= nextTime.Year() {
		panic(fmt.Errorf(errorNullYear, time.Now().Year()+1+maxYearLen))
	}
	if nextTime.Year() != c.year || nextTime.Day() != c.times[day][c.timesIdx[day]] {
		return false
	}
	// 判断周是否在数组中
	if !isInArray(int(nextTime.Weekday()), c.times[week]) {
		return false
	}
	return true
}

func (c *Cron) parseRules() {
	var err error
	ruleList := strings.Split(c.rules, " ")
	if len(ruleList) != 6 {
		panic("cron格式为* * * * * *六位，分别代表秒 分 时 日 月 周")
	}
	// 秒
	seconds := make([]bool, 60)
	if c.times[second], err = c.parseSingle(ruleList[second], second, seconds); err != nil {
		panic(err)
	}
	// 分
	minutes := make([]bool, 60)
	if c.times[minute], err = c.parseSingle(ruleList[minute], minute, minutes); err != nil {
		panic(err)
	}
	// 时
	hours := make([]bool, 24)
	if c.times[hour], err = c.parseSingle(ruleList[hour], hour, hours); err != nil {
		panic(err)
	}
	// 日
	days := make([]bool, 32)
	if c.times[day], err = c.parseSingle(ruleList[day], day, days); err != nil {
		panic(err)
	}
	// 月
	months := make([]bool, 13)
	if c.times[month], err = c.parseSingle(ruleList[month], month, months); err != nil {
		panic(err)
	}
	// 周
	weeks := make([]bool, 7)
	if c.times[week], err = c.parseSingle(ruleList[week], week, weeks); err != nil {
		panic(err)
	}
	for k, v := range c.times {
		c.timesLen[k] = len(v)
	}
}

func (c *Cron) parseSingle(rule string, ct cType, runList []bool) (rs []int, err error) {
	for _, r := range strings.Split(rule, ",") {
		if strings.Contains(r, "/") {
			// 解析每多少时间times
			rList := strings.Split(r, "/")
			if len(rList) != 2 {
				err = errors.New(errorStr)
				return
			}
			var start, end, interval int
			if interval, err = parseInt(rList[1]); err != nil {
				return
			}
			if err = c.validNum(interval, ct, true); err != nil {
				return
			}
			start, end = c.getMaxBetween(ct)
			if rList[0] != "*" {
				charList := strings.Split(rList[0], "-")
				if len(charList) > 2 {
					err = errors.New(errorStr)
					return
				}
				if start, err = parseInt(charList[0]); err != nil {
					return
				}
				if err = c.validNum(start, ct, false); err != nil {
					return
				}
				if len(charList) == 2 {
					if end, err = parseInt(charList[1]); err != nil {
						return
					}
					if err = c.validNum(end, ct, false); err != nil {
						return
					}
				}
			}
			if start > end {
				err = fmt.Errorf(errorStrCompare, ct.text(), start, end)
				return
			}
			c.getTimes(start, end, interval, runList)
		} else if strings.Contains(r, "-") {
			// 解释时间段between
			rList := strings.Split(r, "-")
			if len(rList) != 2 {
				err = errors.New(errorStr)
				return
			}
			var start, end, interval int
			interval = 1
			if start, err = parseInt(rList[0]); err != nil {
				return
			}
			if err = c.validNum(start, ct, false); err != nil {
				return
			}
			if end, err = parseInt(rList[1]); err != nil {
				return
			}
			if err = c.validNum(end, ct, false); err != nil {
				return
			}
			if start > end {
				err = fmt.Errorf(errorStrCompare, ct.text(), start, end)
				return
			}
			c.getTimes(start, end, interval, runList)
		} else {
			var start, end, interval int
			start, end = c.getMaxBetween(ct)
			if r == "*" {
				interval = 1
				c.getTimes(start, end, interval, runList)
				continue
			}
			var charNum int
			if charNum, err = parseInt(r); err != nil {
				return
			}
			if err = c.validNum(charNum, ct, false); err != nil {
				return
			}
			runList[charNum] = true
		}
	}
	for k, v := range runList {
		if v {
			rs = append(rs, k)
		}
	}
	return
}

func (c *Cron) getTimes(start, end, interval int, runList []bool) {
	for i := start; i <= end; i += interval {
		runList[i] = true
	}
}

func (c *Cron) validNum(n int, ct cType, isInterval bool) error {
	min, max := c.getMaxBetween(ct)
	if isInterval && (n < 1 || n > max+1) {
		return fmt.Errorf(errorStrRange, ct.text()+"间隔", 1, max+1)
	}
	if !isInterval && (n < min || n > max) {
		return fmt.Errorf(errorStrRange, ct.text(), min, max)
	}
	return nil
}

func (c *Cron) getMaxBetween(ct cType) (start, end int) {
	switch ct {
	case second, minute:
		start = 0
		end = 59
	case hour:
		start = 0
		end = 23
	case day:
		start = 1
		end = 31
	case week:
		start = 0
		end = 6
	case month:
		start = 1
		end = 12
	}
	return
}
