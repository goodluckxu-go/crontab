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
	week
	month

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
		day:    "天",
		week:   "周",
		month:  "月",
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

	seconds  []int  // 可执行秒
	minutes  []int  // 可执行分
	hours    []int  // 可执行时
	days     []int  // 可执行天
	weeks    []int  // 可执行周
	months   []int  // 可执行月
	year     int    // 执行年
	timesLen [6]int // 可执行的数组长度
	timesIdx [6]int // 执行时间索引
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
		int(c.beforeTimePtr.Month()) == c.months[c.timesIdx[5]] &&
		c.beforeTimePtr.Day() == c.days[c.timesIdx[3]] &&
		c.beforeTimePtr.Hour() == c.hours[c.timesIdx[2]] &&
		c.beforeTimePtr.Minute() == c.minutes[c.timesIdx[1]] &&
		c.beforeTimePtr.Second() == c.seconds[c.timesIdx[0]] {
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
	for _, v := range c.months {
		switch v {
		case 1, 3, 5, 7, 8, 10, 12:
			isExists = true
		// 不验证，天可为31天
		case 2:
			// 2月最大可为29
			if c.days[c.timesLen[3]-1] <= 29 {
				isExists = true
			}
		default:
			// 其他月份都为30天
			if c.days[c.timesLen[3]-1] <= 30 {
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
	isNext := false
	// 秒
	c.timesIdx[0] = inSliceByRun(nowTime.Second(), c.seconds)
	if c.seconds[c.timesIdx[0]] < nowTime.Second() { // 秒进位
		isNext = true
	}
	// 分
	c.timesIdx[1] = inSliceByRun(nowTime.Minute(), c.minutes)
	if isNext { // 如果秒进位则分加1
		c.timesIdx[1] = (c.timesIdx[1] + 1) % c.timesLen[1]
		isNext = false
	}
	if c.minutes[c.timesIdx[1]] < nowTime.Minute() { // 分进位
		isNext = true
	}
	if c.minutes[c.timesIdx[1]] != nowTime.Minute() {
		c.timesIdx[0] = 0
	}
	// 时
	c.timesIdx[2] = inSliceByRun(nowTime.Hour(), c.hours)
	if isNext { // 如果分进位则时加1
		c.timesIdx[2] = (c.timesIdx[2] + 1) % c.timesLen[2]
		isNext = false
	}
	if c.hours[c.timesIdx[2]] < nowTime.Hour() { // 时进位
		isNext = true
	}
	if c.hours[c.timesIdx[2]] != nowTime.Hour() {
		c.timesIdx[0] = 0
		c.timesIdx[1] = 0
	}
	// 天
	c.timesIdx[3] = inSliceByRun(nowTime.Day(), c.days)
	if isNext { // 如果时进位则天加1
		c.timesIdx[3] = (c.timesIdx[3] + 1) % c.timesLen[3]
		isNext = false
	}
	if c.days[c.timesIdx[3]] < nowTime.Day() { // 天进位
		isNext = true
	}
	if c.days[c.timesIdx[3]] != nowTime.Day() {
		c.timesIdx[0] = 0
		c.timesIdx[1] = 0
		c.timesIdx[2] = 0
	}
	// 月
	c.timesIdx[5] = inSliceByRun(int(nowTime.Month()), c.months)
	if isNext { // 如果天进位则月加1
		c.timesIdx[5] = (c.timesIdx[5] + 1) % c.timesLen[5]
		isNext = false
	}
	if c.months[c.timesIdx[5]] < int(nowTime.Month()) { // 月进位
		isNext = true
	}
	if c.months[c.timesIdx[5]] != int(nowTime.Month()) {
		c.timesIdx[0] = 0
		c.timesIdx[1] = 0
		c.timesIdx[2] = 0
		c.timesIdx[3] = 0
	}
	// 年
	if isNext {
		c.year++
		c.timesIdx[0] = 0
		c.timesIdx[1] = 0
		c.timesIdx[2] = 0
		c.timesIdx[3] = 0
		c.timesIdx[5] = 0
	}
	if !c.isTrueNextTime() {
		c.nextTime()
	}
}

func (c *Cron) nextTime() {
	isNext := true
	for i := 0; i < 6; i++ {
		if i == int(week) {
			continue
		}
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
	// 不满足为天和周不匹配，循环天，将时分秒置零
	c.timesIdx[0] = 0
	c.timesIdx[1] = 0
	c.timesIdx[2] = 0
	for {
		isNext = true
		for i := 3; i < 6; i++ {
			if i == int(week) {
				continue
			}
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
	nextTime := time.Date(c.year, time.Month(c.months[c.timesIdx[5]]), c.days[c.timesIdx[3]],
		c.hours[c.timesIdx[2]], c.minutes[c.timesIdx[1]], c.seconds[c.timesIdx[0]], 0, time.Local)
	return nextTime
}

func (c *Cron) isTrueNextTime() bool {
	nextTime := time.Date(c.year, time.Month(c.months[c.timesIdx[5]]), c.days[c.timesIdx[3]],
		c.hours[c.timesIdx[2]], c.minutes[c.timesIdx[1]], c.seconds[c.timesIdx[0]], 0, time.Local)
	// 两年内时间为正确时间
	if maxYearLen != -1 && time.Now().Year()+1+maxYearLen <= nextTime.Year() {
		panic(fmt.Errorf(errorNullYear, time.Now().Year()+1+maxYearLen))
	}
	if nextTime.Year() != c.year || nextTime.Day() != c.days[c.timesIdx[3]] {
		return false
	}
	// 判断周是否在数组中
	if !isInArray(int(nextTime.Weekday()), c.weeks) {
		return false
	}
	return true
}

func (c *Cron) parseRules() {
	var err error
	ruleList := strings.Split(c.rules, " ")
	if len(ruleList) != 6 {
		panic("cron格式为* * * * * *六位，分别代表秒 分 时 天 周 月")
	}
	// 秒
	seconds := make([]bool, 60)
	if c.seconds, err = c.parseSingle(ruleList[0], second, seconds); err != nil {
		panic(err)
	}
	// 分
	minutes := make([]bool, 60)
	if c.minutes, err = c.parseSingle(ruleList[1], minute, minutes); err != nil {
		panic(err)
	}
	// 时
	hours := make([]bool, 24)
	if c.hours, err = c.parseSingle(ruleList[2], hour, hours); err != nil {
		panic(err)
	}
	// 天
	days := make([]bool, 32)
	if c.days, err = c.parseSingle(ruleList[3], day, days); err != nil {
		panic(err)
	}
	// 周
	weeks := make([]bool, 7)
	if c.weeks, err = c.parseSingle(ruleList[4], week, weeks); err != nil {
		panic(err)
	}
	// 月
	months := make([]bool, 13)
	if c.months, err = c.parseSingle(ruleList[5], month, months); err != nil {
		panic(err)
	}
	c.timesLen = [6]int{len(c.seconds), len(c.minutes), len(c.hours), len(c.days), len(c.weeks), len(c.months)}
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
