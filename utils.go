package crontab

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
)

func parseInt(s string) (int, error) {
	i, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("%v必须是整数", s)
	}
	return i, nil
}

func inSliceByRun(v int, slice []int) (idx int) {
	n := len(slice)
	idx = 0
	for i := 0; i < n; i++ {
		if slice[i] == v {
			idx = i
			return
		}
		if slice[i] > v {
			idx = i
			return
		}
	}
	return
}

func isInArray(v int, slice []int) bool {
	for _, i := range slice {
		if v == i {
			return true
		}
	}
	return false
}

// Wait 等待信号发送
func Wait() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig)
	fmt.Println(<-sig)
}
