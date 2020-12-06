package main

import (
	"gitlab.papegames.com/fringe/mskeeper/log"
	"os"
	"os/signal"
	"syscall"
)

//----------------------------------------------- handle unix signals
func SignalProc() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGABRT, syscall.SIGTERM, syscall.SIGINT)
	defer func() {
		if x := recover(); x != nil {
			log.MSKLog().Infof("caught panic in SignalProc(%v)", x)
		}
		pidPath := os.Args[0] + ".pid"
		os.Remove(pidPath)
		log.MSKLog().Infof("pid file %v removed\n", pidPath)
		//os.Exit(0)
		panic("continue panic for stack")
	}()
	for {
		msg := <-ch
		switch msg {
		case syscall.SIGABRT:
			pidPath := os.Args[0] + ".pid"
			os.Remove(pidPath)
			log.MSKLog().Infof("pid file %v removed\n", pidPath)

			panic("syscall.SIGABRT")

		case syscall.SIGHUP: // reload config

		case syscall.SIGINT:
			fallthrough
			//gamedata.LoadGameConfig()
		case syscall.SIGTERM: // server close
			log.MSKLog().Infof("\033[043;1m[SIGTERM]\033[0m")

			log.MSKLog().Infof("\033[043;1mThanks God\033[0m")
			pidPath := os.Args[0] + ".pid"
			os.Remove(pidPath)
			log.MSKLog().Infof("pid file %v removed\n", pidPath)

			os.Exit(0)
		case syscall.SIGKILL:
			log.MSKLog().Infof("\033[043;1m[SIGKILL]\033[0m")
		}
	}
}
