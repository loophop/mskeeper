package main

import (
	logmsk "gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/server/httpsvr"
	"gitlab.papegames.com/fringe/mskeeper/server/service"
	"os"
	"strconv"
)

var (
	sSAddrPort = ":10526"
	sAddrPort  = ":11112"
)

func init() {
	// get environment variables
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}
	sSAddrPort = env("MSKEEPER_SADDR_PORT", ":10526")
	sAddrPort = env("MSKEEPER_ADDR_PORT", ":11112")
}

// @title mskeeper
// @version 2.0.1
// @description  mskeeper api-server
// @BasePath /v1/
func main() {
	// log
	logmsk.MSKLog().SetOutput(os.Stdout)

	// pid file
	pid := os.Getpid()
	pidPath := os.Args[0] + ".pid"
	pidFile, err := os.OpenFile(pidPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		logmsk.MSKLog().Errorf("error(%v) opening pid file %v \n", err, pidPath)
		os.Exit(-1)
	}

	_, err = pidFile.WriteString(strconv.Itoa(pid))
	if err != nil {
		logmsk.MSKLog().Errorf("write to pidfile %v failed(%v)", pidPath, err)
		os.Exit(-1)
	}

	// handling system signals
	go SignalProc()

	langPath := "langs"
	err = service.Init(langPath)
	if err != nil {
		logmsk.MSKLog().Errorf("service.Init of %v failed(%v)", langPath, err)
		os.Exit(-1)
	}

	router := httpsvr.RegisterHTTPSHandler()

	go func() {
		err = router.Run(sAddrPort)
		if err != nil {
			panic(err)
		}
	}()

	err = router.RunTLS(sSAddrPort, "cert.pem", "key.pem")
	if err != nil {
		panic(err)
	}

}
