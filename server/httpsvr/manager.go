package httpsvr

import (
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/swaggo/gin-swagger"
	"github.com/swaggo/gin-swagger/swaggerFiles"
	_ "gitlab.papegames.com/fringe/mskeeper/server/docs"
	"net/http"
)

// 注册 HTTPS handler
func RegisterHTTPSHandler() *gin.Engine {

	gin.SetMode(gin.ReleaseMode)

	router := gin.Default()

	pprof.Register(router) // 性能

	v1 := router.Group("/v1")
	{
		v1.POST("/mysql/init", MySQLInitHandlerS)
		v1.POST("/mysql/check", MySQLCheckHandlerS)
		v1.POST("/mysql/run", MySQLRunHandlerS)
	}

	// 文档界面访问URL
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	router.GET("/", func(c *gin.Context) {
		// 指定重定向的URL 通过HandleContext进行重定向到test2 页面显示json数据
		c.Redirect(http.StatusMovedPermanently, "/swagger/index.html")
	})

	return router
}
