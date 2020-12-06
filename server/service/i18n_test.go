package service

import (
	"github.com/gavv/httpexpect"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

var server *httptest.Server

func RegisterFakeHandler() *gin.Engine {

	gin.SetMode(gin.ReleaseMode)

	router := gin.Default()
	router.POST("/i18Ntest",
		func(c *gin.Context) {

			ret := make(map[string]interface{})
			defer c.JSON(http.StatusOK, ret)

			log.Printf("c %v ret %v", c, ret)
			ret["data"] = Tr(GetLanguage(c.Request), "hello_world")
			ret["data2"] = Tr(GetLanguage(c.Request), "hello_hell")

		})

	return router
}

func TestTrNormal(t *testing.T) {

}

func TestI18NNormal(t *testing.T) {

	Init("../langs")

	server = httptest.NewServer(RegisterFakeHandler())
	defer server.Close()

	e := httpexpect.New(t, server.URL)

	inargs := map[string]interface{}{
		"lang": "en",
	}
	retData := e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data").ValueEqual("data", "Hello World")

	inargs = map[string]interface{}{
		"lang": "zh",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data").ValueEqual("data", "你好，世界")

	inargs = map[string]interface{}{
		"lang": "jp",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()

	log.Printf("retData %v", retData)
	retData.ContainsKey("data").ValueEqual("data", "こんにちは")

	inargs = map[string]interface{}{
		"lang": "jp",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data").ValueEqual("data", "こんにちは")

	inargs = map[string]interface{}{
		"LANG": "jp",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data").ValueEqual("data", "こんにちは")

	inargs = map[string]interface{}{
		"lang": "zh-CHT",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data").ValueEqual("data", "Hello World")

	inargs = map[string]interface{}{
		"lang": "ko",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data").ValueEqual("data", "안녕! 월드!")

	inargs = map[string]interface{}{
		"LANG": "ko",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data").ValueEqual("data", "안녕! 월드!")

}

func TestI18NFailure(t *testing.T) {

	Init("../langs")

	server = httptest.NewServer(RegisterFakeHandler())
	defer server.Close()

	e := httpexpect.New(t, server.URL)

	// 1. 不支持的语言统一英语
	inargs := map[string]interface{}{
		"lang": "hk",
	}
	retData := e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data").ValueEqual("data", "Hello World")

	inargs = map[string]interface{}{
		"lang": "de",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data").ValueEqual("data", "Hello World")

	inargs = map[string]interface{}{
		"lang": "",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data").ValueEqual("data", "Hello World")

	inargs = map[string]interface{}{
		"LANG": "da",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data").ValueEqual("data", "Hello World")

	// Nothing founded for key hello_hell
	inargs = map[string]interface{}{
		"lang": "en",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data2").ValueEqual("data2", "hello_hell")

	inargs = map[string]interface{}{
		"lang": "abc",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data2").ValueEqual("data2", "hello_hell")

	inargs = map[string]interface{}{
		"lang": "kr",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data2").ValueEqual("data2", "hello_hell")
}

func TestI18NLangsInvalidate(t *testing.T) {

	Init("../langs")

	server = httptest.NewServer(RegisterFakeHandler())
	defer server.Close()

	e := httpexpect.New(t, server.URL)

	// 1. 不支持的语言统一英语
	inargs := map[string]interface{}{
		"lang": "hk",
	}
	retData := e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data").ValueEqual("data", "Hello World")

	inargs = map[string]interface{}{
		"lang": "de",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data").ValueEqual("data", "Hello World")

	inargs = map[string]interface{}{
		"lang": "",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data").ValueEqual("data", "Hello World")

	inargs = map[string]interface{}{
		"LANG": "da",
	}
	retData = e.POST("/i18Ntest").WithQueryObject(inargs).Expect().JSON().Object()
	retData.ContainsKey("data").ValueEqual("data", "Hello World")
}

// cleanGlobPath prepares path for glob matching.
func cleanGlobPath(path string) string {
	switch path {
	case "":
		return "."
	case string(os.PathSeparator):
		// do nothing to the path
		return path
	default:
		return path[0 : len(path)-1] // chop off trailing separator
	}
}

func TestI18NLangsInvalidLangs(t *testing.T) {

	// folder not exists
	err := initLocales("langs")
	if err == nil {
		t.Fatalf("folder not exist, should be failed")
	}

	// folder exists but no file
	err = initLocales("../logs")
	if err == nil {
		t.Fatalf("folder exists but no file, should be failed")
	}

	// normallly
	err = initLocales("../langs")
	if err != nil {
		t.Fatalf("folder not exist, should be failed")
	}

	// make a temp folder with lang file in bad format
	_ = os.Mkdir("tmplangs", os.ModePerm)
	enfilePath := "./tmplangs/en.json"
	enfile, err := os.OpenFile(enfilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		t.Fatalf("failed %v to create file %v", err, enfilePath)
	}

	_, err = enfile.WriteString(`{hello_world": "Hello World","dsn_empty": "dsn读取失败}`)
	if err != nil {
		t.Fatalf("failed %v to write file %v", err, enfile)
	}
	err = initLocales("tmplangs")
	if err == nil {
		t.Fatalf("bad file format, should be failed")
	}

	_ = os.RemoveAll("tmplangs")

	// make a temp folder with no read permission
	_ = os.Mkdir("tmplangs", os.ModePerm)
	enfilePath = "./tmplangs/en.json"
	enfile, err = os.OpenFile(enfilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.ModeExclusive)
	if err != nil {
		t.Fatalf("failed %v to create file %v", err, enfilePath)
	}

	_, err = enfile.WriteString(`"{hello_world": "Hello World","dsn_empty": "dsn读取失败"}`)
	if err != nil {
		t.Fatalf("failed %v to write file %v", err, enfile)
	}
	err = initLocales("tmplangs")
	if err == nil {
		t.Fatalf("bad file format, should be failed")
	}

	err = loadTranslations("tmplangs")
	if err == nil {
		t.Fatalf("bad file format, should be failed")
	}

	_ = os.RemoveAll("tmplangs")

	pattern := "[/*" ////!!!!!!!!!!!!!!!!!yes
	dir, _ := filepath.Split(pattern)
	log.Printf("dir1 %v", dir)
	dir = cleanGlobPath(dir)
	log.Printf("dir2 %v", dir)
	err = loadTranslations(pattern)
	log.Printf("~~~~~~err = %v", err)
	if err != filepath.ErrBadPattern {
		t.Fatalf("bad path, should be failed")
	}

	_ = os.RemoveAll("tmplangs")
}
