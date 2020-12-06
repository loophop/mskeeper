package service

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"
)

// Translation - localization structure
type translation struct {
	locales      []string
	translations map[string]map[string]string
}

var trans *translation

// initLocales - initiate locales from the folder
func initLocales(trPath string) error {
	trans = &translation{translations: make(map[string]map[string]string)}
	return loadTranslations(trPath)
}

// Tr - translate for current locale
func Tr(locale string, trKey string) string {
	trValue, ok := trans.translations[locale][trKey]
	if ok {
		return trValue
	}
	trValue, ok = trans.translations["en"][trKey]
	if ok {
		return trValue
	}
	return trKey
}

// GetLanguage - get language, which is in force
func GetLanguage(r *http.Request) string {

	langLower := r.FormValue("lang")
	langUpper := r.FormValue("LANG")

	if langLower != "" {
		return langLower
	} else if langUpper != "" {
		return langUpper
	}
	return ""
}

// LoadTranslations - load translations files from the folder
func loadTranslations(trPath string) error {
	files, err := filepath.Glob(trPath + "/*.json")
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return errors.New("No translations found")
	}

	for _, file := range files {
		err := loadFileToMap(file)
		if err != nil {
			return err
		}
	}
	return nil
}

func loadFileToMap(filename string) error {
	var objmap map[string]string

	localName := strings.Replace(filepath.Base(filename), ".json", "", 1)

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	err = json.Unmarshal(content, &objmap)
	if err != nil {
		return err
	}
	trans.translations[localName] = objmap
	trans.locales = append(trans.locales, localName)
	return nil
}
