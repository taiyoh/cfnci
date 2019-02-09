package parser

import (
	"bytes"
	"log"
	"os"
	"text/template"

	"github.com/awslabs/goformation"
	"github.com/awslabs/goformation/cloudformation"
)

type Parser struct {
	path string
}

func New(path string) *Parser {
	if _, err := os.Stat(path); err != nil {
		log.Fatalf("filepath: %s has error!: %v", path, err)
	}
	return &Parser{path}
}

func (p *Parser) Parse(data map[string]interface{}) (*cloudformation.Template, error) {
	t, err := template.New("resource").ParseFiles(p.path)
	if err != nil {
		log.Fatalf("template parse failed: %v", err)
	}
	t = t.Templates()[0]
	w := bytes.NewBuffer([]byte{})
	if data == nil {
		data = map[string]interface{}{}
	}
	t.Execute(w, data)
	return goformation.ParseYAML(w.Bytes())
}
