package cfnci

import (
	"bytes"
	"log"
	"os"
	"text/template"

	"github.com/awslabs/goformation"
	"github.com/awslabs/goformation/cloudformation"
)

func parse(path string, data map[string]interface{}) []byte {
	w := bytes.NewBuffer([]byte{})
	t, err := template.New("resource").ParseFiles(path)
	if err != nil {
		log.Fatalf("failed to parse template: %v", err)
	}
	t = t.Templates()[0]
	t.Execute(w, data)
	return w.Bytes()
}

// ParseYAML provides parsing go template and returns cloudformation templates from its parsed YAML
func ParseYAML(path string, data map[string]interface{}) (*cloudformation.Template, error) {
	if _, err := os.Stat(path); err != nil {
		log.Fatalf("filepath: %s has error!: %v", path, err)
	}
	bs := parse(path, data)
	return goformation.ParseYAML(bs)
}
