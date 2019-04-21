package cfnci

import (
	"os"

	"github.com/awslabs/goformation"
	"github.com/awslabs/goformation/cloudformation"
	"github.com/awslabs/goformation/intrinsics"
	"github.com/taiyoh/cfnci/runner"
)

type Provider struct {
	cfTemplate *cloudformation.Template
}

func New(path string, overrides map[string]interface{}) (*Provider, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	cft, err := goformation.OpenWithOptions(path, &intrinsics.ProcessorOptions{
		ParameterOverrides: overrides,
	})
	if err != nil {
		return nil, err
	}
	return &Provider{cft}, nil
}

func (p *Provider) DynamoDB(ddb dynamodbiface.DynamoDBAPI, name string) (*runner.DynamoDB, error) {
	tbl, err := p.cfTemplate.GetAWSDynamoDBTableWithName(name)
	if err != nil {
		return nil, err
	}
	return runner.NewDynamoDB(ddb, tbl)
}

func (p *Provider) SQS(svc sqsiface.SQSAPI, endpoint, name string) (*runner.SQS, error) {
	q, err := p.cfTemplate.GetAWSSQSQueueWithName(name)
	if err != nil {
		return nil, err
	}
	return runner.NewSQS(endpoint, svc, q)
}