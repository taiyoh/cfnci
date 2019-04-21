package cfnci

import (
	"os"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/awslabs/goformation"
	"github.com/awslabs/goformation/cloudformation"
	"github.com/awslabs/goformation/intrinsics"
	"github.com/taiyoh/cfnci/runner"
)

// Provider represents holding cloudformation template from code.
type Provider struct {
	cfTemplate *cloudformation.Template
}

// New returns Provider object.
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

// DynamoDB returns runner.DynamoDB object.
func (p *Provider) DynamoDB(ddb dynamodbiface.DynamoDBAPI, name string) (*runner.DynamoDB, error) {
	tbl, err := p.cfTemplate.GetAWSDynamoDBTableWithName(name)
	if err != nil {
		return nil, err
	}
	return runner.NewDynamoDB(ddb, tbl), nil
}

// SQS returns runner.SQS object.
func (p *Provider) SQS(svc sqsiface.SQSAPI, endpoint, name string) (*runner.SQS, error) {
	q, err := p.cfTemplate.GetAWSSQSQueueWithName(name)
	if err != nil {
		return nil, err
	}
	return runner.NewSQS(endpoint, svc, q), nil
}
