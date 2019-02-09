package runner

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/awslabs/goformation/cloudformation"
)

// SQS provides sqs operation from cloudformation settings
type SQS struct {
	endpoint string
	svc      sqsiface.SQSAPI
	que      cloudformation.AWSSQSQueue
}

// NewSQS returns SQS operation object
func NewSQS(endpoint string, svc sqsiface.SQSAPI, que cloudformation.AWSSQSQueue) *SQS {
	return &SQS{endpoint, svc, que}
}

// CreateIfNotExists provides create queue operation
func (q *SQS) CreateIfNotExists() bool {
	_, err := q.svc.CreateQueue((&sqs.CreateQueueInput{}).SetQueueName(q.que.QueueName))
	if err != nil {
		e, ok := err.(awserr.Error)
		if !ok || e.Code() != sqs.ErrCodeQueueNameExists {
			panic(err)
		}
		return false
	}
	return true
}

// Purge provides cleanup queue
func (q *SQS) Purge() {
	queueURL := fmt.Sprintf("%s/%s", q.endpoint, q.que.QueueName)
	q.svc.PurgeQueue((&sqs.PurgeQueueInput{}).SetQueueUrl(queueURL))
}
