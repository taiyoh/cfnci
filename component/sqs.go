package component

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/awslabs/goformation/cloudformation"
)

type SQS struct {
	endpoint string
	svc      *sqs.SQS
	que      cloudformation.AWSSQSQueue
}

func NewSQS(endpoint string, svc *sqs.SQS, que cloudformation.AWSSQSQueue) *SQS {
	return &SQS{endpoint, svc, que}
}

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

func (q *SQS) Purge() {
	queueURL := fmt.Sprintf("%s/%s", q.endpoint, q.que.QueueName)
	q.svc.PurgeQueue((&sqs.PurgeQueueInput{}).SetQueueUrl(queueURL))
}
