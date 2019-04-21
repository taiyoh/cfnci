package runner

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
)

func compareAWSErrorCode(err error, code string) bool {
	e, ok := err.(awserr.Error)
	if !ok {
		return false
	}
	return e.Code() == code
}
