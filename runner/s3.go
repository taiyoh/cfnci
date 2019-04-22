package runner

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/awslabs/goformation/cloudformation/resources"
)

// S3 provides s3 operation from cloudformation settings.
type S3 struct {
	resource *resources.AWSS3Bucket
	svc      s3iface.S3API
}

// NewS3 returns S3 operation object.
func NewS3(svc s3iface.S3API, resource *resources.AWSS3Bucket) *S3 {
	return &S3{resource, svc}
}

// CreateIfNotExists provides create bucket operation.
func (s *S3) CreateIfNotExists() bool {
	_, err := s.svc.CreateBucket((&s3.CreateBucketInput{}).SetBucket(s.resource.BucketName))
	if err != nil {
		if !compareAWSErrorCode(err, s3.ErrCodeBucketAlreadyOwnedByYou) {
			panic(err)
		}
		return false
	}
	return true
}
