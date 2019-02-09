package component

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/awslabs/goformation/cloudformation"
)

type DynamoDB struct {
	ddb *dynamodb.DynamoDB
	tbl cloudformation.AWSDynamoDBTable
}

func NewDynamoDB(ddb *dynamodb.DynamoDB, tbl cloudformation.AWSDynamoDBTable) *DynamoDB {
	return &DynamoDB{ddb, tbl}
}

func (t *DynamoDB) CreateIfNotExists() bool {
	tblName := t.tbl.TableName
	schema := []*dynamodb.KeySchemaElement{}
	for _, s := range t.tbl.KeySchema {
		schema = append(schema, (&dynamodb.KeySchemaElement{}).
			SetAttributeName(s.AttributeName).
			SetKeyType(s.KeyType))
	}
	var created bool
	_, err := t.ddb.CreateTable((&dynamodb.CreateTableInput{}).
		SetTableName(tblName).
		SetKeySchema(schema))
	if err != nil {
		e, ok := err.(awserr.Error)
		if !ok || e.Code() != dynamodb.ErrCodeTableAlreadyExistsException {
			panic(err)
		}
		created = true
	}
	ttl := t.tbl.TimeToLiveSpecification
	t.ddb.UpdateTimeToLive((&dynamodb.UpdateTimeToLiveInput{}).
		SetTableName(tblName).
		SetTimeToLiveSpecification((&dynamodb.TimeToLiveSpecification{}).
			SetAttributeName(ttl.AttributeName).
			SetEnabled(ttl.Enabled)))
	return created
}

func (t *DynamoDB) Truncate() {
	input := (&dynamodb.ScanInput{}).SetTableName(t.tbl.TableName)
	t.ddb.ScanPages(input, func(output *dynamodb.ScanOutput, ok bool) bool {
		for _, item := range output.Items {
			keys := map[string]*dynamodb.AttributeValue{}
			for _, s := range t.tbl.KeySchema {
				name := s.AttributeName
				keys[name] = item[name]
			}
			t.ddb.DeleteItem((&dynamodb.DeleteItemInput{}).
				SetTableName(t.tbl.TableName).
				SetKey(keys),
			)
		}
		return ok
	})
}
