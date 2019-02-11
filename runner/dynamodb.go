package runner

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/awslabs/goformation/cloudformation"
)

// DynamoDB provides dynamodb operation from cloudformation settings
type DynamoDB struct {
	ddb dynamodbiface.DynamoDBAPI
	tbl cloudformation.AWSDynamoDBTable
}

// NewDynamoDB returns DynamoDB operation object
func NewDynamoDB(ddb dynamodbiface.DynamoDBAPI, tbl cloudformation.AWSDynamoDBTable) *DynamoDB {
	return &DynamoDB{ddb, tbl}
}

// CreateIfNotExists provides create table operation
func (t *DynamoDB) CreateIfNotExists() bool {
	tblName := t.tbl.TableName
	schema := []*dynamodb.KeySchemaElement{}
	for _, s := range t.tbl.KeySchema {
		schema = append(schema, (&dynamodb.KeySchemaElement{}).
			SetAttributeName(s.AttributeName).
			SetKeyType(s.KeyType))
	}
	attrs := []*dynamodb.AttributeDefinition{}
	for _, d := range t.tbl.AttributeDefinitions {
		attrs = append(attrs, (&dynamodb.AttributeDefinition{}).
			SetAttributeName(d.AttributeName).
			SetAttributeType(d.AttributeType))
	}
	input := (&dynamodb.CreateTableInput{}).
		SetTableName(tblName).
		SetAttributeDefinitions(attrs).
		SetKeySchema(schema)
	if tp := t.tbl.ProvisionedThroughput; tp != nil {
		input = input.SetProvisionedThroughput((&dynamodb.ProvisionedThroughput{}).
			SetReadCapacityUnits(tp.ReadCapacityUnits).
			SetWriteCapacityUnits(tp.WriteCapacityUnits))
	}
	var created bool
	_, err := t.ddb.CreateTable(input)
	if err != nil {
		e, ok := err.(awserr.Error)
		if !ok || e.Code() != dynamodb.ErrCodeTableAlreadyExistsException {
			panic(err)
		}
		created = true
	}
	if ttl := t.tbl.TimeToLiveSpecification; ttl != nil {
		spec := (&dynamodb.TimeToLiveSpecification{}).
			SetAttributeName(ttl.AttributeName).
			SetEnabled(ttl.Enabled)
		t.ddb.UpdateTimeToLive((&dynamodb.UpdateTimeToLiveInput{}).
			SetTableName(tblName).
			SetTimeToLiveSpecification(spec))
	}
	return created
}

// Truncate provides cleanup table
func (t *DynamoDB) Truncate() {
	input := (&dynamodb.ScanInput{}).SetTableName(t.tbl.TableName)
	t.ddb.ScanPages(input, func(output *dynamodb.ScanOutput, ok bool) bool {
		for _, item := range output.Items {
			keys := map[string]*dynamodb.AttributeValue{}
			for _, s := range t.tbl.KeySchema {
				name := s.AttributeName
				if val, ok := item[name]; ok {
					keys[name] = val
				}
			}
			t.ddb.DeleteItem((&dynamodb.DeleteItemInput{}).
				SetTableName(t.tbl.TableName).
				SetKey(keys),
			)
		}
		return ok
	})
}
