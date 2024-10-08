package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type ObjectStore interface {
	Put(string, []byte) error
	Get(string) ([]byte, error)
	Delete(string) error
	List(string) []string
}

// ObjectStoreS3 this should probably be fixed to
// a particular bucket
type ObjectStoreS3 struct {
	client *s3.Client
}

func NewObjectStoreS3(s3Config aws.Config) *ObjectStoreS3 {
	client := s3.NewFromConfig(s3Config)

	// Get the first page of results for ListObjectsV2 for a bucket
	output, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String("carnax-test-bucket"),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("first page results:")
	for _, object := range output.Contents {
		log.Printf("key=%s size=%d", aws.ToString(object.Key), object.Size)
	}

	return &ObjectStoreS3{
		client: client,
	}
}

func (o *ObjectStoreS3) List(s string) []string {
	var keys []string
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String("carnax-test-bucket"),
		Prefix: aws.String(s),
	}

	paginator := s3.NewListObjectsV2Paginator(o.client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			log.Printf("Error listing objects: %v", err)
			return keys
		}

		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}

	return keys
}

func (o *ObjectStoreS3) Put(s string, data []byte) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String("carnax-test-bucket"),
		Key:    aws.String(s),
		Body:   bytes.NewReader(data),
	}

	_, err := o.client.PutObject(context.Background(), input)
	if err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}

	return nil
}

func (o *ObjectStoreS3) Get(s string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String("carnax-test-bucket"),
		Key:    aws.String(s),
	}

	result, err := o.client.GetObject(context.Background(), input)
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer result.Body.Close()

	return io.ReadAll(result.Body)
}

func (o *ObjectStoreS3) Delete(s string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String("carnax-test-bucket"),
		Key:    aws.String(s),
	}

	_, err := o.client.DeleteObject(context.Background(), input)
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	return nil
}
