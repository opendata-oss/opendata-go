package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3 is an ObjectStore backed by AWS S3 (or S3-compatible stores like MinIO).
type S3 struct {
	client *s3.Client
	bucket string
}

// NewS3 creates a new S3-backed ObjectStore.
func NewS3(client *s3.Client, bucket string) *S3 {
	return &S3{client: client, bucket: bucket}
}

// Get implements ObjectStore.
func (s *S3) Get(ctx context.Context, path string) (GetResult, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &path,
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return GetResult{}, ErrNotFound
		}
		// S3 may also return a generic not-found for missing keys.
		if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "StatusCode: 404") {
			return GetResult{}, ErrNotFound
		}
		return GetResult{}, err
	}
	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return GetResult{}, err
	}

	var etag string
	if out.ETag != nil {
		etag = *out.ETag
	}
	return GetResult{
		Data:    data,
		Version: Version{ETag: etag},
	}, nil
}

// Put implements ObjectStore.
//
// Wraps `data` in a bytes.Reader rather than copying it through a
// string. Building the request body with strings.NewReader(string(data))
// would force an allocation + memcpy of the full payload; for batches
// in the megabytes-to-tens-of-megabytes range that copy would be the
// dominant memory cost of the PUT path.
func (s *S3) Put(ctx context.Context, path string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &path,
		Body:   bytes.NewReader(data),
	})
	return err
}

// PutIfMatch implements ObjectStore. Same zero-copy body wrapper as
// Put.
func (s *S3) PutIfMatch(ctx context.Context, path string, data []byte, version *Version) error {
	input := &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &path,
		Body:   bytes.NewReader(data),
	}
	if version == nil {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(version.ETag)
	}

	_, err := s.client.PutObject(ctx, input)
	if err != nil {
		// S3 returns 412 Precondition Failed for conditional put failures.
		if strings.Contains(err.Error(), "PreconditionFailed") ||
			strings.Contains(err.Error(), "StatusCode: 412") ||
			strings.Contains(err.Error(), "ConditionalRequestConflict") ||
			strings.Contains(err.Error(), "At least one of the pre-conditions") {
			return ErrPreconditionFailed
		}
		return err
	}
	return nil
}

// Delete implements ObjectStore.
func (s *S3) Delete(ctx context.Context, path string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &path,
	})
	return err
}
