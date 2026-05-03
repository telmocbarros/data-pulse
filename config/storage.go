package config

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// FileStorage is the global object-storage client, set by SetupFileStorage.
var FileStorage *minio.Client

// DatasetsBucket is the object-store bucket where uploaded raw files live.
const DatasetsBucket = "datasets"

// SetupFileStorage opens a MinIO client from MINIO_* env vars, ensures the
// datasets bucket exists, and assigns the client to FileStorage.
func SetupFileStorage() error {
	endpoint := os.Getenv("MINIO_ENDPOINT")
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return fmt.Errorf("unable to connect to MinIO: %w", err)
	}

	ctx := context.Background()
	exists, err := client.BucketExists(ctx, DatasetsBucket)
	if err != nil {
		return fmt.Errorf("unable to check bucket: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, DatasetsBucket, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("unable to create bucket: %w", err)
		}
		slog.Info("created object-store bucket", "bucket", DatasetsBucket)
	}

	FileStorage = client
	return nil
}
