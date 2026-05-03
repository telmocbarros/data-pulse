package dataset

import (
	"context"
	"fmt"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/telmocbarros/data-pulse/config"
)

// StoreRawFile uploads the raw uploaded file to the object store under
// the datasets bucket, keyed by "<datasetId>/<fileName>".
func StoreRawFile(datasetId string, filePath string, fileName string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("unable to open file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("unable to stat file: %w", err)
	}

	objectKey := fmt.Sprintf("%s/%s", datasetId, fileName)
	_, err = config.FileStorage.PutObject(
		context.Background(),
		config.DatasetsBucket,
		objectKey,
		f,
		info.Size(),
		minio.PutObjectOptions{},
	)
	if err != nil {
		return fmt.Errorf("unable to upload file to object store: %w", err)
	}

	return nil
}
