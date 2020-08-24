package datastore

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/zatte/dex/storage"
	"github.com/zatte/dex/storage/conformance"
)

const DsProjectId = "DATASTORE_PROJECT_ID"

func TestStorage(t *testing.T) {

	projectId := os.Getenv(DsProjectId)
	if projectId == "" {
		t.Skipf("test environment variable %q not set, skipping", DsProjectId)
	}

	logger := &logrus.Logger{
		Out:       os.Stderr,
		Formatter: &logrus.TextFormatter{DisableColors: true},
		Level:     logrus.DebugLevel,
	}

	newStorage := func() storage.Storage {
		c := Config{
			ProjectID:  projectId,
			KindPrefix: "__TestingDex__",
		}
		s, err := c.Open(logger)
		if err != nil {
			t.Skipf("coulnd't create datastore instance: %v", err)
		}
		return s
	}
	conformance.RunTests(t, newStorage)
}
