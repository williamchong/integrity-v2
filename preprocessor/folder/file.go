package preprocessor_folder

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/starlinglab/integrity-v2/config"
	"github.com/starlinglab/integrity-v2/webhook"
	"lukechampine.com/blake3"
)

// File status constants
var (
	FileStatusFound     = "Found"
	FileStatusUploading = "Uploading"
	FileStatusSuccess   = "Success"
	FileStatusError     = "Error"
)

// getFileMetadata calculates and returns a map of attributes for a file
func getFileMetadata(filePath string) (map[string]any, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	sha := sha256.New()
	md := md5.New()
	blake := blake3.New(32, nil)

	tee := io.TeeReader(file, sha)
	tee = io.TeeReader(tee, md)
	tee = io.TeeReader(tee, blake)

	bytes, err := io.ReadAll(tee)
	if err != nil {
		return nil, err
	}
	mediaType := http.DetectContentType(bytes)

	return map[string]any{
		"sha256":        hex.EncodeToString(sha.Sum(nil)),
		"md5":           hex.EncodeToString(md.Sum(nil)),
		"blake3":        hex.EncodeToString(blake.Sum(nil)),
		"media_type":    mediaType,
		"file_size":     fileInfo.Size(),
		"file_name":     fileInfo.Name(),
		"last_modified": fileInfo.ModTime().Format(time.RFC3339),
		"time_created":  fileInfo.ModTime().Format(time.RFC3339),
	}, nil
}

// handleNewFile takes a discovered file, update file status on database,
// and post the file and its metadata to the webhook server
func handleNewFile(pgPool *pgxpool.Pool, filePath string, project *ProjectQueryResult) (string, error) {
	result, err := queryIfFileExists(pgPool, filePath)
	if err != nil {
		return "", fmt.Errorf("error checking if file exists in database: %v", err)
	}

	status, errorMessage, cid := "", "", ""
	if result != nil {
		if result.Status != nil {
			status = *result.Status
		}
		if result.ErrorMessage != nil {
			errorMessage = *result.ErrorMessage
		}
		if result.Cid != nil {
			cid = *result.Cid
		}
	}

	switch status {
	case FileStatusFound:
		fmt.Println("retrying found file:", filePath)
	case FileStatusUploading:
		fmt.Println("retrying uploading file:", filePath)
	case FileStatusSuccess:
		return cid, nil
	case FileStatusError:
		return "", fmt.Errorf("file %s has error: %s", filePath, errorMessage)
	default:
		err = setFileStatusFound(pgPool, filePath)
		if err != nil {
			return "", fmt.Errorf("error setting file status to found: %v", err)
		}
	}

	metadata, err := getFileMetadata(filePath)
	if err != nil {
		e := setFileStatusError(pgPool, filePath, err.Error())
		if e != nil {
			fmt.Println("error setting file status to error:", e)
		}
		return "", fmt.Errorf("error getting metadata for file %s: %v", filePath, err)
	}

	if project != nil {
		metadata["project_id"] = *project.ProjectId
		metadata["project_path"] = *project.ProjectPath
		if project.AuthorType != nil || project.AuthorName != nil || project.AuthorIdentifier != nil {
			author := map[string]string{}
			if project.AuthorType != nil {
				author["@type"] = *project.AuthorType
			}
			if project.AuthorName != nil {
				author["name"] = *project.AuthorName
			}
			if project.AuthorIdentifier != nil {
				author["identifier"] = *project.AuthorIdentifier
			}
			metadata["author"] = author
		}
	}

	err = setFileStatusUploading(pgPool, filePath, metadata["sha256"].(string))
	if err != nil {
		return "", fmt.Errorf("error setting file status to uploading: %v", err)
	}
	resp, err := webhook.PostFileToWebHook(filePath, metadata, webhook.PostGenericWebhookOpt{})
	if err != nil {
		e := setFileStatusError(pgPool, filePath, err.Error())
		if e != nil {
			fmt.Println("error setting file status to error:", e)
		}
		return "", fmt.Errorf("error posting metadata for file %s: %v", filePath, err)
	}

	err = setFileStatusDone(pgPool, filePath, cid)
	if err != nil {
		return "", fmt.Errorf("error setting file status to done: %v", err)
	}
	return resp.Cid, nil
}

// checkShouldIncludeFile reports whether the file should be included in the processing
func checkShouldIncludeFile(info fs.FileInfo) bool {
	whiteListExtension := config.GetConfig().FolderPreprocessor.FileExtensions
	var ignoreFileNamePrefix byte = '.'
	ignoreFileSuffix := ".partial"
	fileName := info.Name()
	if fileName[0] == ignoreFileNamePrefix {
		return false
	}
	fileExt := filepath.Ext(fileName)
	if fileExt == ignoreFileSuffix {
		return false
	}
	if slices.Contains(whiteListExtension, fileExt) {
		return true
	}
	return false
}
