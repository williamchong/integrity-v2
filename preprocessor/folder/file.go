package preprocessor_folder

import (
	"archive/zip"
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
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/starlinglab/integrity-v2/config"
	"github.com/starlinglab/integrity-v2/util"
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

func getProofModeFileMetadatas(filePath string) ([]map[string]any, error) {
	mediaType := "application/zip"
	assets, err := util.ReadAndVerifyProofModeMetadata(filePath)
	if err != nil {
		return nil, err
	}
	metadatas := []map[string]any{}
	for _, asset := range assets {
		syncRoot := config.GetConfig().FolderPreprocessor.SyncFolderRoot
		fileName := filepath.Base(asset.Metadata.FilePath)
		metadata := map[string]any{
			"sha256":          asset.Sha256,
			"md5":             asset.Md5,
			"blake3":          asset.Blake3,
			"file_size":       asset.FileSize,
			"file_name":       fileName,
			"last_modified":   asset.Metadata.FileModified,
			"time_created":    asset.Metadata.FileCreated,
			"asset_origin":    filepath.Join(strings.TrimPrefix(filePath, syncRoot), asset.Metadata.FilePath),
			"asset_signature": hex.EncodeToString(asset.AssetSignature),
			"media_type":      mediaType,
			"proofmode": map[string]([]byte){
				"metadata":  asset.MetadataBytes,
				"meta_sig":  asset.MetadataSignature,
				"media_sig": asset.AssetSignature,
				"pubkey":    asset.PubKey,
				"ots":       asset.Ots,
				"gst":       asset.Gst,
			},
		}
		metadatas = append(metadatas, metadata)
	}
	return metadatas, nil
}

// getFileMetadata calculates and returns a map of attributes for a file
func getFileMetadata(filePath string, mediaType string) (map[string]any, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	file.Seek(0, 0)
	sha := sha256.New()
	md := md5.New()
	blake := blake3.New(32, nil)

	writers := io.MultiWriter(sha, md, blake)
	_, err = io.Copy(writers, file)
	if err != nil {
		return nil, err
	}

	syncRoot := config.GetConfig().FolderPreprocessor.SyncFolderRoot

	return map[string]any{
		"sha256":        hex.EncodeToString(sha.Sum(nil)),
		"md5":           hex.EncodeToString(md.Sum(nil)),
		"blake3":        hex.EncodeToString(blake.Sum(nil)),
		"file_size":     fileInfo.Size(),
		"file_name":     fileInfo.Name(),
		"media_type":    mediaType, // "application/zip" or "application/octet-stream
		"last_modified": fileInfo.ModTime().Format(time.RFC3339),
		"time_created":  fileInfo.ModTime().Format(time.RFC3339),
		"asset_origin":  strings.TrimPrefix(filePath, syncRoot),
	}, nil
}

func checkFileType(filePath string) (string, string, error) {
	fileType := "generic" // default is generic
	file, err := os.Open(filePath)
	if err != nil {
		return "", "", err
	}
	defer file.Close()
	buffer := make([]byte, 512)
	n, err := file.Read(buffer)
	if err != nil {
		return "", "", err
	}
	mediaType := http.DetectContentType(buffer[:n])
	if mediaType == "application/zip" {
		isProofMode := util.CheckIsProofModeFile(filePath)
		if isProofMode {
			fileType = "proofmode"
		}
	}
	return fileType, mediaType, nil
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

	fileType, mediaType, err := checkFileType(filePath)
	if err != nil {
		setFileStatusError(pgPool, filePath, err.Error())
		return "", fmt.Errorf("error checking file type: %v", err)
	}

	metadatas := []map[string]any{}
	switch fileType {
	case "proofmode":
		metadatas, err = getProofModeFileMetadatas(filePath)
		if err != nil {
			setFileStatusError(pgPool, filePath, err.Error())
			return "", fmt.Errorf("error getting proofmode file metadatas: %v", err)
		}
	case "generic":
		metadata, err := getFileMetadata(filePath, mediaType)
		if err != nil {
			setFileStatusError(pgPool, filePath, err.Error())
			return "", fmt.Errorf("error getting file metadata: %v", err)
		}
		metadatas = append(metadatas, metadata)
	}

	if project != nil {
		for _, metadata := range metadatas {
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
	}

	err = setFileStatusUploading(pgPool, filePath, metadatas[0]["sha256"].(string))
	if err != nil {
		return "", fmt.Errorf("error setting file status to uploading: %v", err)
	}

	switch fileType {
	case "proofmode":
		zipListing, err := zip.OpenReader(filePath)
		if err != nil {
			setFileStatusError(pgPool, filePath, err.Error())
			return "", fmt.Errorf("error opening zip file %s: %v", filePath, err)
		}
		defer zipListing.Close()
		fileMap, _, err := util.GetProofModeZipFiles(zipListing)
		if err != nil {
			setFileStatusError(pgPool, filePath, err.Error())
			return "", fmt.Errorf("error getting files from zip: %v", err)
		}
		for _, metadata := range metadatas {
			fileName := metadata["file_name"].(string)
			if zipFile, ok := fileMap[fileName]; ok {
				file, err := zipFile.Open()
				if err != nil {
					setFileStatusError(pgPool, filePath, err.Error())
					return "", fmt.Errorf("error opening file %s in zip: %v", fileName, err)
				}
				defer file.Close()
				resp, err := webhook.PostFileToWebHook(file, metadata, webhook.PostGenericWebhookOpt{Format: "cbor"})
				if err != nil {
					setFileStatusError(pgPool, filePath, err.Error())
					return "", fmt.Errorf("error posting metadata for file %s: %v", filePath, err)
				}
				cid = resp.Cid
			} else {
				setFileStatusError(pgPool, filePath, fmt.Sprintf("file %s not found in zip", fileName))
				return "", fmt.Errorf("file %s not found in zip", fileName)
			}
		}
	case "generic":
		file, err := os.Open(filePath)
		if err != nil {
			setFileStatusError(pgPool, filePath, err.Error())
			return "", fmt.Errorf("error opening file %s: %v", filePath, err)
		}
		defer file.Close()
		resp, err := webhook.PostFileToWebHook(file, metadatas[0], webhook.PostGenericWebhookOpt{})
		if err != nil {
			setFileStatusError(pgPool, filePath, err.Error())
			return "", fmt.Errorf("error posting metadata for file %s: %v", filePath, err)
		}
		cid = resp.Cid
	}

	err = setFileStatusDone(pgPool, filePath, cid)
	if err != nil {
		return "", fmt.Errorf("error setting file status to done: %v", err)
	}
	return cid, nil
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
