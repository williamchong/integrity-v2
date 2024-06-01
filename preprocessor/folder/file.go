package folder

import (
	"archive/zip"
	"encoding/hex"
	"fmt"
	"log"
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
		syncRoot = filepath.Clean(syncRoot)
		fileName := filepath.Base(asset.Metadata.FilePath)
		assetOrigin := filepath.Join(strings.TrimPrefix(filePath, syncRoot), asset.Metadata.FilePath)

		metadata := map[string]any{
			"file_name":       fileName,
			"last_modified":   asset.Metadata.FileModified,
			"time_created":    asset.Metadata.FileCreated,
			"asset_origin":    assetOrigin,
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

func getWaczFileMetadata(filePath string) (map[string]any, error) {
	mediaType := "application/wacz"
	metadata, err := util.ReadAndVerifyWaczMetadata(filePath)
	if err != nil {
		return nil, err
	}
	syncRoot := config.GetConfig().FolderPreprocessor.SyncFolderRoot
	waczMetadata := map[string]any{
		"last_modified":   metadata.Modified,
		"time_created":    metadata.Created,
		"asset_origin":    strings.TrimPrefix(filePath, syncRoot),
		"asset_signature": hex.EncodeToString(metadata.MetadataSignature),
		"media_type":      mediaType,
		"wacz": map[string]([]byte){
			"metadata": metadata.MetadataBytes,
			"meta_sig": metadata.MetadataSignature,
			"pubkey":   metadata.PubKey,
		},
	}
	return waczMetadata, nil
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

	syncRoot := config.GetConfig().FolderPreprocessor.SyncFolderRoot
	syncRoot = filepath.Clean(syncRoot)
	assetOrigin := filepath.Clean(strings.TrimPrefix(filePath, syncRoot))

	return map[string]any{
		"media_type":    mediaType,
		"asset_origin":  assetOrigin,
		"file_name":     fileInfo.Name(),
		"last_modified": fileInfo.ModTime().UTC().Format(time.RFC3339),
		"time_created":  fileInfo.ModTime().UTC().Format(time.RFC3339),
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
		isWacz := util.CheckIsWaczFile(filePath)
		if isWacz {
			fileType = "wacz"
		}
	}
	return fileType, mediaType, nil
}

// handleNewFile takes a discovered file, update file status on database,
// posts the new file and its metadata to the webhook server,
// and returns the CID of the file according to the server.
func handleNewFile(pgPool *pgxpool.Pool, filePath string, project *ProjectQueryResult) (string, error) {
	if len(project.FileExtensions) > 0 {
		fileExt := filepath.Ext(filePath)
		if !slices.Contains(project.FileExtensions, fileExt) {
			return "", nil
		}
	}
	log.Println("found: " + filePath)
	result, err := queryAndSetFoundFileStatus(pgPool, filePath)
	if err != nil {
		return "", fmt.Errorf("error checking if file exists in database: %v", err)
	}

	status, errorMessage, cid := "", "", ""
	if result != nil {
		status = result.Status
		errorMessage = result.ErrorMessage
		cid = result.Cid
	}

	switch status {
	case FileStatusUploading:
		log.Println("retrying uploading file:", filePath)
	case FileStatusSuccess:
		return cid, nil
	case FileStatusError:
		return "", fmt.Errorf("file %s has error: %s", filePath, errorMessage)
	case FileStatusFound:
	default:
		// proceed to upload
	}

	fileType, mediaType, err := checkFileType(filePath)
	if err != nil {
		if err := setFileStatusError(pgPool, filePath, err.Error()); err != nil {
			log.Println("error setting file status to error:", err)
		}
		return "", fmt.Errorf("error checking file type for %s: %v", filePath, err)
	}

	metadatas := []map[string]any{}
	switch fileType {
	case "proofmode":
		metadatas, err = getProofModeFileMetadatas(filePath)
		if err != nil {
			if err := setFileStatusError(pgPool, filePath, err.Error()); err != nil {
				log.Println("error setting file status to error:", err)
			}
			return "", fmt.Errorf("error getting proofmode file metadatas: %v", err)
		}
	case "wacz":
		fileMetadata, err := getFileMetadata(filePath, mediaType)
		if err != nil {
			if err := setFileStatusError(pgPool, filePath, err.Error()); err != nil {
				log.Println("error setting file status to error:", err)
			}
			return "", fmt.Errorf("error getting file metadata: %v", err)
		}
		waczMetadata, err := getWaczFileMetadata(filePath)
		if err != nil {
			if err := setFileStatusError(pgPool, filePath, err.Error()); err != nil {
				log.Println("error setting file status to error:", err)
			}
			return "", fmt.Errorf("error getting wacz file metadatas: %v", err)
		}
		metadata := map[string]any{}
		for k, v := range fileMetadata {
			metadata[k] = v
		}
		for k, v := range waczMetadata {
			metadata[k] = v
		}
		metadatas = append(metadatas, metadata)
	case "generic":
		metadata, err := getFileMetadata(filePath, mediaType)
		if err != nil {
			if err := setFileStatusError(pgPool, filePath, err.Error()); err != nil {
				log.Println("error setting file status to error:", err)
			}
			return "", fmt.Errorf("error getting file metadata: %v", err)
		}
		metadatas = append(metadatas, metadata)
	}

	err = setFileStatusUploading(pgPool, filePath)
	if err != nil {
		return "", fmt.Errorf("error setting file status to uploading: %v", err)
	}
	if project != nil {
		for _, metadata := range metadatas {
			metadata["project_id"] = project.ProjectId
			metadata["project_path"] = filepath.Clean(project.ProjectPath)
			if project.AuthorType != "" || project.AuthorName != "" || project.AuthorIdentifier != "" {
				author := map[string]string{}
				if project.AuthorType != "" {
					author["@type"] = project.AuthorType
				}
				if project.AuthorName != "" {
					author["name"] = project.AuthorName
				}
				if project.AuthorIdentifier != "" {
					author["identifier"] = project.AuthorIdentifier
				}
				metadata["author"] = author
			}
		}
	}

	switch fileType {
	case "proofmode":
		zipListing, err := zip.OpenReader(filePath)
		if err != nil {
			if err := setFileStatusError(pgPool, filePath, err.Error()); err != nil {
				log.Println("error setting file status to error:", err)
			}
			return "", fmt.Errorf("error opening zip file %s: %v", filePath, err)
		}
		defer zipListing.Close()
		fileMap, _, err := util.GetProofModeZipFiles(zipListing)
		if err != nil {
			if err := setFileStatusError(pgPool, filePath, err.Error()); err != nil {
				log.Println("error setting file status to error:", err)
			}
			return "", fmt.Errorf("error getting files from zip: %v", err)
		}
		for _, metadata := range metadatas {
			fileName := metadata["file_name"].(string)
			if zipFile, ok := fileMap[fileName]; ok {
				file, err := zipFile.Open()
				if err != nil {
					if err := setFileStatusError(pgPool, filePath, err.Error()); err != nil {
						log.Println("error setting file status to error:", err)
					}
					return "", fmt.Errorf("error opening file %s in zip: %v", fileName, err)
				}
				defer file.Close()
				resp, err := webhook.PostFileToWebHook(file, metadata, webhook.PostGenericWebhookOpt{Format: "cbor"})
				if err != nil {
					if err := setFileStatusError(pgPool, filePath, err.Error()); err != nil {
						log.Println("error setting file status to error:", err)
					}
					return "", fmt.Errorf("error posting metadata for file %s: %v", filePath, err)
				}
				cid = resp.Cid
			} else {
				if err := setFileStatusError(pgPool, filePath, err.Error()); err != nil {
					log.Println("error setting file status to error:", err)
				}
				return "", fmt.Errorf("file %s not found in zip", fileName)
			}
		}
	case "wacz":
	case "generic":
		file, err := os.Open(filePath)
		if err != nil {
			if err := setFileStatusError(pgPool, filePath, err.Error()); err != nil {
				log.Println("error setting file status to error:", err)
			}
			return "", fmt.Errorf("error opening file %s: %v", filePath, err)
		}
		defer file.Close()
		resp, err := webhook.PostFileToWebHook(file, metadatas[0], webhook.PostGenericWebhookOpt{})
		if err != nil {
			if err := setFileStatusError(pgPool, filePath, err.Error()); err != nil {
				log.Println("error setting file status to error:", err)
			}
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

// shouldIncludeFile reports whether the file should be included in the processing
func shouldIncludeFile(fileName string) bool {
	if fileName[0] == '.' {
		return false
	}
	fileExt := filepath.Ext(fileName)
	return fileExt != ".partial"
}
