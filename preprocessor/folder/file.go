package preprocessor_folder

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"github.com/gabriel-vasile/mimetype"
	"github.com/starlinglab/integrity-v2/config"
	"lukechampine.com/blake3"
)

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

	mtype, err := mimetype.DetectReader(tee)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"sha256":           hex.EncodeToString(sha.Sum(nil)),
		"md5":              hex.EncodeToString(md.Sum(nil)),
		"blake3":           hex.EncodeToString(blake.Sum(nil)),
		"mimetype":         mtype.String(),
		"fileSize":         fileInfo.Size(),
		"fileName":         fileInfo.Name(),
		"fileLastModified": fileInfo.ModTime(),
	}, nil
}

func handleNewFile(filePath string) (string, error) {
	metadata, err := getFileMetadata(filePath)
	if err != nil {
		return "", fmt.Errorf("error getting metadata for file %s: %v", filePath, err)
	}
	cid, err := postFileMetadataToWebHook(filePath, metadata)
	if err != nil {
		return "", fmt.Errorf("error posting metadata for file %s: %v", filePath, err)
	}
	return cid, nil
}

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
