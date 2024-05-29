package preprocessor_folder

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/starlinglab/integrity-v2/config"
	"github.com/starlinglab/integrity-v2/database"
)

// scanSyncDirectory scans a path under the sync directory and returns a list of files
func scanSyncDirectory(subPath string) (fileList []string, dirList []string, err error) {
	scanRoot := config.GetConfig().FolderPreprocessor.SyncFolderRoot
	if scanRoot == "" {
		scanRoot = "."
	}
	scanPath := filepath.Join(scanRoot, subPath)
	fmt.Println("Scanning: " + scanPath)
	err = filepath.Walk(scanPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			dirList = append(dirList, path)
		} else if checkShouldIncludeFile(info) {
			fileList = append(fileList, path)
			fmt.Println("Found: " + path)
			return nil
		}
		return nil
	})
	return fileList, dirList, err
}

// watchLoop watches for file changes in a directory and checks if it should be handled
func watchLoop(w *fsnotify.Watcher, pgPool *pgxpool.Pool, dirPathToProject map[string]ProjectQueryResult) {
	for {
		select {
		case event, ok := <-w.Events:
			if !ok {
				return
			}
			if event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) {
				filePath := event.Name
				file, err := os.Open(filePath)
				if err != nil {
					// File may be moved away for fsnotify.Rename
					continue
				}
				defer file.Close()
				fileInfo, err := file.Stat()
				if err != nil {
					fmt.Println("error getting file info:", err)
					continue
				}
				if checkShouldIncludeFile(fileInfo) {
					project := dirPathToProject[filepath.Dir(filePath)]
					cid, err := handleNewFile(pgPool, filePath, &project)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("File %s uploaded to webhook with CID %s\n", filePath, cid)
					}
				}
			}
		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			fmt.Println("error:", err)
		}
	}
}

// Scan the sync directory and watch for file changes
func Run(args []string) error {
	pgPool, err := database.GetDatabaseConnectionPool()
	if err != nil {
		return err
	}
	defer database.CloseDatabaseConnectionPool()
	err = initDbTableIfNotExists(pgPool)
	if err != nil {
		return err
	}

	projects, err := queryAllProjects(pgPool)
	if err != nil {
		return err
	}

	dirPathToProject := map[string]ProjectQueryResult{}
	var dirPaths []string

	for _, project := range projects {
		projectPath := *project.ProjectPath
		fileList, dirList, err := scanSyncDirectory(projectPath)
		if err != nil {
			fmt.Println(err)
		}
		for _, filePath := range fileList {
			cid, err := handleNewFile(pgPool, filePath, &project)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Printf("File %s uploaded to webhook with CID %s\n", filePath, cid)
			}
		}
		for _, dirPath := range dirList {
			dirPaths = append(dirPaths, dirPath)
			dirPathToProject[dirPath] = project
		}
	}

	// Init directory watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	go watchLoop(watcher, pgPool, dirPathToProject)

	for _, dirPath := range dirPaths {
		err = watcher.Add(dirPath)
		if err != nil {
			return err
		}
		fmt.Println("Watching folder changes: " + dirPath)
	}

	// Block main goroutine forever.
	<-make(chan struct{})
	return nil
}
