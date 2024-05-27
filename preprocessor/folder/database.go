package preprocessor_folder

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	db "github.com/starlinglab/integrity-v2/database"
)

// initFileStatusTableIfNotExists creates the file_status table if it does not exist
func initFileStatusTableIfNotExists(connPool *pgxpool.Pool) error {
	_, err := connPool.Exec(
		db.GetDatabaseContext(),
		FILE_STATUS_TABLE,
	)
	if err != nil {
		return err
	}
	return nil
}

// initFileStatusTableIfNotExists creates the project_metadata table if it does not exist
func initProjectDataTableIfNotExists(connPool *pgxpool.Pool) error {
	_, err := connPool.Exec(
		db.GetDatabaseContext(),
		PROJECT_METADATA_TABLE,
	)
	if err != nil {
		return err
	}
	return nil
}

// initDbTableIfNotExists initializes the database tables if they do not exist
func initDbTableIfNotExists(connPool *pgxpool.Pool) error {
	err := initFileStatusTableIfNotExists(connPool)
	if err != nil {
		return err
	}
	err = initProjectDataTableIfNotExists(connPool)
	if err != nil {
		return err
	}
	return nil
}

// ProjectQueryResult represents the result of a project metadata query
type ProjectQueryResult struct {
	ProjectId        *string
	ProjectPath      *string
	AuthorType       *string
	AuthorName       *string
	AuthorIdentifier *string
}

// queryAllProjects queries all project metadata from the database
func queryAllProjects(connPool *pgxpool.Pool) ([]ProjectQueryResult, error) {
	var result []ProjectQueryResult
	rows, err := connPool.Query(
		db.GetDatabaseContext(),
		"SELECT project_id, project_path, author_type, author_name, author_identifier FROM project_metadata;",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var row ProjectQueryResult
		err := rows.Scan(&row.ProjectId, &row.ProjectPath, &row.AuthorType, &row.AuthorName, &row.AuthorIdentifier)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, nil
}

// FileQueryResult represents the result of a file query
type FileQueryResult struct {
	Status       *string
	Cid          *string
	ErrorMessage *string
}

// queryIfFileExists checks if a file is already found in the database
func queryIfFileExists(connPool *pgxpool.Pool, filePath string) (*FileQueryResult, error) {
	var result FileQueryResult
	err := connPool.QueryRow(
		db.GetDatabaseContext(),
		"SELECT status, cid, error FROM file_status WHERE file_path = $1;",
		filePath,
	).Scan(&result.Status, &result.Cid, &result.ErrorMessage)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &result, nil
}

// setFileStatusFound add a file to database with status found
func setFileStatusFound(connPool *pgxpool.Pool, filePath string) error {
	_, err := connPool.Exec(
		db.GetDatabaseContext(),
		"INSERT INTO file_status (file_path, status, created_at, updated_at) VALUES ($1, $2, $3, $4);",
		filePath,
		FileStatusFound,
		time.Now().UTC(),
		time.Now().UTC(),
	)
	return err
}

// setFileStatusUploading sets the status of a file to uploading
func setFileStatusUploading(connPool *pgxpool.Pool, filePath string, sha256 string) error {
	_, err := connPool.Exec(
		db.GetDatabaseContext(),
		"UPDATE file_status SET status = $1, sha256 = $2, updated_at = $3 WHERE file_path = $4;",
		FileStatusUploading,
		sha256,
		time.Now().UTC(),
		filePath,
	)
	return err
}

// setFileStatusDone sets the status of a file to done with cid
func setFileStatusDone(connPool *pgxpool.Pool, filePath string, cid string) error {
	_, err := connPool.Exec(
		db.GetDatabaseContext(),
		"UPDATE file_status SET status = $1, cid = $2, updated_at = $3 WHERE file_path = $4;",
		FileStatusSuccess,
		cid,
		time.Now().UTC(),
		filePath,
	)
	return err
}

// setFileStatusError sets the status of a file to error with the error message
func setFileStatusError(connPool *pgxpool.Pool, filePath string, errorMessage string) error {
	_, err := connPool.Exec(
		db.GetDatabaseContext(),
		"UPDATE file_status SET status = $1, error = $2, updated_at = $3 WHERE file_path = $4;",
		FileStatusError,
		errorMessage,
		time.Now().UTC(),
		filePath,
	)
	if err != nil {
		fmt.Println("error setting file status to error:", err)
	}
	return err
}