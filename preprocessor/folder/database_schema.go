package preprocessor_folder

// schema for file_status table
var FILE_STATUS_TABLE = `CREATE TABLE IF NOT EXISTS file_status (
		id BIGSERIAL PRIMARY KEY,
		file_path TEXT UNIQUE NOT NULL,
		sha256 TEXT,
		status TEXT NOT NULL,
		error TEXT,
		cid TEXT,
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_file_status_file_path ON file_status (file_path);
	CREATE INDEX IF NOT EXISTS idx_file_status_sha256 ON file_status (sha256);
	CREATE INDEX IF NOT EXISTS idx_file_status_status ON file_status (status);
`

var PROJECT_METADATA_TABLE = `CREATE TABLE IF NOT EXISTS project_metadata (
	id BIGSERIAL PRIMARY KEY,
	project_id TEXT UNIQUE NOT NULL,
	project_path TEXT UNIQUE NOT NULL,
	author_type TEXT,
	author_name TEXT,
	author_identifier TEXT
);
CREATE INDEX IF NOT EXISTS idx_project_metadata_project_id ON project_metadata (project_id);
`
