-- The user_version should match the "000X" from the file name
-- Ex: 0001_create_notebooks_table should have a user_verison of 1
PRAGMA user_version=2;

-- Create the initial table to store streams
CREATE TABLE streams (
  id VARCHAR(16) UNIQUE NOT NULL,
  org_id VARCHAR(16) NOT NULL,
  name TEXT NOT NULL,
  description TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,

  PRIMARY KEY ("org_id", "name")
);

-- Create an index on stream id to support fast queries on streams by id
CREATE INDEX idx_stream_id ON streams (id);

-- Create the initial table to store annotations
CREATE TABLE annotations (
  id VARCHAR(16) UNIQUE NOT NULL,
  org_id VARCHAR(16) NOT NULL,
  stream_id VARCHAR(16) NOT NULL,
  stream TEXT NOT NULL,
  summary TEXT NOT NULL,
  message TEXT NOT NULL,
  stickers TEXT NOT NULL,
  duration TEXT NOT NULL,
  lower TIMESTAMP NOT NULL,
  upper TIMESTAMP NOT NULL,
    
  PRIMARY KEY ("id", "org_id"),
  FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE
);

-- Create indexes for stream_id and stickers to support fast queries
CREATE INDEX idx_annotations_stream ON annotations (stream_id);
CREATE INDEX idx_annotations_stickers ON annotations (stickers);
