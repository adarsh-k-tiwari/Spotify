CREATE TABLE tracks (
    track_id VARCHAR PRIMARY KEY,            -- Unique identifier for each track
    track_name VARCHAR,                      -- Name of the track
    album_id VARCHAR,                        -- Foreign key to albums
    album_name VARCHAR,                      -- Name of the album
    artist_id VARCHAR,                       -- Foreign key to artists
    artist_name VARCHAR,                     -- Name of the artist
    duration_ms INT,                         -- Duration of the track in milliseconds
    popularity INT,                          -- Popularity score of the track
    release_date DATE,                       -- Release date of the album
    explicit BOOLEAN,                        -- Whether the track is explicit
    added_at TIMESTAMP,                      -- Timestamp when the track was added to the playlist
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Record creation timestamp
);

CREATE TABLE artists (
    artist_id VARCHAR PRIMARY KEY,           -- Unique identifier for each artist
    artist_name VARCHAR,                     -- Name of the artist
    genres ARRAY,                            -- Array of genres associated with the artist
    followers INT,                           -- Number of followers of the artist
    popularity INT,                          -- Popularity score of the artist
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Record creation timestamp
);

CREATE TABLE albums (
    album_id VARCHAR PRIMARY KEY,            -- Unique identifier for each album
    album_name VARCHAR,                      -- Name of the album
    release_date DATE,                       -- Release date of the album
    total_tracks INT,                        -- Total number of tracks in the album
    album_type VARCHAR,                      -- Type of album (e.g., album, single, compilation)
    artist_id VARCHAR,                       -- Foreign key to artists
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Record creation timestamp
);

CREATE TABLE playlists (
    playlist_id VARCHAR PRIMARY KEY,         -- Unique identifier for each playlist
    playlist_name VARCHAR,                   -- Name of the playlist
    description VARCHAR,                     -- Description of the playlist
    owner_id VARCHAR,                        -- User ID of the playlist owner
    followers INT,                           -- Number of followers of the playlist
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Record creation timestamp
);

CREATE TABLE spotify_tracks (
    playlist_id VARCHAR,                     -- Foreign key to playlists
    track_id VARCHAR,                        -- Foreign key to tracks
    added_at TIMESTAMP,                      -- Timestamp when the track was added to the playlist
    PRIMARY KEY (playlist_id, track_id)
);

CREATE TABLE lyrics (
    track_id VARCHAR PRIMARY KEY,            -- Foreign key to tracks
    lyrics TEXT,                             -- Lyrics of the track
    sentiment VARCHAR,                       -- Sentiment analysis result (e.g., positive, negative, neutral)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Record creation timestamp
);

CREATE TABLE track_analytics (
    track_id VARCHAR PRIMARY KEY,            -- Foreign key to tracks
    danceability FLOAT,                      -- Danceability score
    energy FLOAT,                            -- Energy score
    key INT,                                 -- Key of the track (e.g., C, D, etc.)
    loudness FLOAT,                          -- Loudness in decibels
    speechiness FLOAT,                       -- Speechiness score
    acousticness FLOAT,                      -- Acousticness score
    instrumentalness FLOAT,                  -- Instrumentalness score
    liveness FLOAT,                          -- Liveness score
    valence FLOAT,                           -- Valence score (musical positivity)
    tempo FLOAT,                             -- Tempo in beats per minute (BPM)
    time_signature INT,                      -- Time signature (e.g., 4/4)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Record creation timestamp
);
