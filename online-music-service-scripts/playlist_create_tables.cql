CREATE KEYSPACE IF NOT EXISTS playlist WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

USE playlist;

create table users (username text primary key, password text, playlist_names set<text>);

create table track_by_id (track text, artist text, track_id UUID, track_length_in_seconds int, genre text, music_file text, primary key (track_id));

create table track_by_artist (track text, artist text, track_id UUID, track_length_in_seconds int, genre text,music_file text, starred boolean, primary key (artist, track, track_id));

create table track_by_genre (track text, artist text, track_id UUID, track_length_in_seconds int, genre text,music_file text, starred boolean, primary key (genre, artist, track, track_id));

create table artists_by_first_letter (first_letter text, artist text, primary key (first_letter, artist));

create table playlist_tracks (username text, playlist_name text, sequence_no timestamp, artist text, track_name text, genre text, track_length_in_seconds int, track_id UUID, primary key ((username, playlist_name), sequence_no ));

create table statistics (counter_name text PRIMARY KEY, counter_value counter);






