use playlist;
copy track_by_id (track_id, genre, artist, track, track_length_in_seconds, music_file) FROM 'songs.csv' WITH DELIMITER = '|' AND HEADER=true;





