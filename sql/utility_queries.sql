CREATE INDEX idx_game_developers_game_id ON game_developers(game_id);
CREATE INDEX idx_game_publishers_game_id ON game_publishers(game_id);
CREATE INDEX idx_game_genres_game_id ON game_genres(game_id);
CREATE INDEX idx_game_languages_game_id ON game_languages(game_id);
CREATE INDEX idx_crawl_state_appid ON crawl_state(game_appid);