CREATE DATABASE steam_db;
USE steam_db;


CREATE TABLE IF NOT EXISTS games (
    game_id INT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    release_date DATE,
    windows_req TEXT,
    mac_req TEXT,
    linux_req TEXT,
    required_age INT DEFAULT 0,
    awards TEXT
);


CREATE TABLE IF NOT EXISTS developers (
    dev_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS publishers (
    pub_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS game_developers (
    game_id INT REFERENCES games(game_id),
    dev_id INT REFERENCES developers(dev_id),
    PRIMARY KEY (game_id, dev_id)
);

CREATE TABLE IF NOT EXISTS game_publishers (
    game_id INT REFERENCES games(game_id),
    pub_id INT REFERENCES publishers(pub_id),
    PRIMARY KEY (game_id, pub_id)
);


CREATE TABLE IF NOT EXISTS genres (
    genre_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS languages (
    lang_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS game_genres (
    game_id INT REFERENCES games(game_id),
    genre_id INT REFERENCES genres(genre_id),
    PRIMARY KEY (game_id, genre_id)
);

CREATE TABLE IF NOT EXISTS game_languages (
    game_id INT REFERENCES games(game_id),
    lang_id INT REFERENCES languages(lang_id),
    PRIMARY KEY (game_id, lang_id)
);

CREATE TABLE IF NOT EXISTS crawl_state (
    game_appid INT PRIMARY KEY,
    last_review_timestamp DATETIME NULL,
    last_price_timestamp DATETIME NULL,
    FOREIGN KEY (game_appid) REFERENCES games(game_id)
);


CREATE TABLE IF NOT EXISTS prices (
    price_id INT AUTO_INCREMENT PRIMARY KEY,
    game_id INT,
    price DECIMAL(10,2),
    discount INT,
    initial_price DECIMAL(10,2),
    timestamp DATETIME ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);