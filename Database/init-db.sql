CREATE TABLE
    IF NOT EXISTS Users (
        uid SERIAL PRIMARY KEY,
        email VARCHAR(256) UNIQUE,
        passwd VARCHAR(256) NOT NULL,
        is_publisher BOOLEAN DEFAULT FALSE,
        is_admin BOOLEAN DEFAULT FALSE
    );

CREATE TABLE
    IF NOT EXISTS Wallets (
        uid INTEGER PRIMARY KEY REFERENCES users (uid) ON DELETE CASCADE,
        balance INTEGER DEFAULT 0
    );

CREATE TABLE
    IF NOT EXISTS games (
        gid SERIAL PRIMARY KEY,
        name VARCHAR(256) UNIQUE,
        description VARCHAR(256),
        price INTEGER NOT NULL,
        publisher INTEGER REFERENCES users (uid) ON DELETE CASCADE,
        status VARCHAR(256) NOT NULL
    );

CREATE TABLE
    IF NOT EXISTS purchases (
        pid SERIAL PRIMARY KEY,
        game_id INTEGER REFERENCES games (gid) ON DELETE CASCADE,
        user_id INTEGER REFERENCES users (uid) ON DELETE CASCADE,
        date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        hours_played INTEGER DEFAULT 0
    );
