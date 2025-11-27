-- Run this in your Postgres database to create dummy data

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO users (name, email, status) VALUES
('Alice Johnson', 'ALICE@EXAMPLE.COM', 'active'),
('Bob Smith', 'bob.smith@example.com', 'inactive'),
('Charlie Brown', 'CHARLIE@EXAMPLE.COM', 'active'),
('Diana Prince', 'diana@example.com', 'active'),
('Evan Wright', 'evan@example.com', 'suspended');
