-- Create custom types
CREATE TYPE status AS ENUM ('ACTIVE', 'INACTIVE');
CREATE TYPE filter_type AS ENUM ('BASIC', 'ADVANCED');
CREATE TYPE parameter_type AS ENUM ('BASIC', 'ADVANCED');

-- Create users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    status status DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create apis table
CREATE TABLE apis (
    id SERIAL PRIMARY KEY,
    api_name VARCHAR(255) NOT NULL,
    api_description TEXT,
    endpoint_url VARCHAR(500) NOT NULL,
    method VARCHAR(10) NOT NULL DEFAULT 'GET',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create api_tokens table
CREATE TABLE api_tokens (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    api_id INTEGER REFERENCES apis(id) ON DELETE CASCADE,
    token VARCHAR(500) UNIQUE NOT NULL,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create user_api_access table
CREATE TABLE user_api_access (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    api_id INTEGER REFERENCES apis(id) ON DELETE CASCADE,
    has_access BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, api_id)
);

-- Create api_usage_log table
CREATE TABLE api_usage_log (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    api_id INTEGER REFERENCES apis(id) ON DELETE CASCADE,
    request_data JSONB,
    response_data JSONB,
    status_code INTEGER,
    response_time_ms INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create api_filter table
CREATE TABLE api_filter (
    id SERIAL PRIMARY KEY,
    api_id INTEGER REFERENCES apis(id) ON DELETE CASCADE,
    filter_name VARCHAR(255) NOT NULL,
    filter_description TEXT,
    filter_type filter_type DEFAULT 'BASIC',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create user_filter_access table
CREATE TABLE user_filter_access (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    filter_id INTEGER REFERENCES api_filter(id) ON DELETE CASCADE,
    has_access BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, filter_id)
);

-- Create api_parameter table
CREATE TABLE api_parameter (
    id SERIAL PRIMARY KEY,
    api_id INTEGER REFERENCES apis(id) ON DELETE CASCADE,
    parameter_name VARCHAR(255) NOT NULL,
    parameter_description TEXT,
    parameter_type parameter_type DEFAULT 'BASIC',
    is_required BOOLEAN DEFAULT false,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create user_parameter_access table
CREATE TABLE user_parameter_access (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    parameter_id INTEGER REFERENCES api_parameter(id) ON DELETE CASCADE,
    has_access BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, parameter_id)
);

-- Create indexes for better performance
CREATE INDEX CONCURRENTLY idx_user_api_access_user_api ON user_api_access(user_id, api_id);
CREATE INDEX CONCURRENTLY idx_user_api_access_user_has_access ON user_api_access(user_id, has_access);
CREATE INDEX CONCURRENTLY idx_api_usage_log_user_api_created ON api_usage_log(user_id, api_id, created_at);
CREATE INDEX CONCURRENTLY idx_api_usage_log_created ON api_usage_log(created_at);
CREATE INDEX CONCURRENTLY idx_api_usage_log_user_api_status ON api_usage_log(user_id, api_id, status_code);
CREATE INDEX CONCURRENTLY idx_api_filter_api_name ON api_filter(api_id, filter_name);
CREATE INDEX CONCURRENTLY idx_api_filter_api_active ON api_filter(api_id, is_active);
CREATE INDEX CONCURRENTLY idx_user_filter_access_user_filter ON user_filter_access(user_id, filter_id);
CREATE INDEX CONCURRENTLY idx_user_filter_access_user_has_access ON user_filter_access(user_id, has_access);
CREATE INDEX CONCURRENTLY idx_api_parameter_api_name ON api_parameter(api_id, parameter_name);
CREATE INDEX CONCURRENTLY idx_api_parameter_api_active ON api_parameter(api_id, is_active);
CREATE INDEX CONCURRENTLY idx_api_parameter_api_type ON api_parameter(api_id, parameter_type);
CREATE INDEX CONCURRENTLY idx_user_parameter_access_user_parameter ON user_parameter_access(user_id, parameter_id);
CREATE INDEX CONCURRENTLY idx_user_parameter_access_user_has_access ON user_parameter_access(user_id, has_access);
