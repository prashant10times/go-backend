-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create custom types
CREATE TYPE status AS ENUM ('ACTIVE', 'INACTIVE');
CREATE TYPE filter_type AS ENUM ('BASIC', 'ADVANCED');
CREATE TYPE parameter_type AS ENUM ('BASIC', 'ADVANCED');

-- Create users table
CREATE TABLE "User" (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    password_hash VARCHAR(255),
    status status DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create apis table
CREATE TABLE "Api" (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    api_name VARCHAR(255) UNIQUE NOT NULL,
    slug VARCHAR(255) UNIQUE NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create api_tokens table
CREATE TABLE "ApiToken" (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES "User"(id) ON DELETE CASCADE,
    token VARCHAR(500) UNIQUE NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP,
    refreshed_at TIMESTAMP
);

-- Create user_api_access table
CREATE TABLE "UserApiAccess" (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES "User"(id) ON DELETE CASCADE,
    api_id UUID NOT NULL REFERENCES "Api"(id) ON DELETE CASCADE,
    daily_limit INTEGER DEFAULT 100,
    has_access BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, api_id)
);

-- Create api_usage_log table
CREATE TABLE "ApiUsageLog" (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES "User"(id) ON DELETE CASCADE,
    api_id UUID NOT NULL REFERENCES "Api"(id) ON DELETE CASCADE,
    endpoint VARCHAR(500) NOT NULL,
    payload JSONB,
    ip_address VARCHAR(45) NOT NULL,
    status_code INTEGER,
    error_message VARCHAR(500),
    api_response_time FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create api_filter table
CREATE TABLE "ApiFilter" (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    api_id UUID NOT NULL REFERENCES "Api"(id) ON DELETE CASCADE,
    filter_type filter_type NOT NULL,
    filter_name VARCHAR(255) NOT NULL,
    is_paid BOOLEAN DEFAULT false,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
CREATE TABLE "ApiParameter" (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    api_id UUID NOT NULL REFERENCES "Api"(id) ON DELETE CASCADE,
    parameter_name VARCHAR(255) NOT NULL,
    parameter_type parameter_type DEFAULT 'BASIC',
    is_paid BOOLEAN DEFAULT false,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create user_parameter_access table
CREATE TABLE "UserParameterAccess" (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES "User"(id) ON DELETE CASCADE,
    parameter_id UUID NOT NULL REFERENCES "ApiParameter"(id) ON DELETE CASCADE,
    has_access BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, parameter_id)
);

-- Create indexes for better performance
CREATE INDEX CONCURRENTLY idx_user_api_access_user_api ON "UserApiAccess"(user_id, api_id);
CREATE INDEX CONCURRENTLY idx_user_api_access_user_has_access ON "UserApiAccess"(user_id, has_access);
CREATE INDEX CONCURRENTLY idx_api_usage_log_user_api_created ON "ApiUsageLog"(user_id, api_id, created_at);
CREATE INDEX CONCURRENTLY idx_api_usage_log_created ON "ApiUsageLog"(created_at);
CREATE INDEX CONCURRENTLY idx_api_usage_log_user_api_status ON "ApiUsageLog"(user_id, api_id, status_code);
CREATE INDEX CONCURRENTLY idx_api_filter_api_name ON "ApiFilter"(api_id, filter_name);
CREATE INDEX CONCURRENTLY idx_api_filter_api_active ON "ApiFilter"(api_id, is_active);
CREATE INDEX CONCURRENTLY idx_user_filter_access_user_filter ON "UserFilterAccess"(user_id, filter_id);
CREATE INDEX CONCURRENTLY idx_user_filter_access_user_has_access ON "UserFilterAccess"(user_id, has_access);
CREATE INDEX CONCURRENTLY idx_api_parameter_api_name ON "ApiParameter"(api_id, parameter_name);
CREATE INDEX CONCURRENTLY idx_api_parameter_api_active ON "ApiParameter"(api_id, is_active);
CREATE INDEX CONCURRENTLY idx_api_parameter_api_type ON "ApiParameter"(api_id, parameter_type);
CREATE INDEX CONCURRENTLY idx_user_parameter_access_user_parameter ON "UserParameterAccess"(user_id, parameter_id);
CREATE INDEX CONCURRENTLY idx_user_parameter_access_user_has_access ON "UserParameterAccess"(user_id, has_access);
