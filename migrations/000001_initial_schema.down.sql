-- Drop indexes
DROP INDEX IF EXISTS idx_user_parameter_access_user_has_access;
DROP INDEX IF EXISTS idx_user_parameter_access_user_parameter;
DROP INDEX IF EXISTS idx_api_parameter_api_type;
DROP INDEX IF EXISTS idx_api_parameter_api_active;
DROP INDEX IF EXISTS idx_api_parameter_api_name;
DROP INDEX IF EXISTS idx_user_filter_access_user_has_access;
DROP INDEX IF EXISTS idx_user_filter_access_user_filter;
DROP INDEX IF EXISTS idx_api_filter_api_active;
DROP INDEX IF EXISTS idx_api_filter_api_name;
DROP INDEX IF EXISTS idx_api_usage_log_user_api_status;
DROP INDEX IF EXISTS idx_api_usage_log_created;
DROP INDEX IF EXISTS idx_api_usage_log_user_api_created;
DROP INDEX IF EXISTS idx_user_api_access_user_has_access;
DROP INDEX IF EXISTS idx_user_api_access_user_api;

-- Drop tables in reverse order of creation
DROP TABLE IF EXISTS user_parameter_access;
DROP TABLE IF EXISTS api_parameter;
DROP TABLE IF EXISTS user_filter_access;
DROP TABLE IF EXISTS api_filter;
DROP TABLE IF EXISTS api_usage_log;
DROP TABLE IF EXISTS user_api_access;
DROP TABLE IF EXISTS api_tokens;
DROP TABLE IF EXISTS apis;
DROP TABLE IF EXISTS users;

-- Drop custom types
DROP TYPE IF EXISTS parameter_type;
DROP TYPE IF EXISTS filter_type;
DROP TYPE IF EXISTS status;
