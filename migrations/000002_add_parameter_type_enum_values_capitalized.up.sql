-- Add new enum values to ParameterType (capitalized enum type used by ApiParameter table)
ALTER TYPE "ParameterType" ADD VALUE IF NOT EXISTS 'INSIGHTS';
ALTER TYPE "ParameterType" ADD VALUE IF NOT EXISTS 'AUDIENCE';

