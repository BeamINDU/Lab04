// constants/dataTypes.ts
export const POSTGRESQL_DATA_TYPES = [
  // Numeric Types
  'SMALLINT',
  'INTEGER', 
  'BIGINT',
  'DECIMAL',
  'NUMERIC',
  'REAL',
  'DOUBLE PRECISION',
  'SERIAL',
  'BIGSERIAL',

  // Character Types
  'VARCHAR',
  'CHAR',
  'TEXT',

  // Date/Time Types
  'DATE',
  'TIME',
  'TIMESTAMP',
  'TIMESTAMPTZ',
  'INTERVAL',

  // Boolean Type
  'BOOLEAN',

  // JSON Types
  'JSON',
  'JSONB',

  // Other Types
  'UUID',
  'BYTEA',
  'INET',
  'CIDR',
  'MACADDR',
  'ARRAY'
] as const;

export const DATA_TYPE_CATEGORIES = {
  numeric: ['SMALLINT', 'INTEGER', 'BIGINT', 'DECIMAL', 'NUMERIC', 'REAL', 'DOUBLE PRECISION', 'SERIAL', 'BIGSERIAL'],
  character: ['VARCHAR', 'CHAR', 'TEXT'],
  datetime: ['DATE', 'TIME', 'TIMESTAMP', 'TIMESTAMPTZ', 'INTERVAL'],
  boolean: ['BOOLEAN'],
  json: ['JSON', 'JSONB'],
  other: ['UUID', 'BYTEA', 'INET', 'CIDR', 'MACADDR', 'ARRAY']
} as const;