export interface Company {
  code: string;
  name: string;
  dbName: string;
}

export interface Schema {
  name: string;
  type: 'default' | 'custom';
  description?: string;
  tables: string[];
  tableDetails?: TableInfo[];
  createdAt: string;
}

export interface TableInfo {
  name: string;
  comment?: string;
}

export interface Column {
  name: string;
  type: string;
  length?: number;
  isPrimary: boolean;
  isRequired: boolean;
  isUnique: boolean;
  defaultValue: string;
  references?: {
    table: string;
    column: string;
  };
}

export interface TableStructure {
  name: string;
  schema: string;
  columns: DatabaseColumn[];
  constraints: DatabaseConstraint[];
  indexes: DatabaseIndex[];
}

export interface DatabaseColumn {
  column_name: string;
  data_type: string;
  character_maximum_length?: number;
  is_nullable: 'YES' | 'NO';
  column_default?: string;
  is_identity?: 'YES' | 'NO';
  identity_generation?: string;
}

export interface DatabaseConstraint {
  constraint_name: string;
  constraint_type: string;
  column_name: string;
  foreign_table_name?: string;
  foreign_column_name?: string;
}

export interface DatabaseIndex {
  indexname: string;
  indexdef: string;
}

export interface ApiResponse<T = any> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
}

export interface SchemaApiResponse extends ApiResponse {
  schemas?: Schema[];
  company?: string;
}

export interface TableApiResponse extends ApiResponse {
  table?: TableStructure;
}