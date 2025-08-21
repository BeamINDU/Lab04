// types.ts - ไฟล์รวม type definitions สำหรับ schema management system

export interface SchemaInfo {
  name: string;
  description?: string;
  tables: TableInfo[];
}

export interface TableInfo {
  name: string;
  schema: string;
  comment?: string;
  columnCount: number;
  hasData: boolean;
}

export interface DatabaseColumn {
  name: string;
  type: string;
  length?: number;
  isPrimary?: boolean;
  isRequired?: boolean;
  isUnique?: boolean;
  defaultValue?: string;
  comment?: string;
}

export interface FilePreview {
  headers: string[];
  sampleData: any[];
  totalRows: number;
  fileName: string;
  fileType: string;
  suggestedColumns: DatabaseColumn[];
}

export interface ImportResult {
  success: boolean;
  totalRows: number;
  successRows: number;
  errorRows: number;
  errors: Array<{ row: number; error: string }>;
  executionTime: number;
}

export interface ImportOptions {
  createTable: boolean;
  truncateBeforeImport: boolean;
  skipErrors: boolean;
  batchSize: number;
}

export interface ApiResponse<T = any> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
}

// Tab types สำหรับการ navigation
export type TabType = 'schemas' | 'import' | 'history';

// Props interfaces สำหรับ components ต่างๆ
export interface BaseComponentProps {
  loading: boolean;
  onLoadingChange: (loading: boolean) => void;
  onRefresh: () => void;
}

export interface SchemaManagementProps extends BaseComponentProps {
  schemas: SchemaInfo[];
  selectedSchema: string;
  onSchemaSelect: (schema: string) => void;
  onSchemaCreate: () => void;
  onSchemaDelete: (schemaName: string) => void;
}

export interface TableManagementProps extends BaseComponentProps {
  schemas: SchemaInfo[];
  selectedSchema: string;
  onTableCreate: (
    schema: string,
    tableName: string,
    description: string,
    columns: DatabaseColumn[]
  ) => void;
  onTableDelete: (schema: string, tableName: string) => void;
}

export interface FileImportProps extends BaseComponentProps {
  schemas: SchemaInfo[];
  selectedSchema: string;
  onSchemaSelect: (schema: string) => void;
  onImportComplete: (result: ImportResult) => void;
}