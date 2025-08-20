// types/import.ts
export interface ImportLogUser {
  id: string;
  name: string;
  email?: string;
}

export interface ImportLogCompany {
  id: string;
  name: string;
  code: string;
}

export interface ImportLogItem {
  id: string;
  fileName: string;
  fileType: string;
  tableName: string;
  status: 'COMPLETED' | 'PARTIAL' | 'FAILED' | 'PROCESSING';
  totalRows: number;
  successRows: number;
  errorRows: number;
  createdAt: string;
  updatedAt: string;
  user: ImportLogUser;
  company?: ImportLogCompany;
  errors?: any;
}

export interface PaginationInfo {
  page: number;
  limit: number;
  total: number;
  pages: number;
}

export interface ImportHistoryResponse {
  success: boolean;
  data: ImportLogItem[];
  pagination: PaginationInfo;
  error?: string;
}

export interface ImportStatsOverview {
  totalImports: number;
  successfulImports: number;
  failedImports: number;
  partialImports: number;
  successRate: number;
  totalRowsProcessed: number;
  successfulRows: number;
  errorRows: number;
}

export interface ImportDailyStat {
  date: string;
  imports: number;
  totalRows: number;
  successRows: number;
  errorRows: number;
}

export interface ImportTableStat {
  tableName: string;
  imports: number;
  totalRows: number;
}

export interface ImportStatsResponse {
  success: boolean;
  data: {
    overview: ImportStatsOverview;
    dailyStats: ImportDailyStat[];
    topTables: ImportTableStat[];
  };
  period: string;
  error?: string;
}

export type ImportStatus = 'COMPLETED' | 'PARTIAL' | 'FAILED' | 'PROCESSING';
export type FileType = 'csv' | 'excel' | 'xlsx' | 'json';

// Utility types for form data
export interface ImportFormData {
  file: File;
  tableName: string;
  schema?: string;
  options?: {
    hasHeader?: boolean;
    delimiter?: string;
    encoding?: string;
    skipRows?: number;
    onDuplicate?: 'skip' | 'update' | 'error';
  };
}

export interface ColumnMapping {
  csvColumn: string;
  dbColumn: string;
  dataType: string;
  required: boolean;
  defaultValue?: string;
}

export interface ImportValidationResult {
  isValid: boolean;
  errors: string[];
  warnings: string[];
  previewData?: any[];
  columnMappings?: ColumnMapping[];
}