// index.ts - Export all schema management components
export { default as CompleteSchemaManagementSystem } from './CompleteSchemaManagementSystem';
export { default as SchemaManagement } from './SchemaManagement';
export { default as TableManagement } from './TableManagement';
export { default as FileImport } from './FileImport';
export { default as ImportHistory } from './ImportHistory';

// Export types
export type {
  SchemaInfo,
  TableInfo,
  DatabaseColumn,
  FilePreview,
  ImportResult,
  ImportOptions,
  ApiResponse,
  TabType,
  BaseComponentProps,
  SchemaManagementProps,
  TableManagementProps,
  FileImportProps
} from './types';

// Export utility functions
export {
  apiCall,
  validateIdentifier,
  validateColumns,
  createDefaultColumns,
  filterSchemas,
  formatFileSize,
  getFileType,
  generateTableNameFromFile,
  showSuccessToast,
  showErrorToast,
  showLoadingToast
} from './utils';