// utils.ts - Utility functions สำหรับ schema management system
import { toast } from 'react-hot-toast';
import type { ApiResponse, SchemaInfo, DatabaseColumn } from './types';

// การจัดการ API calls ด้วยการจัดการ error แบบ centralized
export const apiCall = async <T = any>(
  url: string, 
  options?: RequestInit
): Promise<ApiResponse<T>> => {
  try {
    const response = await fetch(url, {
      headers: {
        'Content-Type': 'application/json',
        ...options?.headers,
      },
      ...options,
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const result: ApiResponse<T> = await response.json();
    
    if (!result.success) {
      throw new Error(result.error || result.message || 'API call failed');
    }

    return result;
  } catch (error) {
    console.error('API call error:', error);
    throw error;
  }
};

// ฟังก์ชันสำหรับการ validate ชื่อ schema และ table
export const validateIdentifier = (name: string): { isValid: boolean; error?: string } => {
  if (!name.trim()) {
    return { isValid: false, error: 'ชื่อไม่ควรว่างเปล่า' };
  }

  // ตรวจสอบรูปแบบชื่อที่เป็น valid SQL identifier
  const validPattern = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
  if (!validPattern.test(name)) {
    return { 
      isValid: false, 
      error: 'ชื่อต้องขึ้นต้นด้วยตัวอักษรหรือ underscore และประกอบด้วยตัวอักษร ตัวเลข หรือ underscore เท่านั้น' 
    };
  }

  // ตรวจสอบ reserved words ของ PostgreSQL
  const reservedWords = [
    'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP',
    'TABLE', 'INDEX', 'VIEW', 'FUNCTION', 'PROCEDURE', 'TRIGGER', 'DATABASE',
    'SCHEMA', 'USER', 'ROLE', 'GRANT', 'REVOKE', 'BEGIN', 'COMMIT', 'ROLLBACK'
  ];
  
  if (reservedWords.includes(name.toUpperCase())) {
    return { 
      isValid: false, 
      error: 'ชื่อที่ใช้เป็น reserved word ของ SQL ไม่สามารถใช้ได้' 
    };
  }

  return { isValid: true };
};

// ฟังก์ชันสำหรับการ validate columns
export const validateColumns = (columns: DatabaseColumn[]): { isValid: boolean; error?: string } => {
  if (columns.length === 0) {
    return { isValid: false, error: 'ต้องมีอย่างน้อย 1 column' };
  }

  // ตรวจสอบชื่อ column ซ้ำ
  const columnNames = columns.map(col => col.name.toLowerCase());
  const duplicateNames = columnNames.filter((name, index) => columnNames.indexOf(name) !== index);
  
  if (duplicateNames.length > 0) {
    return { 
      isValid: false, 
      error: `มี column ชื่อซ้ำ: ${duplicateNames.join(', ')}` 
    };
  }

  // ตรวจสอบ primary key
  const primaryKeys = columns.filter(col => col.isPrimary);
  if (primaryKeys.length === 0) {
    return { 
      isValid: false, 
      error: 'ต้องมี primary key อย่างน้อย 1 column' 
    };
  }

  // ตรวจสอบชื่อ column แต่ละตัว
  for (const column of columns) {
    const validation = validateIdentifier(column.name);
    if (!validation.isValid) {
      return { 
        isValid: false, 
        error: `Column "${column.name}": ${validation.error}` 
      };
    }
  }

  return { isValid: true };
};

// ฟังก์ชันสำหรับการสร้าง default columns
export const createDefaultColumns = (): DatabaseColumn[] => [
  { 
    name: 'id', 
    type: 'SERIAL', 
    isPrimary: true, 
    isRequired: true, 
    comment: 'Primary key' 
  },
  { 
    name: 'created_at', 
    type: 'TIMESTAMP', 
    isRequired: true, 
    defaultValue: 'NOW()', 
    comment: 'Created timestamp' 
  },
  { 
    name: 'updated_at', 
    type: 'TIMESTAMP', 
    isRequired: true, 
    defaultValue: 'NOW()', 
    comment: 'Updated timestamp' 
  }
];

// ฟังก์ชันสำหรับการ filter และ search schemas
export const filterSchemas = (
  schemas: SchemaInfo[], 
  searchTerm: string, 
  filterSchema: string = ''
): SchemaInfo[] => {
  return schemas.filter(schema => {
    const matchesSearch = searchTerm === '' || 
      schema.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      schema.description?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      schema.tables.some(table => 
        table.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
        table.comment?.toLowerCase().includes(searchTerm.toLowerCase())
      );
    
    const matchesFilter = filterSchema === '' || schema.name === filterSchema;
    
    return matchesSearch && matchesFilter;
  });
};

// ฟังก์ชันสำหรับการ format file size
export const formatFileSize = (bytes: number): string => {
  if (bytes === 0) return '0 Bytes';
  
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

// ฟังก์ชันสำหรับการ detect file type จาก extension
export const getFileType = (fileName: string): string => {
  const extension = fileName.split('.').pop()?.toLowerCase();
  
  switch (extension) {
    case 'csv':
      return 'CSV';
    case 'xlsx':
    case 'xls':
      return 'Excel';
    case 'json':
      return 'JSON';
    case 'txt':
      return 'Text';
    default:
      return 'Unknown';
  }
};

// ฟังก์ชันสำหรับการแสดง toast messages ด้วย consistent styling
export const showSuccessToast = (message: string) => {
  toast.success(message, {
    duration: 4000,
    position: 'top-right',
  });
};

export const showErrorToast = (message: string) => {
  toast.error(message, {
    duration: 5000,
    position: 'top-right',
  });
};

export const showLoadingToast = (message: string) => {
  return toast.loading(message, {
    position: 'top-right',
  });
};

// ฟังก์ชันสำหรับการ generate table name จาก file name
export const generateTableNameFromFile = (fileName: string): string => {
  return fileName
    .split('.')[0] // ลบ extension
    .toLowerCase() // แปลงเป็น lowercase
    .replace(/[^a-z0-9_]/g, '_') // แทนที่ special characters ด้วย underscore
    .replace(/_{2,}/g, '_') // แทนที่ underscore ที่ซ้ำด้วย underscore เดียว
    .replace(/^_+|_+$/g, ''); // ลบ underscore ที่ต้นและท้าย
};