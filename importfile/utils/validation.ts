import { Column } from '../types/schema';

export class ValidationUtils {
  static validateSchemaName(name: string): { isValid: boolean; error?: string } {
    if (!name.trim()) {
      return { isValid: false, error: 'ชื่อ Schema ไม่สามารถเป็นค่าว่างได้' };
    }

    if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(name)) {
      return { isValid: false, error: 'ชื่อ Schema ต้องขึ้นต้นด้วยตัวอักษร และประกอบด้วยตัวอักษร ตัวเลข หรือ _ เท่านั้น' };
    }

    if (['public', 'information_schema', 'pg_catalog', 'pg_toast'].includes(name.toLowerCase())) {
      return { isValid: false, error: 'ไม่สามารถใช้ชื่อ Schema ที่สงวนไว้ของระบบได้' };
    }

    return { isValid: true };
  }

  static validateTableName(name: string): { isValid: boolean; error?: string } {
    if (!name.trim()) {
      return { isValid: false, error: 'ชื่อตารางไม่สามารถเป็นค่าว่างได้' };
    }

    if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(name)) {
      return { isValid: false, error: 'ชื่อตารางต้องขึ้นต้นด้วยตัวอักษร และประกอบด้วยตัวอักษร ตัวเลข หรือ _ เท่านั้น' };
    }

    if (name.length > 63) {
      return { isValid: false, error: 'ชื่อตารางยาวเกิน 63 ตัวอักษร' };
    }

    return { isValid: true };
  }

  static validateColumn(column: Column): { isValid: boolean; errors: string[] } {
    const errors: string[] = [];

    // Validate column name
    if (!column.name.trim()) {
      errors.push('ชื่อ Column ไม่สามารถเป็นค่าว่างได้');
    } else if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(column.name)) {
      errors.push('ชื่อ Column ต้องขึ้นต้นด้วยตัวอักษร และประกอบด้วยตัวอักษร ตัวเลข หรือ _ เท่านั้น');
    }

    // Validate data type specific rules
    if (column.type === 'VARCHAR' && (!column.length || column.length <= 0)) {
      errors.push('VARCHAR ต้องระบุความยาวที่มากกว่า 0');
    }

    if (column.type === 'VARCHAR' && column.length && column.length > 65535) {
      errors.push('VARCHAR ความยาวไม่สามารถเกิน 65535 ตัวอักษร');
    }

    // Validate default value format
    if (column.defaultValue && column.type === 'VARCHAR' && !column.defaultValue.startsWith("'")) {
      errors.push('Default value สำหรับ VARCHAR ต้องอยู่ในเครื่องหมาย single quote');
    }

    return {
      isValid: errors.length === 0,
      errors
    };
  }

  static validateColumns(columns: Column[]): { isValid: boolean; errors: string[] } {
    const errors: string[] = [];

    if (columns.length === 0) {
      errors.push('ต้องมีอย่างน้อย 1 Column');
      return { isValid: false, errors };
    }

    // Check for primary key
    const primaryKeys = columns.filter(col => col.isPrimary);
    if (primaryKeys.length === 0) {
      errors.push('ต้องมี Primary Key อย่างน้อย 1 Column');
    } else if (primaryKeys.length > 1) {
      errors.push('สามารถมี Primary Key ได้เพียง 1 Column เท่านั้น');
    }

    // Check for duplicate column names (แก้ไขปัญหา Set iteration)
    const columnNames = columns.map(col => col.name.toLowerCase()).filter(name => name);
    const duplicates: string[] = [];
    
    // ใช้ for loop แทน Set เพื่อหา duplicates
    for (let i = 0; i < columnNames.length; i++) {
      for (let j = i + 1; j < columnNames.length; j++) {
        if (columnNames[i] === columnNames[j] && duplicates.indexOf(columnNames[i]) === -1) {
          duplicates.push(columnNames[i]);
        }
      }
    }
    
    if (duplicates.length > 0) {
      errors.push(`มีชื่อ Column ซ้ำกัน: ${duplicates.join(', ')}`);
    }

    // Validate each column
    columns.forEach((column, index) => {
      const validation = this.validateColumn(column);
      if (!validation.isValid) {
        validation.errors.forEach(error => {
          errors.push(`Column ${index + 1}: ${error}`);
        });
      }
    });

    return {
      isValid: errors.length === 0,
      errors
    };
  }
}