// lib/services/FileImportService.ts - Fixed Data Type Detection
import { DatabaseService, DatabaseColumn, ImportResult } from './DatabaseService';
import * as fs from 'fs';
import * as path from 'path';
import csv from 'csv-parser';
import * as XLSX from 'xlsx';

export interface ImportOptions {
  filePath: string;
  fileName: string;
  mimeType: string;
  schema: string;
  tableName: string;
  createTable: boolean;
  truncateBeforeImport: boolean;
  skipErrors: boolean;
  batchSize: number;
}

export interface FilePreview {
  headers: string[];
  sampleData: any[];
  totalRows: number;
  fileName: string;
  fileType: string;
  suggestedColumns: DatabaseColumn[];
}

export interface ImportOptionsWithCustomColumns extends ImportOptions {
  customColumns?: DatabaseColumn[];
}

/**
 * Enhanced File Import Service with Fixed Data Type Detection
 */
export class FileImportService {
  private dbService: DatabaseService;

  constructor(dbService: DatabaseService) {
    this.dbService = dbService;
  }

  /**
   * Preview ไฟล์ก่อนการ import - อัพเดทเพื่อรองรับ architecture ใหม่
   */
  async previewFile(filePath: string, fileName: string, mimeType: string): Promise<FilePreview> {
    try {
      console.log(`🔍 Previewing file: ${fileName} (${mimeType})`);
      
      // ตรวจสอบว่าไฟล์มีอยู่จริง
      if (!fs.existsSync(filePath)) {
        throw new Error('File not found for preview');
      }

      const fileType = this.getFileType(mimeType, fileName);
      console.log(`📄 Detected file type: ${fileType}`);
      
      // ใช้ method ที่เหมาะสมตามประเภทไฟล์
      switch (fileType) {
        case 'csv':
          return await this.previewCSV(filePath, fileName);
        case 'excel':
          return await this.previewExcel(filePath, fileName);
        case 'json':
          return await this.previewJSON(filePath, fileName);
        case 'txt':
          return await this.previewTXT(filePath, fileName);
        default:
          throw new Error(`Unsupported file type for preview: ${fileType}`);
      }
    } catch (error) {
      console.error('❌ File preview error:', error);
      throw new Error(`Failed to preview file: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  async importFileWithCustomColumns(options: ImportOptionsWithCustomColumns): Promise<ImportResult> {
  const startTime = Date.now();
  
  try {
    console.log(`🚀 Starting enhanced import: ${options.fileName}`);
    
    // ตรวจสอบไฟล์
    if (!fs.existsSync(options.filePath)) {
      throw new Error('File not found');
    }

    const fileType = this.getFileType(options.mimeType, options.fileName);
    
    // อ่านข้อมูลจากไฟล์
    const data = await this.readFileData(options.filePath, fileType);
    
    if (!data || data.length === 0) {
      throw new Error('No data found in file');
    }

    console.log(`📊 Data loaded: ${data.length} rows`);

    // สร้างตารางถ้าจำเป็น
    if (options.createTable) {
      if (options.customColumns && options.customColumns.length > 0) {
        // ใช้ custom columns ที่ผู้ใช้กำหนด
        console.log('🎨 Using custom column structure from user');
        await this.createTableFromCustomColumns(
          options.schema, 
          options.tableName, 
          options.customColumns
        );
      } else {
        // ใช้ auto-detection แบบเดิม
        console.log('🔍 Using auto-detected column structure');
        await this.createTableFromData(options.schema, options.tableName, data);
      }
    }

    // ล้างข้อมูลถ้าจำเป็น
    if (options.truncateBeforeImport) {
      await this.truncateTable(options.schema, options.tableName);
    }

    // Import ข้อมูล
    const result = await this.insertDataInBatches(
      options.schema,
      options.tableName,
      data,
      options.batchSize,
      options.skipErrors
    );

    const executionTime = Date.now() - startTime;
    
    console.log(`✅ Enhanced import completed: ${result.successRows}/${result.totalRows} rows in ${executionTime}ms`);
    
    return {
      ...result,
      executionTime
    };

  } catch (error) {
    const executionTime = Date.now() - startTime;
    console.error(`❌ Enhanced import failed after ${executionTime}ms:`, error);
    
    return {
      success: false,
      totalRows: 0,
      successRows: 0,
      errorRows: 0,
      errors: [{
        row: 0,
        error: error instanceof Error ? error.message : 'Unknown error'
      }],
      executionTime
    };
  } finally {
    // ลบไฟล์ temporary
    try {
      if (fs.existsSync(options.filePath)) {
        fs.unlinkSync(options.filePath);
      }
    } catch (error) {
      console.warn('Failed to delete temporary file:', error);
    }
  }
}

  /**
   * Preview CSV file - ใช้ระบบ CSV parsing ใหม่ที่ปรับปรุงแล้ว
   */
  private async createTableFromCustomColumns(
  schema: string, 
  tableName: string, 
  customColumns: DatabaseColumn[]
): Promise<void> {
  console.log(`📋 Creating table with custom structure: ${customColumns.length} columns`);

  // Validate custom columns
  this.validateCustomColumns(customColumns);

  // สร้างตาราง
  await this.dbService.createTable({
    companyCode: '', // Will be handled by service
    schema,
    tableName,
    columns: customColumns,
    ifNotExists: true
  });

  console.log(`✅ Table "${schema}"."${tableName}" created with custom structure`);
}

/**
   * Validate custom columns structure
   */
  private validateCustomColumns(columns: DatabaseColumn[]): void {
    if (columns.length === 0) {
      throw new Error('At least one column is required');
    }

    // ตรวจสอบชื่อ column ซ้ำ
    const columnNames = columns.map(col => col.name.toLowerCase());
    const duplicates = columnNames.filter((name, index) => columnNames.indexOf(name) !== index);
    
    if (duplicates.length > 0) {
      throw new Error(`Duplicate column names found: ${[...new Set(duplicates)].join(', ')}`);
    }

    // ตรวจสอบ primary key
    const primaryKeys = columns.filter(col => col.isPrimary);
    if (primaryKeys.length === 0) {
      throw new Error('At least one primary key column is required');
    }

    // ตรวจสอบ column properties
    for (const col of columns) {
      if (!col.name || col.name.trim().length === 0) {
        throw new Error('All columns must have names');
      }
      
      if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(col.name)) {
        throw new Error(`Invalid column name "${col.name}". Must start with a letter and contain only letters, numbers, and underscores`);
      }
      
      if (!col.type || col.type.trim().length === 0) {
        throw new Error(`Column "${col.name}" must have a data type`);
      }

      // Type-specific validations
      if ((col.type === 'VARCHAR' || col.type === 'CHAR') && (!col.length || col.length <= 0)) {
        throw new Error(`Column "${col.name}" with type ${col.type} must have a length greater than 0`);
      }
    }

    console.log('✅ Custom columns validation passed');
  }
  private async previewCSV(filePath: string, fileName: string): Promise<FilePreview> {
    return new Promise((resolve, reject) => {
      const results: any[] = [];
      let headers: string[] = [];
      let isFirstRow = true;
      
      fs.createReadStream(filePath)
        .pipe(csv({
          separator: ',',
          quote: '"',
          escape: '"'
        }))
        .on('data', (data: any) => {
          // เก็บ headers จาก row แรก
          if (isFirstRow) {
            headers = Object.keys(data);
            isFirstRow = false;
            console.log(`📋 CSV headers detected: ${headers.join(', ')}`);
          }
          
          // จัดการ empty lines และ data cleaning
          const hasValidData = Object.values(data).some(value => 
            value !== null && value !== undefined && String(value).trim() !== ''
          );
          
          if (!hasValidData) {
            return; // ข้าม empty rows
          }
          
          // ทำความสะอาดข้อมูลและเก็บเฉพาะ 10 rows แรกสำหรับ preview
          if (results.length < 10) {
            const cleanData: any = {};
            for (const [key, value] of Object.entries(data)) {
              const stringValue = String(value).trim();
              cleanData[key] = stringValue === '' ? null : value;
            }
            results.push(cleanData);
          }
        })
        .on('end', () => {
          console.log(`✅ CSV preview completed: ${results.length} sample rows loaded`);
          
          // สร้าง suggested columns โดยใช้ improved type detection
          const suggestedColumns = this.generateSuggestedColumns(headers, results);
          
          resolve({
            headers,
            sampleData: results,
            totalRows: results.length, // Note: นี่คือจำนวน sample rows, ไม่ใช่ total ทั้งไฟล์
            fileName,
            fileType: 'CSV',
            suggestedColumns
          });
        })
        .on('error', (error) => {
          console.error('❌ CSV preview parsing error:', error);
          reject(new Error(`CSV preview failed: ${error.message}`));
        });
    });
  }

  /**
   * Preview Excel file - รองรับทั้ง .xlsx และ .xls with proper type handling
   */
  private async previewExcel(filePath: string, fileName: string): Promise<FilePreview> {
    try {
      const workbook = XLSX.readFile(filePath);
      const sheetName = workbook.SheetNames[0]; // ใช้ sheet แรก
      const worksheet = workbook.Sheets[sheetName];
      
      console.log(`📊 Excel file opened: ${fileName}, using sheet: ${sheetName}`);
      
      // แปลงเป็น JSON และใช้ type guard เพื่อความปลอดภัย
      const rawData = XLSX.utils.sheet_to_json(worksheet, { defval: null });
      
      // Type guard: ตรวจสอบว่าข้อมูลเป็น array ที่ไม่ว่าง และ element แรกเป็น object
      if (!Array.isArray(rawData) || rawData.length === 0) {
        throw new Error('Excel file is empty or has no readable data');
      }
      
      // ตรวจสอบว่า element แรกเป็น object ที่มี properties
      const firstRow = rawData[0];
      if (!this.isValidDataObject(firstRow)) {
        throw new Error('Excel file does not contain valid tabular data - first row must be an object with properties');
      }
      
      // ตอนนี้เราแน่ใจแล้วว่า rawData เป็น array ของ objects
      const allData = rawData as Record<string, any>[];
      const sampleData = allData.slice(0, 10);
      
      // ดึง headers จาก object แรก (ตอนนี้ TypeScript รู้แล้วว่าเป็น object)
      const headers = Object.keys(firstRow);
      console.log(`📋 Excel headers detected: ${headers.join(', ')}`);
      
      // ตรวจสอบว่ามี headers หรือไม่
      if (headers.length === 0) {
        throw new Error('Excel file does not contain any columns');
      }
      
      const suggestedColumns = this.generateSuggestedColumns(headers, sampleData);
      
      return {
        headers,
        sampleData,
        totalRows: allData.length,
        fileName,
        fileType: 'Excel',
        suggestedColumns
      };
    } catch (error) {
      console.error('❌ Excel preview error:', error);
      throw new Error(`Excel preview failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  /**
   * Preview JSON file - รองรับทั้ง array และ single object with proper type handling
   */
  private async previewJSON(filePath: string, fileName: string): Promise<FilePreview> {
    try {
      const fileContent = fs.readFileSync(filePath, 'utf8');
      let parsed: unknown;
      
      try {
        parsed = JSON.parse(fileContent);
      } catch (parseError) {
        throw new Error('Invalid JSON format in file');
      }
      
      // Type guard: แปลงเป็น array และตรวจสอบความถูกต้อง
      let jsonData: unknown[];
      if (Array.isArray(parsed)) {
        jsonData = parsed;
      } else if (parsed !== null && typeof parsed === 'object') {
        jsonData = [parsed];
      } else {
        throw new Error('JSON file must contain an object or array of objects');
      }
      
      if (jsonData.length === 0) {
        throw new Error('JSON file is empty');
      }
      
      // ตรวจสอบว่า element แรกเป็น valid object
      const firstItem = jsonData[0];
      if (!this.isValidDataObject(firstItem)) {
        throw new Error('JSON data must contain objects with properties, not primitive values');
      }
      
      // ตอนนี้เราแน่ใจแล้วว่า jsonData เป็น array ของ objects
      const typedData = jsonData as Record<string, any>[];
      const sampleData = typedData.slice(0, 10);
      const headers = Object.keys(firstItem);
      
      console.log(`📋 JSON structure detected: ${headers.join(', ')}`);
      
      if (headers.length === 0) {
        throw new Error('JSON objects do not contain any properties');
      }
      
      const suggestedColumns = this.generateSuggestedColumns(headers, sampleData);
      
      return {
        headers,
        sampleData,
        totalRows: typedData.length,
        fileName,
        fileType: 'JSON',
        suggestedColumns
      };
    } catch (error) {
      console.error('❌ JSON preview error:', error);
      if (error instanceof SyntaxError) {
        throw new Error('Invalid JSON format in file');
      }
      throw new Error(`JSON preview failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  /**
   * Type guard function: ตรวจสอบว่า value เป็น valid data object หรือไม่
   * Object ที่ valid ต้องเป็น non-null object ที่มี properties อย่างน้อย 1 ตัว
   */
  private isValidDataObject(value: unknown): value is Record<string, any> {
    // ตรวจสอบว่าเป็น object และไม่ใช่ null
    if (typeof value !== 'object' || value === null) {
      return false;
    }
    
    // ตรวจสอบว่าไม่ใช่ array (เพราะ array ก็เป็น object ใน JavaScript)
    if (Array.isArray(value)) {
      return false;
    }
    
    // ตรวจสอบว่ามี properties อย่างน้อย 1 ตัว
    const keys = Object.keys(value);
    return keys.length > 0;
  }

  /**
   * Preview text file - รองรับ tab-separated และ pipe-separated values
   */
  private async previewTXT(filePath: string, fileName: string): Promise<FilePreview> {
    try {
      const fileContent = fs.readFileSync(filePath, 'utf8');
      const lines = fileContent.split('\n').filter(line => line.trim().length > 0);
      
      if (lines.length === 0) {
        throw new Error('Text file is empty');
      }
      
      // ตรวจหา delimiter อัตโนมัติ
      const delimiter = this.detectDelimiter(lines[0]);
      console.log(`🔍 Detected delimiter in text file: "${delimiter}"`);
      
      const headers = lines[0].split(delimiter).map(h => h.trim());
      
      // สร้าง sample data จาก 10 แถวแรก (ไม่รวม header)
      const dataLines = lines.slice(1, 11);
      const sampleData = dataLines.map(line => {
        const values = line.split(delimiter);
        const obj: any = {};
        headers.forEach((header, index) => {
          const value = values[index]?.trim() || null;
          obj[header] = value === '' ? null : value;
        });
        return obj;
      });
      
      console.log(`📋 Text file headers detected: ${headers.join(', ')}`);
      
      const suggestedColumns = this.generateSuggestedColumns(headers, sampleData);
      
      return {
        headers,
        sampleData,
        totalRows: lines.length - 1, // ลบ header row
        fileName,
        fileType: 'Text',
        suggestedColumns
      };
    } catch (error) {
      throw new Error(`Text file preview failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  /**
   * สร้าง suggested columns โดยใช้ improved type detection
   */
  private generateSuggestedColumns(headers: string[], sampleData: any[]): DatabaseColumn[] {
    return headers.map((header, index) => {
      // เก็บ values ที่ไม่ใช่ null/empty สำหรับการวิเคราะห์
      const columnData = sampleData
        .map(row => row[header])
        .filter(val => val != null && val !== '' && val !== 'null');
      
      // ใช้ improved type inference
      const typeInfo = this.inferColumnTypeImproved(header, columnData);
      const sanitizedName = this.sanitizeColumnName(header);
      const isPrimary = this.isPrimaryKeyCandidate(header, columnData, index);
      
      console.log(`🔍 Column suggestion: ${header} -> ${sanitizedName} (${typeInfo.type}${typeInfo.length ? `(${typeInfo.length})` : ''}) - Primary: ${isPrimary}`);
      
      return {
        name: sanitizedName,
        type: typeInfo.type,
        length: typeInfo.length,
        isPrimary: isPrimary,
        isRequired: isPrimary || columnData.length > sampleData.length * 0.8,
        isUnique: isPrimary,
        comment: `Generated from column: ${header} (${columnData.length}/${sampleData.length} non-empty values)`
      };
    });
  }
  async importFile(options: ImportOptions): Promise<ImportResult> {
    const startTime = Date.now();
    
    try {
      console.log(`🚀 Starting import: ${options.fileName}`);
      
      // ตรวจสอบไฟล์
      if (!fs.existsSync(options.filePath)) {
        throw new Error('File not found');
      }

      const fileType = this.getFileType(options.mimeType, options.fileName);
      
      // อ่านข้อมูลจากไฟล์
      const data = await this.readFileData(options.filePath, fileType);
      
      if (!data || data.length === 0) {
        throw new Error('No data found in file');
      }

      console.log(`📊 Data loaded: ${data.length} rows`);

      // สร้างตารางถ้าจำเป็น
      if (options.createTable) {
        await this.createTableFromData(options.schema, options.tableName, data);
      }

      // ล้างข้อมูลถ้าจำเป็น
      if (options.truncateBeforeImport) {
        await this.truncateTable(options.schema, options.tableName);
      }

      // Import ข้อมูล
      const result = await this.insertDataInBatches(
        options.schema,
        options.tableName,
        data,
        options.batchSize,
        options.skipErrors
      );

      const executionTime = Date.now() - startTime;
      
      console.log(`✅ Import completed: ${result.successRows}/${result.totalRows} rows in ${executionTime}ms`);
      
      return {
        ...result,
        executionTime
      };

    } catch (error) {
      const executionTime = Date.now() - startTime;
      console.error(`❌ Import failed after ${executionTime}ms:`, error);
      
      return {
        success: false,
        totalRows: 0,
        successRows: 0,
        errorRows: 0,
        errors: [{
          row: 0,
          error: error instanceof Error ? error.message : 'Unknown error'
        }],
        executionTime
      };
    } finally {
      // ลบไฟล์ temporary
      try {
        if (fs.existsSync(options.filePath)) {
          fs.unlinkSync(options.filePath);
        }
      } catch (error) {
        console.warn('Failed to delete temporary file:', error);
      }
    }
  }

  /**
   * สร้างตารางจากข้อมูล - Fixed Version
   */
  private async createTableFromData(schema: string, tableName: string, data: any[]): Promise<void> {
    if (data.length === 0) {
      throw new Error('Cannot create table from empty data');
    }

    const headers = Object.keys(data[0]);
    console.log(`📋 Detected headers: ${headers.join(', ')}`);

    // สร้าง columns โดยใช้ improved type detection
    const columns: DatabaseColumn[] = headers.map((header, index) => {
      const columnData = data.map(row => row[header]).filter(val => val != null && val !== '' && val !== 'null');
      const typeInfo = this.inferColumnTypeImproved(header, columnData);
      
      const sanitizedName = this.sanitizeColumnName(header);
      const isPrimary = this.isPrimaryKeyCandidate(header, columnData, index);
      
      console.log(`🔍 Column analysis: ${header} -> ${sanitizedName} (${typeInfo.type}${typeInfo.length ? `(${typeInfo.length})` : ''}) - ${columnData.length} values`);
      
      return {
        name: sanitizedName,
        type: typeInfo.type,
        length: typeInfo.length,
        isPrimary: isPrimary,
        isRequired: isPrimary || columnData.length > data.length * 0.8,
        isUnique: isPrimary,
        comment: `Generated from column: ${header}`
      };
    });

    // ตรวจสอบว่ามี primary key หรือไม่
    const hasPrimaryKey = columns.some(col => col.isPrimary);
    if (!hasPrimaryKey) {
      // เพิ่ม auto-increment ID column
      columns.unshift({
        name: 'id',
        type: 'SERIAL',
        isPrimary: true,
        isRequired: true,
        isUnique: true,
        comment: 'Auto-generated primary key'
      });
      console.log('➕ Added auto-increment ID column as primary key');
    }

    // สร้างตาราง
    await this.dbService.createTable({
      companyCode: '', // Will be handled by service
      schema,
      tableName,
      columns,
      ifNotExists: true
    });
  }

  /**
   * Improved Column Type Inference - แก้ไขปัญหา date detection
   */
  private inferColumnTypeImproved(columnName: string, values: any[]): { type: string; length?: number } {
    if (values.length === 0) {
      return { type: 'VARCHAR', length: 255 };
    }

    // Clean และ analyze ข้อมูล
    const cleanValues = values.map(val => String(val).trim()).filter(val => val.length > 0);
    
    if (cleanValues.length === 0) {
      return { type: 'VARCHAR', length: 255 };
    }

    const maxLength = Math.max(...cleanValues.map(val => val.length));
    
    // Boolean detection (very strict)
    const booleanValues = cleanValues.filter(val => 
      ['true', 'false', '1', '0', 'yes', 'no', 'y', 'n'].includes(val.toLowerCase())
    );
    if (booleanValues.length === cleanValues.length && cleanValues.length > 0) {
      return { type: 'BOOLEAN' };
    }

    // Integer detection (strict)
    const integerValues = cleanValues.filter(val => {
      const num = Number(val);
      return !isNaN(num) && Number.isInteger(num) && Math.abs(num) < Number.MAX_SAFE_INTEGER;
    });
    if (integerValues.length === cleanValues.length && cleanValues.length > 0) {
      return { type: 'INTEGER' };
    }

    // Decimal detection (strict)
    const decimalValues = cleanValues.filter(val => {
      const num = Number(val);
      return !isNaN(num) && !Number.isInteger(num);
    });
    if (decimalValues.length === cleanValues.length && cleanValues.length > 0) {
      return { type: 'DECIMAL' };
    }

    // Date/Timestamp detection (VERY RESTRICTIVE)
    const dateValues = cleanValues.filter(val => {
      // เฉพาะ format ที่ชัดเจนเท่านั้น
      if (!/^\d{4}-\d{2}-\d{2}/.test(val) && !/^\d{2}\/\d{2}\/\d{4}/.test(val)) {
        return false;
      }
      const date = new Date(val);
      return !isNaN(date.getTime()) && date.getFullYear() > 1900 && date.getFullYear() < 2100;
    });
    
    // ต้องมีอย่างน้อย 80% ที่เป็น valid date และชื่อ column ต้องเกี่ยวกับวันที่
    const isDateColumn = columnName.toLowerCase().includes('date') || 
                        columnName.toLowerCase().includes('time') ||
                        columnName.toLowerCase().includes('created') ||
                        columnName.toLowerCase().includes('updated');
    
    if (dateValues.length >= cleanValues.length * 0.8 && isDateColumn) {
      // ตรวจสอบว่ามี time component หรือไม่
      const hasTime = cleanValues.some(val => /\d{2}:\d{2}/.test(val));
      return { type: hasTime ? 'TIMESTAMP' : 'DATE' };
    }

    // Email detection
    const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    const emailValues = cleanValues.filter(val => emailPattern.test(val));
    if (emailValues.length === cleanValues.length && cleanValues.length > 0) {
      return { type: 'VARCHAR', length: Math.max(255, maxLength + 50) };
    }

    // Default to VARCHAR with appropriate length
    let suggestedLength = 255;
    if (maxLength <= 50) suggestedLength = 100;
    else if (maxLength <= 100) suggestedLength = 255;
    else if (maxLength <= 500) suggestedLength = 1000;
    else suggestedLength = Math.min(maxLength * 2, 5000);

    return { type: 'VARCHAR', length: suggestedLength };
  }

  /**
   * ตรวจสอบว่า column เป็น primary key candidate หรือไม่
   */
  private isPrimaryKeyCandidate(columnName: string, values: any[], index: number): boolean {
    const name = columnName.toLowerCase();
    
    // ชื่อที่บ่งชี้ว่าเป็น primary key
    if (name === 'id' || name === 'no' || name.endsWith('_id') || name === 'number') {
      return true;
    }

    // Column แรกที่เป็นตัวเลขเรียงลำดับ
    if (index === 0 && values.length > 1) {
      const numericValues = values.map(v => Number(v)).filter(v => !isNaN(v));
      if (numericValues.length === values.length) {
        const uniqueValues = new Set(numericValues);
        if (uniqueValues.size === numericValues.length) {
          return true; // Unique numeric values
        }
      }
    }

    return false;
  }

  /**
   * Insert data in batches with improved error handling
   */
  private async insertDataInBatches(
    schema: string, 
    tableName: string, 
    data: any[], 
    batchSize: number,
    skipErrors: boolean
  ): Promise<ImportResult> {
    const result: ImportResult = {
      success: true,
      totalRows: data.length,
      successRows: 0,
      errorRows: 0,
      errors: [],
      executionTime: 0
    };

    if (data.length === 0) {
      return result;
    }

    // Get table structure to map columns correctly
    const tableColumns = await this.getTableColumns(schema, tableName);
    const headers = Object.keys(data[0]);
    
    console.log(`📊 Table columns: ${tableColumns.map(c => c.column_name).join(', ')}`);
    console.log(`📊 Data headers: ${headers.join(', ')}`);

    for (let i = 0; i < data.length; i += batchSize) {
      const batch = data.slice(i, Math.min(i + batchSize, data.length));
      const batchResult = await this.insertBatch(schema, tableName, batch, tableColumns, skipErrors, i);
      
      result.successRows += batchResult.successRows;
      result.errorRows += batchResult.errorRows;
      result.errors.push(...batchResult.errors);
      
      console.log(`📊 Progress: ${Math.min(i + batchSize, data.length)}/${data.length} rows processed`);
    }

    result.success = result.errorRows === 0;
    return result;
  }

  /**
   * Insert single batch with better error handling and type conversion
   */
  private async insertBatch(
    schema: string,
    tableName: string,
    batch: any[],
    tableColumns: any[],
    skipErrors: boolean,
    offset: number
  ): Promise<Omit<ImportResult, 'executionTime'>> {
    const result = {
      success: true,
      totalRows: batch.length,
      successRows: 0,
      errorRows: 0,
      errors: [] as any[]
    };

    for (let i = 0; i < batch.length; i++) {
      const row = batch[i];
      const rowNumber = offset + i + 1;

      try {
        // Prepare data for insertion
        const insertData = this.prepareRowForInsertion(row, tableColumns);
        
        // Build insert query
        const columns = Object.keys(insertData);
        const placeholders = columns.map((_, index) => `$${index + 1}`).join(', ');
        const values = columns.map(col => insertData[col]);
        
        const query = `INSERT INTO "${schema}"."${tableName}" (${columns.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`;
        
        // Execute insert
        await this.dbService.pool.query(query, values);
        result.successRows++;
        
      } catch (error) {
        result.errorRows++;
        const errorMsg = error instanceof Error ? error.message : 'Unknown error';
        
        if (!skipErrors) {
          throw new Error(`Row ${rowNumber}: ${errorMsg}`);
        }
        
        result.errors.push({
          row: rowNumber,
          error: errorMsg,
          data: row
        });
        
        console.warn(`⚠️ Row ${rowNumber} skipped: ${errorMsg}`);
      }
    }

    return result;
  }

  /**
   * Prepare row data for insertion with type conversion
   */
  private prepareRowForInsertion(row: any, tableColumns: any[]): any {
    const insertData: any = {};
    
    for (const column of tableColumns) {
      const columnName = column.column_name;
      
      // Skip auto-generated columns
      if (column.is_identity === 'YES' || column.column_default?.includes('nextval')) {
        continue;
      }
      
      // Find matching data (case-insensitive)
      const dataKeys = Object.keys(row);
      const matchingKey = dataKeys.find(key => 
        this.sanitizeColumnName(key) === columnName ||
        key.toLowerCase() === columnName.toLowerCase()
      );
      
      if (matchingKey && row[matchingKey] != null && row[matchingKey] !== '') {
        insertData[columnName] = this.convertValueToColumnType(row[matchingKey], column);
      } else if (column.is_nullable === 'NO' && !column.column_default) {
        // Required field but no data
        throw new Error(`Missing required field: ${columnName}`);
      }
    }
    
    return insertData;
  }

  /**
   * Convert value to appropriate column type
   */
  private convertValueToColumnType(value: any, column: any): any {
    if (value == null || value === '') {
      return null;
    }

    const dataType = column.data_type.toLowerCase();
    const stringValue = String(value).trim();

    switch (dataType) {
      case 'integer':
      case 'bigint':
      case 'smallint':
        const intValue = parseInt(stringValue);
        if (isNaN(intValue)) throw new Error(`Invalid integer: ${stringValue}`);
        return intValue;

      case 'numeric':
      case 'decimal':
      case 'real':
      case 'double precision':
        const numValue = parseFloat(stringValue);
        if (isNaN(numValue)) throw new Error(`Invalid number: ${stringValue}`);
        return numValue;

      case 'boolean':
        return ['true', '1', 'yes', 'y', 'on'].includes(stringValue.toLowerCase());

      case 'date':
        const dateValue = new Date(stringValue);
        if (isNaN(dateValue.getTime())) throw new Error(`Invalid date: ${stringValue}`);
        return dateValue.toISOString().split('T')[0];

      case 'timestamp':
      case 'timestamp with time zone':
      case 'timestamp without time zone':
        const timestampValue = new Date(stringValue);
        if (isNaN(timestampValue.getTime())) throw new Error(`Invalid timestamp: ${stringValue}`);
        return timestampValue.toISOString();

      default:
        return stringValue;
    }
  }

  /**
   * Get table column information
   */
  private async getTableColumns(schema: string, tableName: string): Promise<any[]> {
    const query = `
      SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default,
        is_identity,
        character_maximum_length
      FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position;
    `;
    
    const result = await this.dbService.pool.query(query, [schema, tableName]);
    return result.rows;
  }

  /**
   * Truncate table
   */
  private async truncateTable(schema: string, tableName: string): Promise<void> {
    const query = `TRUNCATE TABLE "${schema}"."${tableName}" RESTART IDENTITY CASCADE;`;
    await this.dbService.pool.query(query);
    console.log(`🗑️ Table "${schema}"."${tableName}" truncated`);
  }

  /**
   * ระบุประเภทไฟล์
   */
  private getFileType(mimeType: string, fileName: string): string {
    const extension = path.extname(fileName).toLowerCase();
    
    if (mimeType === 'text/csv' || extension === '.csv') {
      return 'csv';
    }
    
    if (mimeType === 'application/vnd.ms-excel' || 
        mimeType === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
        extension === '.xlsx' || extension === '.xls') {
      return 'excel';
    }
    
    if (mimeType === 'application/json' || extension === '.json') {
      return 'json';
    }
    
    if (mimeType === 'text/plain' || extension === '.txt' || extension === '.tsv') {
      return 'txt';
    }
    
    throw new Error(`Unsupported file type: ${mimeType}`);
  }

  /**
   * อ่านข้อมูลจากไฟล์
   */
  private async readFileData(filePath: string, fileType: string): Promise<any[]> {
    switch (fileType) {
      case 'csv':
        return await this.readCSVData(filePath);
      case 'excel':
        return await this.readExcelData(filePath);
      case 'json':
        return await this.readJSONData(filePath);
      case 'txt':
        return await this.readTXTData(filePath);
      default:
        throw new Error(`Unsupported file type: ${fileType}`);
    }
  }

  private async readCSVData(filePath: string): Promise<any[]> {
    return new Promise((resolve, reject) => {
      const results: any[] = [];
      
      fs.createReadStream(filePath)
        .pipe(csv({
          // csv-parser รองรับ options เหล่านี้เท่านั้น
          separator: ',',  // สามารถเป็น auto-detect ได้โดยไม่ระบุ
          quote: '"',
          escape: '"'
        }))
        .on('data', (data: any) => {
          // จัดการ empty lines และ data cleaning ด้วยตัวเอง
          const hasValidData = Object.values(data).some(value => 
            value !== null && value !== undefined && String(value).trim() !== ''
          );
          
          // ข้าม rows ที่ว่างเปล่าทั้งหมด
          if (!hasValidData) {
            return;
          }
          
          // ทำความสะอาดข้อมูล: แปลง empty strings เป็น null
          const cleanData: any = {};
          for (const [key, value] of Object.entries(data)) {
            // จัดการ values ที่เป็น string ว่างหรือมีแต่ whitespace
            const stringValue = String(value).trim();
            cleanData[key] = stringValue === '' ? null : value;
          }
          
          results.push(cleanData);
        })
        .on('end', () => {
          console.log(`📊 CSV parsing completed: ${results.length} valid rows found`);
          resolve(results);
        })
        .on('error', (error) => {
          console.error('❌ CSV parsing error:', error);
          // แทนที่จะ reject ทันที เราสามารถส่ง partial results ได้
          // หรือสร้าง custom error ที่มีข้อมูลเพิ่มเติม
          reject(new Error(`CSV parsing failed: ${error.message}`));
        });
    });
  }

  private async readExcelData(filePath: string): Promise<any[]> {
    const workbook = XLSX.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    
    return XLSX.utils.sheet_to_json(worksheet, { defval: null });
  }

  private async readJSONData(filePath: string): Promise<any[]> {
    const fileContent = fs.readFileSync(filePath, 'utf8');
    const parsed = JSON.parse(fileContent);
    return Array.isArray(parsed) ? parsed : [parsed];
  }

  private async readTXTData(filePath: string): Promise<any[]> {
    const fileContent = fs.readFileSync(filePath, 'utf8');
    const lines = fileContent.split('\n').filter(line => line.trim().length > 0);
    
    if (lines.length === 0) {
      throw new Error('Empty text file');
    }
    
    const delimiter = this.detectDelimiter(lines[0]);
    const headers = lines[0].split(delimiter).map(h => h.trim());
    
    return lines.slice(1).map(line => {
      const values = line.split(delimiter);
      const obj: any = {};
      headers.forEach((header, index) => {
        obj[header] = values[index]?.trim() || null;
      });
      return obj;
    });
  }

  private detectDelimiter(firstLine: string): string {
    const delimiters = ['\t', '|', ';', ','];
    let maxCount = 0;
    let bestDelimiter = '\t';
    
    for (const delimiter of delimiters) {
      const count = (firstLine.match(new RegExp(delimiter.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g')) || []).length;
      if (count > maxCount) {
        maxCount = count;
        bestDelimiter = delimiter;
      }
    }
    
    return bestDelimiter;
  }

  /**
   * ทำความสะอาดชื่อ column
   */
  private sanitizeColumnName(name: string): string {
    return name
      .replace(/[^a-zA-Z0-9_]/g, '_')
      .replace(/^([0-9])/, '_$1')
      .toLowerCase()
      .substring(0, 63); // PostgreSQL limit
  }
}