// lib/services/FileImportService.ts - Complete Fixed Version
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
  delimiter?: string;
}

export interface ImportOptionsWithCustomColumns extends ImportOptions {
  customColumns?: DatabaseColumn[];
}

export interface DelimiterAnalysis {
  delimiter: string;
  confidence: number;
  columnCount: number;
}

/**
 * Enhanced File Import Service - Fixed Version
 */
export class FileImportService {
  private dbService: DatabaseService;

  constructor(dbService: DatabaseService) {
    this.dbService = dbService;
  }

  async previewFile(filePath: string, fileName: string, mimeType: string): Promise<FilePreview> {
    try {
      console.log(`Previewing file: ${fileName} (${mimeType})`);
      
      if (!fs.existsSync(filePath)) {
        throw new Error('File not found for preview');
      }

      this.logFileSample(filePath, fileName);

      const fileType = this.getFileType(mimeType, fileName);
      console.log(`Detected file type: ${fileType}`);
      
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
      console.error('File preview error:', error);
      throw new Error(`Failed to preview file: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  /**
   * Enhanced delimiter detection with confidence scoring
   */
  private analyzeDelimiters(sampleLines: string[]): DelimiterAnalysis {
    const delimiters = [
      { char: '\t', name: 'Tab', weight: 5 },
      { char: ',', name: 'Comma', weight: 3 },
      { char: ';', name: 'Semicolon', weight: 2 },
      { char: '|', name: 'Pipe', weight: 1 }
    ];

    let bestAnalysis: DelimiterAnalysis = {
      delimiter: ',',
      confidence: 0,
      columnCount: 0
    };

    for (const delim of delimiters) {
      const analysis = this.testDelimiter(sampleLines, delim.char, delim.weight);
      console.log(`Delimiter "${delim.name}": ${analysis.columnCount} columns, confidence: ${analysis.confidence}`);
      
      if (analysis.confidence > bestAnalysis.confidence) {
        bestAnalysis = {
          delimiter: delim.char,
          confidence: analysis.confidence,
          columnCount: analysis.columnCount
        };
      }
    }

    console.log(`Selected delimiter: "${bestAnalysis.delimiter}" (confidence: ${bestAnalysis.confidence})`);
    return bestAnalysis;
  }

  private testDelimiter(lines: string[], delimiter: string, weight: number): DelimiterAnalysis {
    const counts = lines.map(line => (line.split(delimiter).length - 1));
    const maxCount = Math.max(...counts);
    const consistency = counts.filter(count => count === maxCount).length / counts.length;
    
    const confidence = maxCount > 0 ? (maxCount * consistency * weight) : 0;
    
    return {
      delimiter,
      confidence,
      columnCount: maxCount + 1
    };
  }

  /**
   * Fixed CSV preview - NO invalid options
   */
  private async previewCSV(filePath: string, fileName: string): Promise<FilePreview> {
    const fileContent = fs.readFileSync(filePath, 'utf8');
    const sampleLines = fileContent.split('\n').slice(0, 5).filter(line => line.trim().length > 0);
    
    if (sampleLines.length === 0) {
      throw new Error('CSV file is empty');
    }

    const delimiterAnalysis = this.analyzeDelimiters(sampleLines);
    
    return new Promise((resolve, reject) => {
      const results: any[] = [];
      let headers: string[] = [];
      let isFirstRow = true;
      
      fs.createReadStream(filePath, { encoding: 'utf8' })
        .pipe(csv({
          separator: delimiterAnalysis.delimiter,
          quote: '"',
          escape: '"'
        }))
        .on('headers', (detectedHeaders: string[]) => {
          headers = detectedHeaders.map(h => h.trim());
          console.log(`CSV headers detected (${headers.length}): [${headers.join('", "')}]`);
        })
        .on('data', (data: any) => {
          if (isFirstRow) {
            isFirstRow = false;
            const dataKeys = Object.keys(data);
            console.log(`First row columns (${dataKeys.length}): [${dataKeys.join('", "')}]`);
          }
          
          const cleanData = this.cleanRowData(data);
          
          const hasValidData = Object.values(cleanData).some(value => 
            value !== null && value !== undefined && String(value).trim() !== ''
          );
          
          if (!hasValidData) {
            return;
          }
          
          if (results.length < 10) {
            results.push(cleanData);
          }
        })
        .on('end', () => {
          console.log(`CSV preview completed: ${results.length} sample rows loaded`);
          
          const suggestedColumns = this.generateSuggestedColumns(headers, results);
          
          resolve({
            headers,
            sampleData: results,
            totalRows: results.length,
            fileName,
            fileType: 'CSV',
            suggestedColumns,
            delimiter: delimiterAnalysis.delimiter
          });
        })
        .on('error', (error) => {
          console.error('CSV preview parsing error:', error);
          reject(new Error(`CSV preview failed: ${error.message}`));
        });
    });
  }

  /**
   * Enhanced Excel preview with proper date handling
   */
  private async previewExcel(filePath: string, fileName: string): Promise<FilePreview> {
    try {
      const workbook = XLSX.readFile(filePath, {
        cellDates: false,
        dateNF: 'yyyy-mm-dd'
      });
      
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      
      console.log(`Excel file opened: ${fileName}, using sheet: ${sheetName}`);
      
      const rawData = XLSX.utils.sheet_to_json(worksheet, { 
        defval: null,
        raw: false
      });
      
      if (!Array.isArray(rawData) || rawData.length === 0) {
        throw new Error('Excel file is empty or has no readable data');
      }
      
      const firstRow = rawData[0];
      if (!this.isValidDataObject(firstRow)) {
        throw new Error('Excel file does not contain valid tabular data');
      }
      
      const allData = rawData as Record<string, any>[];
      const processedData = allData.map(row => this.processExcelRow(row));
      const sampleData = processedData.slice(0, 10);
      
      const headers = Object.keys(firstRow);
      console.log(`Excel headers detected (${headers.length}): [${headers.join('", "')}]`);
      
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
      console.error('Excel preview error:', error);
      throw new Error(`Excel preview failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  private processExcelRow(row: Record<string, any>): Record<string, any> {
    const processedRow: Record<string, any> = {};
    
    for (const [key, value] of Object.entries(row)) {
      if (value == null) {
        processedRow[key] = null;
        continue;
      }
      
      const stringValue = String(value).trim();
      
      if (this.isExcelDateSerial(stringValue, key)) {
        const serialNumber = parseInt(stringValue);
        const convertedDate = this.convertExcelSerial(serialNumber);
        processedRow[key] = convertedDate;
        console.log(`Converted Excel serial ${serialNumber} → ${convertedDate} for column "${key}"`);
      } else {
        processedRow[key] = stringValue === '' ? null : value;
      }
    }
    
    return processedRow;
  }

  private isExcelDateSerial(value: string, columnName: string): boolean {
    if (!/^\d{5,6}$/.test(value)) {
      return false;
    }
    
    const num = parseInt(value);
    
    if (num < 25567 || num > 55000) {
      return false;
    }
    
    const lowerName = columnName.toLowerCase();
    const isDateColumn = lowerName.includes('date') || 
                        lowerName.includes('time') ||
                        lowerName.includes('วันที่') ||
                        lowerName.includes('created') ||
                        lowerName.includes('updated');
    
    return isDateColumn;
  }

  private convertExcelSerial(serialNumber: number): string {
    const excelEpoch = new Date(1900, 0, 1);
    const millisecondsPerDay = 24 * 60 * 60 * 1000;
    
    const adjustment = serialNumber > 59 ? -2 : -1;
    const jsDate = new Date(excelEpoch.getTime() + (serialNumber + adjustment) * millisecondsPerDay);
    
    return jsDate.toISOString().split('T')[0];
  }

  private cleanRowData(data: any): any {
    const cleanData: any = {};
    
    for (const [key, value] of Object.entries(data)) {
      if (value == null || value === undefined) {
        cleanData[key] = null;
        continue;
      }
      
      const stringValue = String(value).trim();
      
      if (stringValue === '' || 
          stringValue === '-' || 
          stringValue === 'null' || 
          stringValue === 'NULL' ||
          stringValue === 'n/a' ||
          stringValue === 'N/A') {
        cleanData[key] = null;
      } else {
        cleanData[key] = value;
      }
    }
    
    return cleanData;
  }

  private inferColumnTypeImproved(columnName: string, values: any[]): { type: string; length?: number } {
    if (values.length === 0) {
      return { type: 'VARCHAR', length: 255 };
    }

    const cleanValues: string[] = values
      .map(val => val == null ? null : String(val).trim())
      .filter((val): val is string => val != null && val !== '' && val !== '-');
    
    if (cleanValues.length === 0) {
      return { type: 'VARCHAR', length: 255 };
    }

    const maxLength = Math.max(...cleanValues.map(val => this.calculateUTF8Length(val)));
    const sampleSize = cleanValues.length;
    const lowerColumnName = columnName.toLowerCase();

    console.log(`Analyzing column "${columnName}": ${sampleSize} values, max length: ${maxLength}`);

    if (this.detectExcelDateColumn(cleanValues, lowerColumnName)) {
      return { type: 'DATE' };
    }

    if (this.detectDateColumn(cleanValues, lowerColumnName)) {
      return { type: 'DATE' };
    }

    if (this.detectBooleanColumn(cleanValues)) {
      return { type: 'BOOLEAN' };
    }

    if (this.detectIntegerColumn(cleanValues)) {
      return { type: 'INTEGER' };
    }

    if (this.detectDecimalColumn(cleanValues)) {
      return { type: 'DECIMAL', length: 10 };
    }

    if (this.detectEmailColumn(cleanValues)) {
      return { type: 'VARCHAR', length: Math.max(320, maxLength + 50) };
    }

    const suggestedLength = this.calculateVarcharLength(maxLength);
    return { type: 'VARCHAR', length: suggestedLength };
  }

  private detectExcelDateColumn(values: string[], columnName: string): boolean {
    const excelDatePattern = /^\d{5,6}$/;
    const excelDateValues = values.filter(val => {
      if (!excelDatePattern.test(val)) return false;
      const num = parseInt(val);
      return num >= 25567 && num <= 55000;
    });
    
    const isDateColumn = columnName.includes('date') || 
                        columnName.includes('time') ||
                        columnName.includes('วันที่') ||
                        columnName.includes('created') ||
                        columnName.includes('updated');
    
    const threshold = isDateColumn ? 0.5 : 0.9;
    return excelDateValues.length >= values.length * threshold;
  }

  private detectDateColumn(values: string[], columnName: string): boolean {
    const datePatterns = [
      /^\d{4}-\d{2}-\d{2}$/,
      /^\d{2}\/\d{2}\/\d{4}$/,
      /^\d{1,2}[-\/]\d{1,2}[-\/]\d{4}$/
    ];
    
    const dateValues = values.filter(val => {
      const matchesPattern = datePatterns.some(pattern => pattern.test(val));
      if (!matchesPattern) return false;
      
      const date = new Date(val);
      return !isNaN(date.getTime()) && 
             date.getFullYear() > 1900 && 
             date.getFullYear() < 2100;
    });
    
    const isDateColumn = columnName.includes('date') || 
                        columnName.includes('time') ||
                        columnName.includes('วันที่');
    
    const threshold = isDateColumn ? 0.6 : 0.9;
    return dateValues.length >= values.length * threshold;
  }

  private detectBooleanColumn(values: string[]): boolean {
    const booleanValues = values.filter(val => 
      ['true', 'false', '1', '0', 'yes', 'no', 'y', 'n', 'ใช่', 'ไม่'].includes(val.toLowerCase())
    );
    return booleanValues.length === values.length && values.length > 0;
  }

  private detectIntegerColumn(values: string[]): boolean {
    const integerValues = values.filter(val => {
      const num = Number(val);
      return !isNaN(num) && 
             Number.isInteger(num) && 
             Math.abs(num) < Number.MAX_SAFE_INTEGER &&
             !val.includes('.');
    });
    return integerValues.length === values.length && values.length > 0;
  }

  private detectDecimalColumn(values: string[]): boolean {
    const decimalValues = values.filter(val => {
      const num = Number(val);
      return !isNaN(num) && !Number.isInteger(num) && val.includes('.');
    });
    return decimalValues.length === values.length && values.length > 0;
  }

  private detectEmailColumn(values: string[]): boolean {
    const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    const emailValues = values.filter(val => emailPattern.test(val));
    return emailValues.length === values.length && values.length > 0;
  }

  private calculateUTF8Length(text: string): number {
    let byteLength = 0;
    for (let i = 0; i < text.length; i++) {
      const code = text.charCodeAt(i);
      if (code <= 0x7F) {
        byteLength += 1;
      } else if (code <= 0x7FF) {
        byteLength += 2;
      } else if (code <= 0xFFFF) {
        byteLength += 3;
      } else {
        byteLength += 4;
      }
    }
    return byteLength;
  }

  private calculateVarcharLength(maxByteLength: number): number {
    if (maxByteLength <= 50) return 100;
    if (maxByteLength <= 100) return 255;
    if (maxByteLength <= 500) return 1000;
    if (maxByteLength <= 1000) return 2000;
    return Math.min(maxByteLength * 2, 5000);
  }

  private convertValueToColumnType(value: any, column: any): any {
    if (value == null || value === '') {
      return null;
    }

    const dataType = column.data_type.toLowerCase();
    const stringValue = String(value).trim();

    if ((dataType === 'date' || dataType === 'timestamp') && this.isExcelDateSerial(stringValue, column.column_name || '')) {
      const serialNumber = parseInt(stringValue);
      const convertedDate = this.convertExcelSerial(serialNumber);
      
      if (dataType === 'date') {
        return convertedDate;
      } else {
        return convertedDate + 'T00:00:00Z';
      }
    }

    if (dataType === 'date' || dataType === 'timestamp') {
      const dateValue = new Date(stringValue);
      if (!isNaN(dateValue.getTime())) {
        if (dataType === 'date') {
          return dateValue.toISOString().split('T')[0];
        } else {
          return dateValue.toISOString();
        }
      }
      throw new Error(`Invalid date value: ${stringValue}`);
    }

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
        return ['true', '1', 'yes', 'y', 'on', 'ใช่'].includes(stringValue.toLowerCase());

      default:
        return stringValue;
    }
  }

  private logFileSample(filePath: string, fileName: string): void {
    try {
      const fileContent = fs.readFileSync(filePath, 'utf8');
      const lines = fileContent.split('\n').slice(0, 3);
      
      console.log(`File sample for ${fileName}:`);
      lines.forEach((line, i) => {
        const preview = line.length > 100 ? line.substring(0, 100) + '...' : line;
        console.log(`Line ${i + 1}: "${preview}"`);
      });
      
      if (lines[0]) {
        const firstLine = lines[0];
        console.log('Character analysis:');
        ['\t', ',', ';', '|'].forEach(char => {
          const count = (firstLine.split(char).length - 1);
          const charName = char === '\t' ? 'Tab' : char;
          console.log(`  ${charName}: ${count} occurrences`);
        });
      }
    } catch (error) {
      console.warn('Could not read file sample:', error);
    }
  }

  private async previewJSON(filePath: string, fileName: string): Promise<FilePreview> {
    try {
      const fileContent = fs.readFileSync(filePath, 'utf8');
      let parsed: unknown;
      
      try {
        parsed = JSON.parse(fileContent);
      } catch (parseError) {
        throw new Error('Invalid JSON format in file');
      }
      
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
      
      const firstItem = jsonData[0];
      if (!this.isValidDataObject(firstItem)) {
        throw new Error('JSON data must contain objects with properties, not primitive values');
      }
      
      const typedData = jsonData as Record<string, any>[];
      const sampleData = typedData.slice(0, 10);
      const headers = Object.keys(firstItem);
      
      console.log(`JSON structure detected: ${headers.join(', ')}`);
      
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
      console.error('JSON preview error:', error);
      if (error instanceof SyntaxError) {
        throw new Error('Invalid JSON format in file');
      }
      throw new Error(`JSON preview failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  private async previewTXT(filePath: string, fileName: string): Promise<FilePreview> {
    try {
      const fileContent = fs.readFileSync(filePath, 'utf8');
      const lines = fileContent.split('\n').filter(line => line.trim().length > 0);
      
      if (lines.length === 0) {
        throw new Error('Text file is empty');
      }
      
      const delimiterAnalysis = this.analyzeDelimiters(lines.slice(0, 5));
      console.log(`Text file delimiter detected: "${delimiterAnalysis.delimiter}"`);
      
      const headers = lines[0].split(delimiterAnalysis.delimiter).map(h => h.trim());
      
      const dataLines = lines.slice(1, 11);
      const sampleData = dataLines.map(line => {
        const values = line.split(delimiterAnalysis.delimiter);
        const obj: any = {};
        headers.forEach((header, index) => {
          const value = values[index]?.trim() || null;
          obj[header] = value === '' ? null : value;
        });
        return obj;
      });
      
      console.log(`Text file headers detected: ${headers.join(', ')}`);
      
      const suggestedColumns = this.generateSuggestedColumns(headers, sampleData);
      
      return {
        headers,
        sampleData,
        totalRows: lines.length - 1,
        fileName,
        fileType: 'Text',
        suggestedColumns,
        delimiter: delimiterAnalysis.delimiter
      };
    } catch (error) {
      throw new Error(`Text file preview failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  private isValidDataObject(value: unknown): value is Record<string, any> {
    if (typeof value !== 'object' || value === null) {
      return false;
    }
    
    if (Array.isArray(value)) {
      return false;
    }
    
    const keys = Object.keys(value);
    return keys.length > 0;
  }

  private generateSuggestedColumns(headers: string[], sampleData: any[]): DatabaseColumn[] {
    return headers.map((header, index) => {
      const columnData = sampleData
        .map(row => row[header])
        .filter(val => val != null && val !== '' && val !== 'null');
      
      const typeInfo = this.inferColumnTypeImproved(header, columnData);
      const sanitizedName = this.sanitizeColumnName(header);
      const isPrimary = this.isPrimaryKeyCandidate(header, columnData, index);
      
      console.log(`Column suggestion: ${header} -> ${sanitizedName} (${typeInfo.type}${typeInfo.length ? `(${typeInfo.length})` : ''}) - Primary: ${isPrimary}`);
      
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

  private isPrimaryKeyCandidate(columnName: string, values: any[], index: number): boolean {
    const name = columnName.toLowerCase();
    
    if (name === 'id' || name === 'no' || name.endsWith('_id') || name === 'number') {
      return true;
    }

    if (index === 0 && values.length > 1) {
      const numericValues = values.map(v => Number(v)).filter(v => !isNaN(v));
      if (numericValues.length === values.length) {
        const uniqueValues = new Set(numericValues);
        if (uniqueValues.size === numericValues.length) {
          return true;
        }
      }
    }

    return false;
  }

  private sanitizeColumnName(name: string): string {
    return name
      .replace(/[^a-zA-Z0-9_]/g, '_')
      .replace(/^([0-9])/, '_$1')
      .toLowerCase()
      .substring(0, 63);
  }

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

  // Import methods
  async importFile(options: ImportOptions): Promise<ImportResult> {
    const startTime = Date.now();
    
    try {
      console.log(`Starting import: ${options.fileName}`);
      
      if (!fs.existsSync(options.filePath)) {
        throw new Error('File not found');
      }

      const fileType = this.getFileType(options.mimeType, options.fileName);
      const data = await this.readFileData(options.filePath, fileType);
      
      if (!data || data.length === 0) {
        throw new Error('No data found in file');
      }

      console.log(`Data loaded: ${data.length} rows`);

      if (options.createTable) {
        await this.createTableFromData(options.schema, options.tableName, data);
      }

      if (options.truncateBeforeImport) {
        await this.truncateTable(options.schema, options.tableName);
      }

      const result = await this.insertDataInBatches(
        options.schema,
        options.tableName,
        data,
        options.batchSize,
        options.skipErrors
      );

      const executionTime = Date.now() - startTime;
      
      console.log(`Import completed: ${result.successRows}/${result.totalRows} rows in ${executionTime}ms`);
      
      return {
        ...result,
        executionTime
      };

    } catch (error) {
      const executionTime = Date.now() - startTime;
      console.error(`Import failed after ${executionTime}ms:`, error);
      
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
      try {
        if (fs.existsSync(options.filePath)) {
          fs.unlinkSync(options.filePath);
        }
      } catch (error) {
        console.warn('Failed to delete temporary file:', error);
      }
    }
  }

  async importFileWithCustomColumns(options: ImportOptionsWithCustomColumns): Promise<ImportResult> {
    const startTime = Date.now();
    
    try {
      console.log(`Starting enhanced import: ${options.fileName}`);
      
      if (!fs.existsSync(options.filePath)) {
        throw new Error('File not found');
      }

      const fileType = this.getFileType(options.mimeType, options.fileName);
      const data = await this.readFileData(options.filePath, fileType);
      
      if (!data || data.length === 0) {
        throw new Error('No data found in file');
      }

      console.log(`Data loaded: ${data.length} rows`);

      if (options.createTable) {
        if (options.customColumns && options.customColumns.length > 0) {
          console.log('Using custom column structure from user');
          await this.createTableFromCustomColumns(
            options.schema, 
            options.tableName, 
            options.customColumns
          );
        } else {
          console.log('Using auto-detected column structure');
          await this.createTableFromData(options.schema, options.tableName, data);
        }
      }

      if (options.truncateBeforeImport) {
        await this.truncateTable(options.schema, options.tableName);
      }

      const result = await this.insertDataInBatches(
        options.schema,
        options.tableName,
        data,
        options.batchSize,
        options.skipErrors
      );

      const executionTime = Date.now() - startTime;
      
      console.log(`Enhanced import completed: ${result.successRows}/${result.totalRows} rows in ${executionTime}ms`);
      
      return {
        ...result,
        executionTime
      };

    } catch (error) {
      const executionTime = Date.now() - startTime;
      console.error(`Enhanced import failed after ${executionTime}ms:`, error);
      
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
      try {
        if (fs.existsSync(options.filePath)) {
          fs.unlinkSync(options.filePath);
        }
      } catch (error) {
        console.warn('Failed to delete temporary file:', error);
      }
    }
  }

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
    const fileContent = fs.readFileSync(filePath, 'utf8');
    const sampleLines = fileContent.split('\n').slice(0, 5).filter(line => line.trim().length > 0);
    const delimiterAnalysis = this.analyzeDelimiters(sampleLines);
    
    return new Promise((resolve, reject) => {
      const results: any[] = [];
      
      fs.createReadStream(filePath, { encoding: 'utf8' })
        .pipe(csv({
          separator: delimiterAnalysis.delimiter,
          quote: '"',
          escape: '"'
        }))
        .on('data', (data: any) => {
          const cleanData = this.cleanRowData(data);
          
          const hasValidData = Object.values(cleanData).some(value => 
            value !== null && value !== undefined && String(value).trim() !== ''
          );
          
          if (hasValidData) {
            results.push(cleanData);
          }
        })
        .on('end', () => {
          console.log(`CSV parsing completed: ${results.length} valid rows found`);
          resolve(results);
        })
        .on('error', (error) => {
          console.error('CSV parsing error:', error);
          reject(new Error(`CSV parsing failed: ${error.message}`));
        });
    });
  }

  private async readExcelData(filePath: string): Promise<any[]> {
    const workbook = XLSX.readFile(filePath, { cellDates: false });
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    
    const rawData = XLSX.utils.sheet_to_json(worksheet, { defval: null, raw: false });
    return (rawData as Record<string, any>[]).map(row => this.processExcelRow(row));
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
    
    const delimiterAnalysis = this.analyzeDelimiters(lines.slice(0, 5));
    const headers = lines[0].split(delimiterAnalysis.delimiter).map(h => h.trim());
    
    return lines.slice(1).map(line => {
      const values = line.split(delimiterAnalysis.delimiter);
      const obj: any = {};
      headers.forEach((header, index) => {
        const value = values[index]?.trim() || null;
        obj[header] = value === '' ? null : value;
      });
      return obj;
    });
  }

  private async createTableFromData(schema: string, tableName: string, data: any[]): Promise<void> {
    if (data.length === 0) {
      throw new Error('Cannot create table from empty data');
    }

    const headers = Object.keys(data[0]);
    console.log(`Detected headers: ${headers.join(', ')}`);

    const columns: DatabaseColumn[] = headers.map((header, index) => {
      const columnData = data.map(row => row[header]).filter(val => val != null && val !== '' && val !== 'null');
      const typeInfo = this.inferColumnTypeImproved(header, columnData);
      
      const sanitizedName = this.sanitizeColumnName(header);
      const isPrimary = this.isPrimaryKeyCandidate(header, columnData, index);
      
      console.log(`Column analysis: ${header} -> ${sanitizedName} (${typeInfo.type}${typeInfo.length ? `(${typeInfo.length})` : ''}) - ${columnData.length} values`);
      
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

    const hasPrimaryKey = columns.some(col => col.isPrimary);
    if (!hasPrimaryKey) {
      columns.unshift({
        name: 'id',
        type: 'SERIAL',
        isPrimary: true,
        isRequired: true,
        isUnique: true,
        comment: 'Auto-generated primary key'
      });
      console.log('Added auto-increment ID column as primary key');
    }

    await this.dbService.createTable({
      companyCode: '',
      schema,
      tableName,
      columns,
      ifNotExists: true
    });
  }

  private async createTableFromCustomColumns(schema: string, tableName: string, customColumns: DatabaseColumn[]): Promise<void> {
    console.log(`Creating table with custom structure: ${customColumns.length} columns`);
    this.validateCustomColumns(customColumns);

    await this.dbService.createTable({
      companyCode: '',
      schema,
      tableName,
      columns: customColumns,
      ifNotExists: true
    });

    console.log(`Table "${schema}"."${tableName}" created with custom structure`);
  }

  private validateCustomColumns(columns: DatabaseColumn[]): void {
    if (columns.length === 0) {
      throw new Error('At least one column is required');
    }

    const columnNames = columns.map(col => col.name.toLowerCase());
    const duplicates = columnNames.filter((name, index) => columnNames.indexOf(name) !== index);
    
    if (duplicates.length > 0) {
      throw new Error(`Duplicate column names found: ${[...new Set(duplicates)].join(', ')}`);
    }

    const primaryKeys = columns.filter(col => col.isPrimary);
    if (primaryKeys.length === 0) {
      throw new Error('At least one primary key column is required');
    }

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

      if ((col.type === 'VARCHAR' || col.type === 'CHAR') && (!col.length || col.length <= 0)) {
        throw new Error(`Column "${col.name}" with type ${col.type} must have a length greater than 0`);
      }
    }

    console.log('Custom columns validation passed');
  }

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

    const tableColumns = await this.getTableColumns(schema, tableName);
    const headers = Object.keys(data[0]);
    
    // *** แก้ไข: ใช้ filter condition เดียวกัน ***
    const insertableColumns = tableColumns.filter(col => {
      if (col.column_name === 'id') return false;
      if (col.is_identity === 'YES') return false;
      if (col.column_default && col.column_default.includes('nextval')) return false;
      return true;
    });
    
    console.log(`Table columns: ${tableColumns.map(c => c.column_name).join(', ')}`);
    console.log(`Insertable columns: ${insertableColumns.map(c => c.column_name).join(', ')}`);
    console.log(`Data headers: ${headers.join(', ')}`);

    // ตรวจสอบว่ามี insertable columns
    if (insertableColumns.length === 0) {
      throw new Error('No insertable columns found in table');
    }

    for (let i = 0; i < data.length; i += batchSize) {
      const batch = data.slice(i, Math.min(i + batchSize, data.length));
      // *** ส่ง tableColumns ทั้งหมด ให้ insertBatch กรองเอง ***
      const batchResult = await this.insertBatch(schema, tableName, batch, tableColumns, skipErrors, i);
      
      result.successRows += batchResult.successRows;
      result.errorRows += batchResult.errorRows;
      result.errors.push(...batchResult.errors);
      
      console.log(`Progress: ${Math.min(i + batchSize, data.length)}/${data.length} rows processed`);
    }

    result.success = result.errorRows === 0 || (result.successRows > 0 && skipErrors);
    return result;
  }

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

    // กรอง id column ออก
    const insertableColumns = tableColumns.filter(col => {
      if (col.column_name === 'id') return false;
      if (col.is_identity === 'YES') return false;
      if (col.column_default && col.column_default.includes('nextval')) return false;
      return true;
    });

    console.log(`Insertable columns count: ${insertableColumns.length}`);
    console.log(`Insertable column names: ${insertableColumns.map(c => c.column_name).join(', ')}`);

    for (let i = 0; i < batch.length; i++) {
      const row = batch[i];
      const rowNumber = offset + i + 1;

      try {
        const insertData = this.prepareRowForInsertion(row, insertableColumns);
        
        const columns = Object.keys(insertData);
        
        // ตรวจสอบว่ามี columns ที่จะ insert
        if (columns.length === 0) {
          console.warn(`Row ${rowNumber}: No data to insert (all fields are empty or null)`);
          result.errorRows++;
          result.errors.push({
            row: rowNumber,
            error: 'No data to insert - all fields are empty or no matching columns',
            data: row
          });
          continue;
        }
        
        const values = columns.map(col => insertData[col]);
        const placeholders = columns.map((_, index) => `$${index + 1}`).join(', ');
        
        // *** ลบ ON CONFLICT DO NOTHING ออก เพื่อให้ append ได้จริง ***
        const query = `
          INSERT INTO "${schema}"."${tableName}" 
          (${columns.map(c => `"${c}"`).join(', ')}) 
          VALUES (${placeholders})
          RETURNING id
        `;
        
        const insertResult = await this.dbService.pool.query(query, values);
        
        if (insertResult.rows && insertResult.rows.length > 0) {
          result.successRows++;
          console.log(`Row ${rowNumber}: Inserted successfully with id ${insertResult.rows[0].id}`);
        }
        
      } catch (error) {
        result.errorRows++;
        const errorMsg = error instanceof Error ? error.message : 'Unknown error';
        
        // ถ้าเป็น duplicate key error จริง จะแสดง error message ชัดเจน
        if (errorMsg.includes('duplicate key')) {
          console.warn(`Row ${rowNumber}: Duplicate key - ${errorMsg}`);
        } else {
          console.warn(`Row ${rowNumber} skipped: ${errorMsg}`);
        }
        
        if (!skipErrors) {
          throw new Error(`Row ${rowNumber}: ${errorMsg}`);
        }
        
        result.errors.push({
          row: rowNumber,
          error: errorMsg,
          data: row
        });
      }
    }

    return result;
  }

  private prepareRowForInsertion(row: any, tableColumns: any[]): any {
    const insertData: any = {};
    
    for (const column of tableColumns) {
      const columnName = column.column_name;
      
      if (columnName === 'id' || 
          column.is_identity === 'YES' || 
          column.column_default?.includes('nextval')) {
        continue;
      }
      
      const dataKeys = Object.keys(row);
      const matchingKey = dataKeys.find(key => 
        this.sanitizeColumnName(key) === columnName ||
        key.toLowerCase() === columnName.toLowerCase()
      );
      
      if (matchingKey && row[matchingKey] != null && row[matchingKey] !== '') {
        insertData[columnName] = this.convertValueToColumnType(row[matchingKey], column);
      } else if (column.is_nullable === 'NO' && !column.column_default) {
        throw new Error(`Missing required field: ${columnName}`);
      }
    }
    
    return insertData;
  }

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

  private async truncateTable(schema: string, tableName: string): Promise<void> {
    const query = `TRUNCATE TABLE "${schema}"."${tableName}" RESTART IDENTITY CASCADE;`;
    await this.dbService.pool.query(query);
    console.log(`Table "${schema}"."${tableName}" truncated`);
  }
}