import csv from 'csv-parser';
import * as XLSX from 'xlsx';
import { createReadStream } from 'fs';
import { getCompanyDatabase, getTableSchema } from './database';

export interface ImportResult {
  success: boolean;
  totalRows: number;
  successRows: number;
  errorRows: number;
  errors: Array<{ row: number; error: string; data?: any }>;
}

export class DataImportProcessor {
  private companyCode: string;
  
  constructor(companyCode: string) {
    this.companyCode = companyCode;
  }

  async processFile(
    filePath: string,
    fileName: string,
    tableName: string
  ): Promise<ImportResult> {
    const fileExtension = fileName.split('.').pop()?.toLowerCase();
    
    let data: any[] = [];
    
    try {
      switch (fileExtension) {
        case 'csv':
          data = await this.parseCsv(filePath);
          break;
        case 'xlsx':
        case 'xls':
          data = await this.parseExcel(filePath);
          break;
        case 'json':
          data = await this.parseJson(filePath);
          break;
        default:
          throw new Error(`Unsupported file type: ${fileExtension}`);
      }

      return await this.validateAndImport(data, tableName);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
      return {
        success: false,
        totalRows: 0,
        successRows: 0,
        errorRows: 0,
        errors: [{ row: 0, error: errorMessage }]
      };
    }
  }

  private async parseCsv(filePath: string): Promise<any[]> {
    return new Promise((resolve, reject) => {
      const results: any[] = [];
      
      createReadStream(filePath)
        .pipe(csv())
        .on('data', (data) => results.push(data))
        .on('end', () => resolve(results))
        .on('error', reject);
    });
  }

  private async parseExcel(filePath: string): Promise<any[]> {
    const workbook = XLSX.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    
    return XLSX.utils.sheet_to_json(worksheet);
  }

  private async parseJson(filePath: string): Promise<any[]> {
    const fs = require('fs').promises;
    const content = await fs.readFile(filePath, 'utf8');
    const jsonData = JSON.parse(content);
    
    return Array.isArray(jsonData) ? jsonData : [jsonData];
  }

  private async validateAndImport(
    data: any[], 
    tableName: string
  ): Promise<ImportResult> {
    const schema = await getTableSchema(this.companyCode, tableName);
    const pool = getCompanyDatabase(this.companyCode);
    
    const result: ImportResult = {
      success: true,
      totalRows: data.length,
      successRows: 0,
      errorRows: 0,
      errors: []
    };

    // Build column mapping
    const columns = schema.map(col => col.column_name);
    const requiredColumns = schema
      .filter(col => col.is_nullable === 'NO' && !col.column_default)
      .map(col => col.column_name);

    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      
      try {
        // Validate required fields
        const missingFields = requiredColumns.filter(
          col => !row[col] && row[col] !== 0
        );
        
        if (missingFields.length > 0) {
          throw new Error(`Missing required fields: ${missingFields.join(', ')}`);
        }

        // Prepare insert query
        const validColumns = columns.filter(col => 
          row.hasOwnProperty(col) && row[col] !== undefined && row[col] !== ''
        );
        
        const placeholders = validColumns.map((_, index) => `$${index + 1}`).join(', ');
        const values = validColumns.map(col => this.formatValue(row[col], schema.find(s => s.column_name === col)));
        
        const insertQuery = `
          INSERT INTO ${tableName} (${validColumns.join(', ')}) 
          VALUES (${placeholders})
        `;

        await pool.query(insertQuery, values);
        result.successRows++;
        
      } catch (error) {
        result.errorRows++;
        const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
        result.errors.push({
          row: i + 1,
          error: errorMessage,
          data: row
        });
      }
    }

    result.success = result.errorRows === 0;
    return result;
  }

  private formatValue(value: any, columnSchema: any): any {
    if (value === null || value === undefined || value === '') {
      return null;
    }

    switch (columnSchema.data_type) {
      case 'integer':
      case 'bigint':
        return parseInt(value);
      case 'numeric':
      case 'decimal':
      case 'real':
      case 'double precision':
        return parseFloat(value);
      case 'boolean':
        return value === 'true' || value === '1' || value === 1 || value === true;
      case 'date':
        return new Date(value).toISOString().split('T')[0];
      case 'timestamp':
      case 'timestamp with time zone':
        return new Date(value).toISOString();
      default:
        return String(value);
    }
  }
}