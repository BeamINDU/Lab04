// lib/services/ImportHistoryService.ts
import { getCompanyDatabase } from '../database';

export interface ImportHistoryData {
  fileName: string;
  fileSize: number;
  fileType: string;
  schema: string;
  tableName: string;
  totalRows: number;
  successRows: number;
  errorRows: number;
  executionTime: number;
  status: 'SUCCESS' | 'PARTIAL' | 'FAILED';
  createdBy: string;
  errors?: Array<{ row: number; error: string }>;
}

export class ImportHistoryService {
  private companyCode: string;

  constructor(companyCode: string) {
    this.companyCode = companyCode;
  }

  /**
   * Record import history to database
   */
  async recordImportHistory(data: ImportHistoryData): Promise<number> {
    const pool = getCompanyDatabase(this.companyCode);
    
    try {
      const query = `
        INSERT INTO import_history (
          company_code, file_name, file_size, file_type, schema_name, table_name,
          total_rows, success_rows, error_rows, execution_time, status, created_by, errors
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        RETURNING id
      `;

      const values = [
        this.companyCode,
        data.fileName,
        data.fileSize,
        data.fileType,
        data.schema,
        data.tableName,
        data.totalRows,
        data.successRows,
        data.errorRows,
        data.executionTime,
        data.status,
        data.createdBy,
        JSON.stringify(data.errors || [])
      ];

      const result = await pool.query(query, values);
      return result.rows[0].id;
    } catch (error) {
      console.error('Error recording import history:', error);
      // Don't throw error to not break the import process
      return -1;
    }
  }

  /**
   * Determine import status based on results
   */
  static determineStatus(totalRows: number, successRows: number, errorRows: number): 'SUCCESS' | 'PARTIAL' | 'FAILED' {
    if (errorRows === 0 && successRows === totalRows) {
      return 'SUCCESS';
    } else if (successRows === 0) {
      return 'FAILED';
    } else {
      return 'PARTIAL';
    }
  }

  /**
   * Check if import_history table exists
   */
  async checkTableExists(): Promise<boolean> {
    const pool = getCompanyDatabase(this.companyCode);
    
    try {
      const query = `
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_schema = 'public' 
          AND table_name = 'import_history'
        )
      `;
      
      const result = await pool.query(query);
      return result.rows[0].exists;
    } catch (error) {
      console.error('Error checking import_history table:', error);
      return false;
    }
  }

  /**
   * Create import_history table if it doesn't exist
   */
  async createTableIfNotExists(): Promise<void> {
    const pool = getCompanyDatabase(this.companyCode);
    
    try {
      const query = `
        CREATE TABLE IF NOT EXISTS import_history (
          id SERIAL PRIMARY KEY,
          company_code VARCHAR(50) NOT NULL,
          file_name VARCHAR(255) NOT NULL,
          file_size BIGINT DEFAULT 0,
          file_type VARCHAR(50) NOT NULL,
          schema_name VARCHAR(100) DEFAULT 'public',
          table_name VARCHAR(100) NOT NULL,
          total_rows INTEGER DEFAULT 0,
          success_rows INTEGER DEFAULT 0,
          error_rows INTEGER DEFAULT 0,
          execution_time DECIMAL(10, 2) DEFAULT 0,
          status VARCHAR(20) NOT NULL CHECK (status IN ('SUCCESS', 'PARTIAL', 'FAILED')),
          created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
          created_by VARCHAR(255) NOT NULL,
          errors JSONB DEFAULT '[]'::jsonb,
          metadata JSONB DEFAULT '{}'::jsonb
        );

        CREATE INDEX IF NOT EXISTS idx_import_history_company_code ON import_history(company_code);
        CREATE INDEX IF NOT EXISTS idx_import_history_created_at ON import_history(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_import_history_table_name ON import_history(table_name);
        CREATE INDEX IF NOT EXISTS idx_import_history_status ON import_history(status);
      `;

      await pool.query(query);
      console.log('✅ Import history table created successfully');
    } catch (error) {
      console.error('Error creating import_history table:', error);
      throw error;
    }
  }
}