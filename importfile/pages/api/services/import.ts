// pages/api/services/import.ts - Fixed Validation for Form Data
import { NextApiRequest, NextApiResponse } from 'next';
import { getServerSession } from 'next-auth';
import { authOptions } from '../../../lib/auth';
import { DatabaseService, DatabaseColumn } from '../../../lib/services/DatabaseService'; // เพิ่ม DatabaseColumn import
import { FileImportService } from '../../../lib/services/FileImportService';
import multer from 'multer';
import { z } from 'zod';
import { getCompanyDatabase } from '../../../lib/database';
// API Response interface
interface ApiResponse<T = any> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
  timestamp: string;
}

// Helper functions
function createResponse<T>(data: T, message?: string): ApiResponse<T> {
  return {
    success: true,
    data,
    message,
    timestamp: new Date().toISOString()
  };
}

function createErrorResponse(error: string): ApiResponse {
  return {
    success: false,
    error,
    timestamp: new Date().toISOString()
  };
}

// Middleware helper function
function runMiddleware(req: any, res: any, fn: any): Promise<any> {
  return new Promise((resolve, reject) => {
    fn(req, res, (result: any) => {
      if (result instanceof Error) {
        return reject(result);
      }
      return resolve(result);
    });
  });
}

// Configure multer for file upload with enhanced settings
const upload = multer({
  dest: 'uploads/',
  limits: {
    fileSize: 100 * 1024 * 1024, // 100MB limit
    files: 1
  },
  fileFilter: (req, file, cb) => {
    const allowedTypes = [
      'text/csv',
      'application/vnd.ms-excel',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'text/plain',
      'application/json',
      'text/tab-separated-values'
    ];
    
    if (allowedTypes.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error('File type not supported'));
    }
  }
});

/**
 * Enhanced Validation Schema สำหรับ Form Data
 * ใช้ coercion และ preprocessing เพื่อจัดการกับ string inputs
 */
const ImportDataSchema = z.object({
  schema: z.string().min(1, 'Schema name is required').default('public'),
  tableName: z.string().min(1, 'Table name is required'),
  
  // เพิ่ม column mapping support (ชื่อใหม่ที่ frontend ส่งมา)
  columnMapping: z.string().optional(), // JSON string of DatabaseColumn[]
  
  // เก็บ customColumns ไว้เพื่อ backward compatibility
  customColumns: z.string().optional(), // JSON string of DatabaseColumn[]
  
  createTable: z.preprocess(
    (val) => {
      if (typeof val === 'string') {
        return val.toLowerCase() === 'true' || val === '1';
      }
      return Boolean(val);
    },
    z.boolean().default(false)
  ),
  
  truncateBeforeImport: z.preprocess(
    (val) => {
      if (typeof val === 'string') {
        return val.toLowerCase() === 'true' || val === '1';
      }
      return Boolean(val);
    },
    z.boolean().default(false)
  ),
  
  skipErrors: z.preprocess(
    (val) => {
      if (typeof val === 'string') {
        return val.toLowerCase() === 'true' || val === '1';
      }
      return Boolean(val);
    },
    z.boolean().default(true)
  ),
  
  batchSize: z.preprocess(
    (val) => {
      if (typeof val === 'string') {
        const parsed = parseInt(val, 10);
        return isNaN(parsed) ? undefined : parsed;
      }
      return val;
    },
    z.number().int().min(1).max(10000).default(1000)
  )
});

// Disable Next.js body parser for file upload
export const config = {
  api: {
    bodyParser: false,
  },
};

// Main handler function
export default async function importHandler(req: NextApiRequest, res: NextApiResponse<ApiResponse>) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', ['POST']);
    return res.status(405).json(createErrorResponse('Method not allowed'));
  }

  try {
    const session = await getServerSession(req, res, authOptions);
    if (!session?.user?.companyCode) {
      return res.status(401).json(createErrorResponse('Unauthorized'));
    }

    const { companyCode } = session.user;

    await runMiddleware(req, res, upload.single('file'));
    const { file, body } = req as any;
    
    if (!file) {
      return res.status(400).json(createErrorResponse('No file uploaded'));
    }

    console.log('📥 Raw form data received:', {
      bodyKeys: Object.keys(body),
      columnMapping: body.columnMapping ? 'Present' : 'Not provided',
      customColumns: body.customColumns ? 'Present' : 'Not provided'
    });

    let validatedData;
    try {
      validatedData = ImportDataSchema.parse(body);
      console.log('✅ Validation successful:', {
        ...validatedData,
        columnMapping: validatedData.columnMapping ? 'Column mapping provided' : 'Using auto-detected',
        customColumns: validatedData.customColumns ? 'Custom columns provided' : 'Using auto-detected'
      });
    } catch (validationError) {
      console.error('❌ Validation failed:', validationError);
      if (validationError instanceof z.ZodError) {
        const errorMessages = validationError.issues.map(issue => 
          `${issue.path.join('.')}: ${issue.message}`
        );
        return res.status(400).json(createErrorResponse(
          `Validation failed: ${errorMessages.join(', ')}`
        ));
      }
      throw validationError;
    }
    
    const dbService = new DatabaseService(companyCode);
    const importService = new FileImportService(dbService);

    // Parse column mapping (ให้ความสำคัญกับ columnMapping ก่อน แล้วค่อย fallback ไป customColumns)
    let customColumns: DatabaseColumn[] | undefined;
    
    // ลองใช้ columnMapping ก่อน (parameter ใหม่จาก frontend)
    if (validatedData.columnMapping) {
      try {
        customColumns = JSON.parse(validatedData.columnMapping);
        // เพิ่ม type guard เพื่อป้องกัน undefined
        if (customColumns && Array.isArray(customColumns)) {
          console.log('📋 Using column mapping from frontend:', customColumns.map(col => `${col.name}:${col.type}`));
        }
      } catch (parseError) {
        console.warn('⚠️ Failed to parse columnMapping, trying customColumns fallback');
        customColumns = undefined;
      }
    }
    
    // ถ้า columnMapping ไม่มีหรือ parse ไม่ได้ ให้ลอง customColumns (backward compatibility)
    if (!customColumns && validatedData.customColumns) {
      try {
        customColumns = JSON.parse(validatedData.customColumns);
        // เพิ่ม type guard เพื่อป้องกัน undefined
        if (customColumns && Array.isArray(customColumns)) {
          console.log('📋 Using custom columns (fallback):', customColumns.map(col => `${col.name}:${col.type}`));
        }
      } catch (parseError) {
        console.warn('⚠️ Failed to parse customColumns, using auto-detection');
        customColumns = undefined;
      }
    }

    console.log(`📥 Starting import: ${file.originalname} to ${validatedData.schema}.${validatedData.tableName}`);
    console.log(`🔧 Column strategy: ${customColumns ? 'Custom columns' : 'Auto-detection'}`);

    // Execute import with custom columns
    const result = await importService.importFileWithCustomColumns({
      filePath: file.path,
      fileName: file.originalname,
      mimeType: file.mimetype,
      schema: validatedData.schema,
      tableName: validatedData.tableName,
      createTable: validatedData.createTable,
      truncateBeforeImport: validatedData.truncateBeforeImport,
      skipErrors: validatedData.skipErrors,
      batchSize: validatedData.batchSize,
      customColumns: customColumns // ส่ง custom columns ไปด้วย
    });
    try {
      const historyQuery = `
        INSERT INTO import_history (
          company_code, 
          file_name, 
          file_size, 
          file_type,
          schema_name, 
          table_name, 
          total_rows, 
          success_rows,
          error_rows, 
          execution_time, 
          status, 
          created_by, 
          errors
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
      `;

      // Determine status based on results
      let status: 'SUCCESS' | 'PARTIAL' | 'FAILED';
      if (result.errorRows === 0 && result.successRows === result.totalRows) {
        status = 'SUCCESS';
      } else if (result.successRows === 0) {
        status = 'FAILED';
      } else {
        status = 'PARTIAL';
      }

      // Get file type from mime type or file extension
      let fileType = 'unknown';
      if (file.mimetype.includes('csv')) {
        fileType = 'csv';
      } else if (file.mimetype.includes('excel') || file.mimetype.includes('spreadsheet')) {
        fileType = 'excel';
      } else if (file.mimetype.includes('json')) {
        fileType = 'json';
      } else if (file.originalname) {
        const ext = file.originalname.split('.').pop()?.toLowerCase();
        if (ext === 'csv') fileType = 'csv';
        else if (ext === 'xlsx' || ext === 'xls') fileType = 'excel';
        else if (ext === 'json') fileType = 'json';
      }

      // Prepare errors array (limit to first 100 errors for storage)
      const errorsToStore = result.errors ? result.errors.slice(0, 100) : [];

      const historyValues = [
        companyCode,                                    // company_code
        file.originalname || 'unknown',                 // file_name
        file.size || 0,                                  // file_size
        fileType,                                        // file_type
        validatedData.schema,                           // schema_name
        validatedData.tableName,                        // table_name
        result.totalRows || 0,                          // total_rows
        result.successRows || 0,                        // success_rows
        result.errorRows || 0,                          // error_rows
        (result.executionTime || 0) / 1000,            // execution_time (convert ms to seconds)
        status,                                          // status
        session.user.email || 'unknown',                // created_by
        JSON.stringify(errorsToStore)                   // errors
      ];

      const pool = getCompanyDatabase(companyCode);
      await pool.query(historyQuery, historyValues);
      
      console.log(`✅ Import history recorded: ${status} - ${result.successRows}/${result.totalRows} rows`);
    } catch (historyError) {
      // Log error but don't fail the import
      console.error('⚠️ Failed to record import history:', historyError);
    }
    console.log('✅ Import completed:', {
      success: result.success,
      totalRows: result.totalRows,
      successRows: result.successRows,
      errorRows: result.errorRows,
      usingCustomColumns: !!customColumns
    });

    return res.status(200).json(createResponse(result, 'File imported successfully'));

  } catch (error) {
    console.error('❌ Import Error:', error);
    
    if (error instanceof z.ZodError) {
      const firstIssue = error.issues[0];
      const errorMsg = firstIssue ? `${firstIssue.path.join('.')}: ${firstIssue.message}` : 'Validation failed';
      return res.status(400).json(createErrorResponse(`Validation error: ${errorMsg}`));
    }
    
    if (error instanceof Error) {
      if (error.message.includes('File type not supported')) {
        return res.status(400).json(createErrorResponse('File type not supported. Please upload CSV, Excel, or JSON files.'));
      }
      
      if (error.message.includes('File size too large')) {
        return res.status(400).json(createErrorResponse('File size too large. Maximum size is 100MB.'));
      }
      
      return res.status(500).json(createErrorResponse(`Import failed: ${error.message}`));
    }
    
    return res.status(500).json(createErrorResponse('Internal server error during import'));
  }
}
/**
 * Utility functions for handling form data conversion
 */
export class FormDataConverter {
  /**
   * Convert string boolean values to actual booleans
   * Handles various formats: 'true', 'false', '1', '0', 'on', 'off'
   */
  static stringToBoolean(value: unknown): boolean {
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') {
      const lowercased = value.toLowerCase().trim();
      return lowercased === 'true' || lowercased === '1' || lowercased === 'on' || lowercased === 'yes';
    }
    if (typeof value === 'number') {
      return value > 0;
    }
    return false;
  }

  /**
   * Convert string numbers to actual numbers
   * Handles integer and float conversion with validation
   */
  static stringToNumber(value: unknown, options: { 
    allowFloat?: boolean; 
    min?: number; 
    max?: number; 
  } = {}): number | undefined {
    if (typeof value === 'number') return value;
    if (typeof value === 'string') {
      const parsed = options.allowFloat ? parseFloat(value) : parseInt(value, 10);
      if (isNaN(parsed)) return undefined;
      
      if (options.min !== undefined && parsed < options.min) return undefined;
      if (options.max !== undefined && parsed > options.max) return undefined;
      
      return parsed;
    }
    return undefined;
  }

  /**
   * Safe string trimming and validation
   */
  static safeString(value: unknown, options: {
    minLength?: number;
    maxLength?: number;
    allowEmpty?: boolean;
  } = {}): string | undefined {
    if (typeof value !== 'string') return undefined;
    
    const trimmed = value.trim();
    
    if (!options.allowEmpty && trimmed.length === 0) return undefined;
    if (options.minLength && trimmed.length < options.minLength) return undefined;
    if (options.maxLength && trimmed.length > options.maxLength) return undefined;
    
    return trimmed;
  }
}