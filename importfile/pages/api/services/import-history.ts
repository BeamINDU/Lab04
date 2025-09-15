// pages/api/services/import-history.ts
import { NextApiRequest, NextApiResponse } from 'next';
import { getServerSession } from 'next-auth';
import { authOptions } from '../../../lib/auth';
import { getCompanyDatabase } from '../../../lib/database';

// API Response interface
interface ApiResponse<T = any> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
  timestamp: string;
}

// Import History Record interface
interface ImportHistoryRecord {
  id: string;
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
  createdAt: string;
  createdBy: string;
  errors?: Array<{ row: number; error: string }>;
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

export default async function handler(req: NextApiRequest, res: NextApiResponse<ApiResponse>) {
  if (req.method !== 'GET') {
    res.setHeader('Allow', ['GET']);
    return res.status(405).json(createErrorResponse('Method not allowed'));
  }

  try {
    const session = await getServerSession(req, res, authOptions);
    if (!session?.user?.companyCode) {
      return res.status(401).json(createErrorResponse('Unauthorized'));
    }

    const { companyCode } = session.user;
    const pool = getCompanyDatabase(companyCode);

    // Query parameters
    const { 
      page = '1', 
      limit = '50', 
      status, 
      tableName,
      startDate,
      endDate,
      sortBy = 'createdAt',
      sortOrder = 'DESC'
    } = req.query;

    const pageNum = parseInt(page as string) || 1;
    const limitNum = parseInt(limit as string) || 50;
    const offset = (pageNum - 1) * limitNum;

    // Build query
    let query = `
      SELECT 
        id,
        file_name as "fileName",
        file_size as "fileSize",
        file_type as "fileType",
        schema_name as "schema",
        table_name as "tableName",
        total_rows as "totalRows",
        success_rows as "successRows",
        error_rows as "errorRows",
        execution_time as "executionTime",
        status,
        created_at as "createdAt",
        created_by as "createdBy",
        errors
      FROM import_history
      WHERE company_code = $1
    `;

    const queryParams: any[] = [companyCode];
    let paramCount = 1;

    // Add filters
    if (status && status !== 'ALL') {
      paramCount++;
      query += ` AND status = $${paramCount}`;
      queryParams.push(status);
    }

    if (tableName) {
      paramCount++;
      query += ` AND table_name ILIKE $${paramCount}`;
      queryParams.push(`%${tableName}%`);
    }

    if (startDate) {
      paramCount++;
      query += ` AND created_at >= $${paramCount}`;
      queryParams.push(startDate);
    }

    if (endDate) {
      paramCount++;
      query += ` AND created_at <= $${paramCount}`;
      queryParams.push(endDate);
    }

    // Add sorting
    const allowedSortColumns = ['createdAt', 'fileName', 'tableName', 'totalRows', 'status'];
    const sortColumn = allowedSortColumns.includes(sortBy as string) ? sortBy as string : 'createdAt';
    const order = sortOrder === 'ASC' ? 'ASC' : 'DESC';
    
    const columnMap: Record<string, string> = {
      'createdAt': 'created_at',
      'fileName': 'file_name',
      'tableName': 'table_name',
      'totalRows': 'total_rows',
      'status': 'status'
    };
    
    const dbColumn = columnMap[sortColumn] || 'created_at';
    query += ` ORDER BY ${dbColumn} ${order}`;

    // Add pagination
    paramCount++;
    query += ` LIMIT $${paramCount}`;
    queryParams.push(limitNum);
    
    paramCount++;
    query += ` OFFSET $${paramCount}`;
    queryParams.push(offset);

    // Execute query
    const result = await pool.query(query, queryParams);

    // Transform the data
    const records: ImportHistoryRecord[] = result.rows.map(row => ({
      ...row,
      fileSize: parseInt(row.fileSize || '0'),
      totalRows: parseInt(row.totalRows || '0'),
      successRows: parseInt(row.successRows || '0'),
      errorRows: parseInt(row.errorRows || '0'),
      executionTime: parseFloat(row.executionTime || '0'),
      errors: row.errors || []
    }));

    console.log(`📊 Found ${records.length} import history records`);
    return res.status(200).json(createResponse(records));

  } catch (error) {
    console.error('Error fetching import history:', error);
    
    // Check if it's a "table does not exist" error
    if (error instanceof Error && error.message.includes('does not exist')) {
      console.log('⚠️ import_history table not found, returning empty array');
      return res.status(200).json(createResponse([], 'Import history table not found'));
    }
    
    // For other errors, return empty array to keep UI working
    return res.status(200).json(createResponse([]));
  }
}