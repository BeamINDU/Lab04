// pages/api/services/tables.ts - Unified Table Management API (Fixed)
import { NextApiRequest, NextApiResponse } from 'next';
import { getServerSession } from 'next-auth';
import { authOptions } from '../../../lib/auth';
import { DatabaseService, CreateTableOptions, DatabaseColumn } from '../../../lib/services/DatabaseService';
import { z } from 'zod';

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

// Validation schemas
const DatabaseColumnSchema = z.object({
  name: z.string().min(1, 'Column name is required'),
  type: z.string().min(1, 'Column type is required'),
  length: z.number().optional(),
  isPrimary: z.boolean().default(false),
  isRequired: z.boolean().default(false),
  isUnique: z.boolean().default(false),
  defaultValue: z.string().optional(),
  comment: z.string().optional(),
  references: z.object({
    table: z.string(),
    column: z.string()
  }).optional()
});

const CreateTableSchema = z.object({
  schema: z.string().default('public'),
  tableName: z.string()
    .min(1, 'Table name is required')
    .max(63, 'Table name too long')
    .regex(/^[a-zA-Z][a-zA-Z0-9_]*$/, 'Invalid table name format'),
  description: z.string().optional(),
  columns: z.array(DatabaseColumnSchema).min(1, 'At least one column is required'),
  ifNotExists: z.boolean().default(true)
});

const DropTableSchema = z.object({
  schema: z.string().default('public'),
  tableName: z.string().min(1, 'Table name is required'),
  cascade: z.boolean().default(false)
});

// Main handler function
export default async function tablesHandler(req: NextApiRequest, res: NextApiResponse<ApiResponse>) {
  try {
    // Authentication check
    const session = await getServerSession(req, res, authOptions);
    if (!session?.user?.companyCode) {
      return res.status(401).json(createErrorResponse('Unauthorized'));
    }

    const { companyCode } = session.user;
    const dbService = new DatabaseService(companyCode);

    switch (req.method) {
      case 'POST':
        return await handleCreateTable(req, dbService, res);
      
      case 'DELETE':
        return await handleDropTable(req, dbService, res);
      
      default:
        res.setHeader('Allow', ['POST', 'DELETE']);
        return res.status(405).json(createErrorResponse('Method not allowed'));
    }

  } catch (error) {
    console.error('API Error:', error);
    const message = error instanceof Error ? error.message : 'Internal server error';
    return res.status(500).json(createErrorResponse(message));
  }
}

// Create table handler
async function handleCreateTable(
  req: NextApiRequest,
  dbService: DatabaseService,
  res: NextApiResponse<ApiResponse>
) {
  try {
    const validatedData = CreateTableSchema.parse(req.body);
    
    const options: CreateTableOptions = {
      companyCode: '', // Will be handled by the service
      schema: validatedData.schema,
      tableName: validatedData.tableName,
      description: validatedData.description,
      columns: validatedData.columns,
      ifNotExists: validatedData.ifNotExists
    };
    
    await dbService.createTable(options);
    
    return res.status(201).json(createResponse(
      {
        schema: validatedData.schema,
        tableName: validatedData.tableName,
        description: validatedData.description,
        columnCount: validatedData.columns.length
      },
      'Table created successfully'
    ));
  } catch (error) {
    if (error instanceof z.ZodError) {
    const firstIssue = error.issues[0]?.message ?? 'Validation failed';
    return res.status(400).json(createErrorResponse(`Validation error: ${firstIssue}`));
    }

    
    const message = error instanceof Error ? error.message : 'Failed to create table';
    return res.status(500).json(createErrorResponse(message));
  }
}

// Drop table handler
async function handleDropTable(
  req: NextApiRequest,
  dbService: DatabaseService,
  res: NextApiResponse<ApiResponse>
) {
  try {
    const validatedData = DropTableSchema.parse(req.body);
    
    await dbService.dropTable(validatedData.schema, validatedData.tableName, validatedData.cascade);
    
    return res.status(200).json(createResponse(
      {
        schema: validatedData.schema,
        tableName: validatedData.tableName
      },
      'Table dropped successfully'
    ));
  } catch (error) {
    if (error instanceof z.ZodError) {
    const firstIssue = error.issues[0]?.message ?? 'Validation failed';
    return res.status(400).json(createErrorResponse(`Validation error: ${firstIssue}`));
    }

    
    const message = error instanceof Error ? error.message : 'Failed to drop table';
    return res.status(500).json(createErrorResponse(message));
  }
}