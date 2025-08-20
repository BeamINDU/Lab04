// pages/api/services/schemas.ts - Unified Schema Management API (Fixed)
import { NextApiRequest, NextApiResponse } from 'next';
import { getServerSession } from 'next-auth';
import { authOptions } from '../../../lib/auth';
import { DatabaseService } from '../../../lib/services/DatabaseService';
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
const CreateSchemaSchema = z.object({
  name: z.string()
    .min(1, 'Schema name is required')
    .max(63, 'Schema name too long')
    .regex(/^[a-zA-Z][a-zA-Z0-9_]*$/, 'Invalid schema name format'),
  description: z.string().optional()
});

const DropSchemaSchema = z.object({
  name: z.string().min(1, 'Schema name is required'),
  cascade: z.boolean().default(false)
});

// Main handler function
export default async function schemasHandler(req: NextApiRequest, res: NextApiResponse<ApiResponse>) {
  try {
    // Authentication check
    const session = await getServerSession(req, res, authOptions);
    if (!session?.user?.companyCode) {
      return res.status(401).json(createErrorResponse('Unauthorized'));
    }

    const { companyCode } = session.user;
    const dbService = new DatabaseService(companyCode);

    switch (req.method) {
      case 'GET':
        return await handleGetSchemas(dbService, res);
      
      case 'POST':
        return await handleCreateSchema(req, dbService, res);
      
      case 'DELETE':
        return await handleDropSchema(req, dbService, res);
      
      default:
        res.setHeader('Allow', ['GET', 'POST', 'DELETE']);
        return res.status(405).json(createErrorResponse('Method not allowed'));
    }

  } catch (error) {
    console.error('API Error:', error);
    const message = error instanceof Error ? error.message : 'Internal server error';
    return res.status(500).json(createErrorResponse(message));
  }
}

// Get schemas handler
async function handleGetSchemas(
  dbService: DatabaseService, 
  res: NextApiResponse<ApiResponse>
) {
  try {
    const schemas = await dbService.getSchemas();
    return res.status(200).json(createResponse(schemas, 'Schemas retrieved successfully'));
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Failed to fetch schemas';
    return res.status(500).json(createErrorResponse(message));
  }
}

// Create schema handler
async function handleCreateSchema(
  req: NextApiRequest,
  dbService: DatabaseService,
  res: NextApiResponse<ApiResponse>
) {
  try {
    const validatedData = CreateSchemaSchema.parse(req.body);
    
    await dbService.createSchema(validatedData.name, validatedData.description);
    
    return res.status(201).json(createResponse(
      { name: validatedData.name, description: validatedData.description },
      'Schema created successfully'
    ));
  } catch (error) {
    if (error instanceof z.ZodError) {
    const firstIssue = error.issues[0]?.message ?? 'Validation failed';
    return res.status(400).json(createErrorResponse(`Validation error: ${firstIssue}`));
    }

    
    const message = error instanceof Error ? error.message : 'Failed to create schema';
    return res.status(500).json(createErrorResponse(message));
  }
}

// Drop schema handler
async function handleDropSchema(
  req: NextApiRequest,
  dbService: DatabaseService,
  res: NextApiResponse<ApiResponse>
) {
  try {
    const validatedData = DropSchemaSchema.parse(req.body);
    
    await dbService.dropSchema(validatedData.name, validatedData.cascade);
    
    return res.status(200).json(createResponse(
      { name: validatedData.name },
      'Schema dropped successfully'
    ));
  } catch (error) {
    if (error instanceof z.ZodError) {
    const firstIssue = error.issues[0]?.message ?? 'Validation failed';
    return res.status(400).json(createErrorResponse(`Validation error: ${firstIssue}`));
    }

    
    const message = error instanceof Error ? error.message : 'Failed to drop schema';
    return res.status(500).json(createErrorResponse(message));
  }
}