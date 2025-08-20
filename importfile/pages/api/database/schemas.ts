// pages/api/database/schemas.ts
import { NextApiRequest, NextApiResponse } from 'next';
import { getServerSession } from 'next-auth';
import { authOptions } from '../../../lib/auth';
import { getCompanyDatabase, testDatabaseConnection, getConnectionInfo } from '../../../lib/database';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  console.log(`📍 API ${req.method} /api/database/schemas called`);
  
  try {
    // Authentication check
    const session = await getServerSession(req, res, authOptions);
    if (!session?.user) {
      console.log('❌ Unauthorized access attempt');
      return res.status(401).json({ error: 'Unauthorized' });
    }

    console.log(`✅ Authenticated user: ${session.user.email}`);
    console.log(`🏢 Company: ${session.user.companyCode}`);

    // Get company code from session
    const companyCode = session.user.companyCode;
    if (!companyCode) {
      console.log('❌ No company code in session');
      return res.status(400).json({ error: 'Company code not found in session' });
    }

    // Log connection info for debugging
    const connInfo = getConnectionInfo(companyCode);
    console.log('🔧 Database connection info:', connInfo);

    if (req.method === 'GET') {
      return await handleGetSchemas(req, res, session, companyCode);
    } else if (req.method === 'POST') {
      return await handleCreateSchema(req, res, session, companyCode);
    } else {
      res.setHeader('Allow', ['GET', 'POST']);
      return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
    }

  } catch (error) {
    console.error('❌ API Error:', error);
    return res.status(500).json({
      error: 'Internal server error',
      details: error instanceof Error ? error.message : 'Unknown error'
    });
  }
}

async function handleGetSchemas(req: NextApiRequest, res: NextApiResponse, session: any, companyCode: string) {
  console.log('📋 Getting schemas for company:', companyCode);
  
  try {
    // Test connection first
    const isConnected = await testDatabaseConnection(companyCode);
    if (!isConnected) {
      console.log('❌ Database connection failed, returning fallback data');
      return returnFallbackData(res, companyCode, 'Database connection failed');
    }

    // Get database pool
    const pool = getCompanyDatabase(companyCode);
    
    // Get all schemas with their tables
    const schemasQuery = `
      SELECT 
        schema_name,
        obj_description(oid, 'pg_namespace') as description
      FROM information_schema.schemata s
      LEFT JOIN pg_namespace n ON n.nspname = s.schema_name
      WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
      ORDER BY 
        CASE 
          WHEN schema_name = 'public' THEN 0 
          ELSE 1 
        END,
        schema_name;
    `;

    const tablesQuery = `
      SELECT 
        table_schema,
        table_name,
        obj_description(pgc.oid, 'pg_class') as table_comment
      FROM information_schema.tables t
      LEFT JOIN pg_class pgc ON pgc.relname = t.table_name
      WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
      AND table_type = 'BASE TABLE'
      ORDER BY table_schema, table_name;
    `;

    console.log('🔍 Executing database queries...');
    const [schemasResult, tablesResult] = await Promise.all([
      pool.query(schemasQuery),
      pool.query(tablesQuery)
    ]);

    console.log(`📊 Found ${schemasResult.rows.length} schemas and ${tablesResult.rows.length} tables`);

    // Group tables by schema
    const tablesBySchema = tablesResult.rows.reduce((acc: any, table: any) => {
      if (!acc[table.table_schema]) {
        acc[table.table_schema] = [];
      }
      acc[table.table_schema].push({
        name: table.table_name,
        comment: table.table_comment || `Table in ${table.table_schema} schema`
      });
      return acc;
    }, {});

    // Combine schemas with their tables
    const schemas = schemasResult.rows.map((schema: any) => ({
      name: schema.schema_name,
      description: schema.description || (schema.schema_name === 'public' ? 'Default public schema' : `Custom schema: ${schema.schema_name}`),
      type: schema.schema_name === 'public' ? 'default' : 'custom',
      tables: (tablesBySchema[schema.schema_name] || []).map((t: any) => t.name),
      tableDetails: tablesBySchema[schema.schema_name] || [],
      createdAt: new Date().toISOString()
    }));

    console.log('✅ Schemas processed successfully');

    return res.status(200).json({
      success: true,
      schemas,
      company: companyCode,
      mode: 'live',
      connectionInfo: getConnectionInfo(companyCode)
    });

  } catch (error) {
    console.error('❌ Database error:', error);
    return returnFallbackData(res, companyCode, `Database error: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

async function handleCreateSchema(req: NextApiRequest, res: NextApiResponse, session: any, companyCode: string) {
  console.log('➕ Creating new schema for company:', companyCode);
  
  const { name, description } = req.body;
  
  if (!name || name.trim() === '') {
    return res.status(400).json({ error: 'Schema name is required' });
  }

  // Validate schema name
  const schemaNameRegex = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
  if (!schemaNameRegex.test(name)) {
    return res.status(400).json({ 
      error: 'Invalid schema name. Use only letters, numbers, and underscores. Must start with letter or underscore.' 
    });
  }

  try {
    // Test connection first
    const isConnected = await testDatabaseConnection(companyCode);
    if (!isConnected) {
      return res.status(500).json({ 
        error: 'Database connection failed',
        mode: 'offline'
      });
    }

    const pool = getCompanyDatabase(companyCode);
    
    // Create schema
    const createSchemaSQL = `CREATE SCHEMA IF NOT EXISTS "${name}";`;
    await pool.query(createSchemaSQL);

    // Add comment if description provided
    if (description && description.trim()) {
      const commentSQL = `COMMENT ON SCHEMA "${name}" IS $1;`;
      await pool.query(commentSQL, [description]);
    }

    console.log(`✅ Schema "${name}" created for company ${companyCode} by ${session.user.email}`);

    return res.status(201).json({ 
      success: true, 
      message: `Schema "${name}" created successfully`,
      schema: { 
        name, 
        description: description || null,
        type: 'custom',
        companyCode 
      }
    });

  } catch (error) {
    console.error('❌ Error creating schema:', error);
    
    if (error instanceof Error && error.message.includes('already exists')) {
      return res.status(409).json({
        error: `Schema "${name}" already exists`,
        details: error.message
      });
    }

    return res.status(500).json({
      error: 'Failed to create schema',
      details: error instanceof Error ? error.message : 'Unknown error'
    });
  }
}

function returnFallbackData(res: NextApiResponse, companyCode: string, reason: string) {
  console.log('🔄 Returning fallback data for company:', companyCode);
  
  const fallbackSchemas = [
    {
      name: 'public',
      type: 'default',
      description: 'Default public schema (fallback - no connection)',
      tables: [],
      tableDetails: [],
      createdAt: new Date().toISOString()
    }
  ];

  return res.status(200).json({
    success: true,
    schemas: fallbackSchemas,
    company: companyCode,
    mode: 'fallback',
    warning: reason
  });
}