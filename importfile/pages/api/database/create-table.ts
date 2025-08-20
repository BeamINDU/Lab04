// pages/api/database/create-table.ts
import { NextApiRequest, NextApiResponse } from 'next';
import { getServerSession } from 'next-auth';
import { authOptions } from '../../../lib/auth';
import { getCompanyDatabase } from '../../../lib/database';

interface Column {
  name: string;
  type: string;
  length?: number;
  isPrimary: boolean;
  isRequired: boolean;
  isUnique: boolean;
  defaultValue?: string;
}

interface CreateTableRequest {
  schema: string;
  name: string;
  description?: string;
  columns: Column[];
  importFile?: string;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const session = await getServerSession(req, res, authOptions);
    if (!session?.user) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const companyCode = session.user.companyCode;
    if (!companyCode) {
      return res.status(400).json({ error: 'Company code not found in session' });
    }

    const { schema, name, description, columns, importFile }: CreateTableRequest = req.body;

    // Validation
    if (!name || !columns || columns.length === 0) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Validate table name
    const tableNameRegex = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
    if (!tableNameRegex.test(name)) {
      return res.status(400).json({ 
        error: 'Invalid table name. Use only letters, numbers, and underscores. Must start with letter or underscore.' 
      });
    }

    // Validate column names
    for (const column of columns) {
      if (!column.name || !tableNameRegex.test(column.name)) {
        return res.status(400).json({ 
          error: `Invalid column name: ${column.name}. Use only letters, numbers, and underscores.` 
        });
      }
    }

    console.log(`🏗️ Creating table "${schema}"."${name}" for company ${companyCode}`);

    const pool = getCompanyDatabase(companyCode);
    const client = await pool.connect();

    try {
      await client.query('BEGIN');

      // Check if table already exists
      const existsQuery = `
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_schema = $1 AND table_name = $2
        );
      `;
      const existsResult = await client.query(existsQuery, [schema, name]);
      
      if (existsResult.rows[0].exists) {
        throw new Error(`Table "${schema}"."${name}" already exists`);
      }

      // Build CREATE TABLE SQL
      const columnDefinitions = columns.map(col => {
        let colDef = `"${col.name}" ${col.type}`;
        
        // Add length for VARCHAR/CHAR
        if (col.length && ['VARCHAR', 'CHAR'].includes(col.type.toUpperCase())) {
          colDef += `(${col.length})`;
        }
        
        // Add constraints
        if (col.isRequired && !col.isPrimary) {
          colDef += ' NOT NULL';
        }
        
        if (col.isUnique && !col.isPrimary) {
          colDef += ' UNIQUE';
        }
        
        // Add default value
        if (col.defaultValue && col.defaultValue.trim() !== '') {
          // Handle different default value types
          if (col.type === 'BOOLEAN') {
            colDef += ` DEFAULT ${col.defaultValue.toLowerCase()}`;
          } else if (['INTEGER', 'BIGINT', 'DECIMAL', 'NUMERIC', 'REAL', 'DOUBLE PRECISION'].includes(col.type)) {
            colDef += ` DEFAULT ${col.defaultValue}`;
          } else {
            colDef += ` DEFAULT '${col.defaultValue}'`;
          }
        }
        
        return colDef;
      });

      // Add primary key constraint
      const primaryKeys = columns.filter(col => col.isPrimary).map(col => `"${col.name}"`);
      if (primaryKeys.length > 0) {
        columnDefinitions.push(`PRIMARY KEY (${primaryKeys.join(', ')})`);
      }

      // Add foreign key constraints (if any)
      // This would be extended based on your requirements

      const createTableSQL = `
        CREATE TABLE "${schema}"."${name}" (
          ${columnDefinitions.join(',\n          ')}
        );
      `;

      console.log('📝 Executing SQL:', createTableSQL);
      await client.query(createTableSQL);

      // Add table comment if description provided
      if (description && description.trim() !== '') {
        const commentSQL = `COMMENT ON TABLE "${schema}"."${name}" IS $1;`;
        await client.query(commentSQL, [description]);
      }

      // Add column comments for special columns
      for (const column of columns) {
        if (column.isPrimary) {
          const columnCommentSQL = `COMMENT ON COLUMN "${schema}"."${name}"."${column.name}" IS 'Primary key';`;
          await client.query(columnCommentSQL);
        }
      }

      await client.query('COMMIT');

      console.log(`✅ Table "${schema}"."${name}" created successfully for company ${companyCode}`);

      // Log the creation
      const logData = {
        action: 'CREATE_TABLE',
        companyCode,
        schema,
        tableName: name,
        columns: columns.length,
        importFile: importFile || null,
        user: session.user.email,
        timestamp: new Date().toISOString()
      };

      res.status(201).json({
        success: true,
        message: `Table "${name}" created successfully in schema "${schema}"`,
        table: {
          schema,
          name,
          description,
          columns: columns.length,
          primaryKeys: primaryKeys.length,
          importFile
        },
        sql: createTableSQL,
        log: logData
      });

    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

  } catch (error) {
    console.error('❌ Error creating table:', error);
    
    if (error instanceof Error) {
      if (error.message.includes('already exists')) {
        return res.status(409).json({
          error: 'Table already exists',
          details: error.message
        });
      }
      
      if (error.message.includes('syntax error')) {
        return res.status(400).json({
          error: 'SQL syntax error',
          details: error.message
        });
      }
    }

    return res.status(500).json({
      error: 'Failed to create table',
      details: error instanceof Error ? error.message : 'Unknown error'
    });
  }
}