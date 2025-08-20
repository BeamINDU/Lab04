// api/database.ts - API functions for database operations
import { Pool } from 'pg';

// Database connection configuration
const pool = new Pool({
  user: process.env.DB_USER || 'postgres',
  host: process.env.DB_HOST || 'localhost',
  database: process.env.DB_NAME || 'siamtemp_company_a',
  password: process.env.DB_PASSWORD || 'your_password',
  port: parseInt(process.env.DB_PORT || '5432'),
});

// API functions that should replace mock functions
export const databaseAPI = {
  // Get all schemas and their tables
  async getSchemas() {
    const client = await pool.connect();
    try {
      const schemasQuery = `
        SELECT 
          schema_name,
          CASE 
            WHEN schema_name IN ('public', 'information_schema', 'pg_catalog') 
            THEN 'default' 
            ELSE 'custom' 
          END as type
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name;
      `;
      
      const schemasResult = await client.query(schemasQuery);
      const schemas = [];

      for (const schema of schemasResult.rows) {
        const tablesQuery = `
          SELECT table_name 
          FROM information_schema.tables 
          WHERE table_schema = $1 
          AND table_type = 'BASE TABLE'
          ORDER BY table_name;
        `;
        
        const tablesResult = await client.query(tablesQuery, [schema.schema_name]);
        
        schemas.push({
          name: schema.schema_name,
          type: schema.type,
          tables: tablesResult.rows.map(row => row.table_name),
          createdAt: new Date().toISOString()
        });
      }

      return schemas;
    } finally {
      client.release();
    }
  },

  // Create new schema
  async createSchema(name: string, description?: string) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      
      // Create schema
      await client.query(`CREATE SCHEMA IF NOT EXISTS ${name}`);
      
      // Add comment if description provided
      if (description) {
        await client.query(
          `COMMENT ON SCHEMA ${name} IS $1`,
          [description]
        );
      }
      
      await client.query('COMMIT');
      return { success: true, message: `Schema "${name}" created successfully` };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  },

  // Create new table
  async createTable(tableInfo: {
    name: string;
    schema: string;
    description?: string;
    columns: Array<{
      name: string;
      type: string;
      length?: number;
      isPrimary: boolean;
      isRequired: boolean;
      isUnique: boolean;
      defaultValue: string;
      references?: { table: string; column: string };
    }>;
  }) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      
      // Build CREATE TABLE SQL
      let columnDefinitions = tableInfo.columns.map(col => {
        let colDef = `${col.name} ${col.type}`;
        
        if (col.length && ['VARCHAR', 'CHAR'].includes(col.type.toUpperCase())) {
          colDef += `(${col.length})`;
        }
        
        if (col.isRequired && !col.isPrimary) {
          colDef += ' NOT NULL';
        }
        
        if (col.isUnique && !col.isPrimary) {
          colDef += ' UNIQUE';
        }
        
        if (col.defaultValue) {
          colDef += ` DEFAULT ${col.defaultValue}`;
        }
        
        return colDef;
      }).join(', ');
      
      // Add primary key constraint
      const primaryKeys = tableInfo.columns
        .filter(col => col.isPrimary)
        .map(col => col.name);
      
      if (primaryKeys.length > 0) {
        columnDefinitions += `, PRIMARY KEY (${primaryKeys.join(', ')})`;
      }
      
      // Add foreign key constraints
      const foreignKeys = tableInfo.columns
        .filter(col => col.references)
        .map(col => 
          `FOREIGN KEY (${col.name}) REFERENCES ${col.references!.table}(${col.references!.column})`
        );
      
      if (foreignKeys.length > 0) {
        columnDefinitions += `, ${foreignKeys.join(', ')}`;
      }
      
      const createTableSQL = `
        CREATE TABLE ${tableInfo.schema}.${tableInfo.name} (
          ${columnDefinitions}
        )
      `;
      
      await client.query(createTableSQL);
      
      // Add table comment
      if (tableInfo.description) {
        await client.query(
          `COMMENT ON TABLE ${tableInfo.schema}.${tableInfo.name} IS $1`,
          [tableInfo.description]
        );
      }
      
      await client.query('COMMIT');
      return { 
        success: true, 
        message: `Table "${tableInfo.name}" created successfully in schema "${tableInfo.schema}"` 
      };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  },

  // Import data from file
  async importData(schemaName: string, tableName: string, data: any[], options: {
    hasHeader: boolean;
    onDuplicate: 'skip' | 'update' | 'error';
  }) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      
      // Get table structure
      const columnsQuery = `
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
      `;
      
      const columnsResult = await client.query(columnsQuery, [schemaName, tableName]);
      const columns = columnsResult.rows;
      
      if (columns.length === 0) {
        throw new Error(`Table ${schemaName}.${tableName} not found`);
      }
      
      // Process and insert data
      let insertedRows = 0;
      const columnNames = columns.map(col => col.column_name);
      
      for (const row of data) {
        const values = columnNames.map(colName => row[colName] || null);
        const placeholders = values.map((_, index) => `$${index + 1}`).join(', ');
        
        const insertSQL = `
          INSERT INTO ${schemaName}.${tableName} (${columnNames.join(', ')})
          VALUES (${placeholders})
          ${options.onDuplicate === 'skip' ? 'ON CONFLICT DO NOTHING' : ''}
        `;
        
        try {
          await client.query(insertSQL, values);
          insertedRows++;
        } catch (error) {
          if (options.onDuplicate === 'error') {
            throw error;
          }
          // Skip on conflict if onDuplicate is 'skip'
        }
      }
      
      await client.query('COMMIT');
      return { 
        success: true, 
        message: `Imported ${insertedRows} rows successfully into ${schemaName}.${tableName}`,
        insertedRows 
      };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
};

// Close pool when application shuts down
process.on('SIGTERM', () => {
  pool.end();
});

process.on('SIGINT', () => {
  pool.end();
});