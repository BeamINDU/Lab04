// lib/database.ts
import { Pool, PoolConfig } from 'pg';

// Database configuration for each company (แก้ไข IPv6 issue)
const companyDatabaseConfig: Record<string, PoolConfig> = {
  company_a: {
    user: process.env.POSTGRES_USER || 'postgres',
    // บังคับใช้ IPv4 เท่านั้น
    host: process.env.NODE_ENV === 'production' ? 'postgres-company-a' : '127.0.0.1',
    database: 'siamtemp_company_a',
    password: process.env.POSTGRES_PASSWORD || 'password123',
    port: 5432,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  },
  company_b: {
    user: process.env.POSTGRES_USER || 'postgres',
    host: process.env.NODE_ENV === 'production' ? 'postgres-company-b' : '127.0.0.1',
    database: 'siamtemp_company_b',
    password: process.env.POSTGRES_PASSWORD || 'password123',
    port: process.env.NODE_ENV === 'production' ? 5432 : 5433,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  },
  company_c: {
    user: process.env.POSTGRES_USER || 'postgres',
    host: process.env.NODE_ENV === 'production' ? 'postgres-company-c' : '127.0.0.1',
    database: 'siamtemp_company_c',
    password: process.env.POSTGRES_PASSWORD || 'password123',
    port: process.env.NODE_ENV === 'production' ? 5432 : 5434,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  }
};

// Pool instances cache
const poolInstances = new Map<string, Pool>();

/**
 * Get database connection pool for specific company
 */
export function getCompanyDatabase(companyCode: string): Pool {
  // Check if pool already exists
  if (poolInstances.has(companyCode)) {
    const existingPool = poolInstances.get(companyCode)!;
    return existingPool;
  }

  // Get configuration for company
  const config = companyDatabaseConfig[companyCode];
  if (!config) {
    throw new Error(`Database configuration not found for company: ${companyCode}`);
  }

  // Create new pool
  const pool = new Pool(config);

  // Handle pool errors
  pool.on('error', (err) => {
    console.error(`Database pool error for ${companyCode}:`, err);
  });

  // Cache the pool
  poolInstances.set(companyCode, pool);

  console.log(`✅ Database pool created for ${companyCode} on port ${config.port}`);
  return pool;
}

/**
 * Test database connection
 */
export async function testDatabaseConnection(companyCode: string): Promise<boolean> {
  try {
    const pool = getCompanyDatabase(companyCode);
    const client = await pool.connect();
    
    // Simple query to test connection
    const result = await client.query('SELECT NOW() as current_time');
    client.release();
    
    console.log(`✅ Database connection test passed for ${companyCode}:`, result.rows[0]);
    return true;
    
  } catch (error) {
    console.error(`❌ Database connection test failed for ${companyCode}:`, error);
    return false;
  }
}

/**
 * Get all tables for a company
 */
export async function getAllTables(companyCode: string) {
  const pool = getCompanyDatabase(companyCode);
  
  const query = `
    SELECT 
      table_name,
      obj_description(pgc.oid, 'pg_class') as table_comment
    FROM information_schema.tables t
    LEFT JOIN pg_class pgc ON pgc.relname = t.table_name
    WHERE table_schema = 'public' 
    AND table_type = 'BASE TABLE'
    ORDER BY table_name;
  `;
  
  const result = await pool.query(query);
  return result.rows;
}

/**
 * Get table schema information for import validation
 */
export async function getTableSchema(companyCode: string, tableName: string) {
  const pool = getCompanyDatabase(companyCode);
  
  const query = `
    SELECT
      column_name,
      data_type,
      is_nullable,
      column_default,
      character_maximum_length,
      numeric_precision,
      numeric_scale,
      udt_name
    FROM information_schema.columns
    WHERE table_name = $1
    AND table_schema = 'public'
    ORDER BY ordinal_position;
  `;
  
  const result = await pool.query(query, [tableName]);
  return result.rows;
}

/**
 * Get detailed table structure with constraints and indexes
 */
export async function getTableStructure(companyCode: string, tableName: string, schemaName: string = 'public') {
  const pool = getCompanyDatabase(companyCode);
  
  // Get columns
  const columnsQuery = `
    SELECT
      column_name,
      data_type,
      character_maximum_length,
      is_nullable,
      column_default,
      is_identity,
      identity_generation
    FROM information_schema.columns
    WHERE table_name = $1 AND table_schema = $2
    ORDER BY ordinal_position;
  `;
  
  // Get constraints
  const constraintsQuery = `
    SELECT
      tc.constraint_name,
      tc.constraint_type,
      kcu.column_name,
      ccu.table_name AS foreign_table_name,
      ccu.column_name AS foreign_column_name
    FROM information_schema.table_constraints tc
    LEFT JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    LEFT JOIN information_schema.constraint_column_usage ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
    WHERE tc.table_name = $1 AND tc.table_schema = $2;
  `;
  
  // Get indexes
  const indexesQuery = `
    SELECT indexname, indexdef
    FROM pg_indexes
    WHERE tablename = $1 AND schemaname = $2;
  `;
  
  try {
    const [columnsResult, constraintsResult, indexesResult] = await Promise.all([
      pool.query(columnsQuery, [tableName, schemaName]),
      pool.query(constraintsQuery, [tableName, schemaName]),
      pool.query(indexesQuery, [tableName, schemaName])
    ]);
    
    return {
      name: tableName,
      schema: schemaName,
      columns: columnsResult.rows,
      constraints: constraintsResult.rows,
      indexes: indexesResult.rows
    };
  } catch (error) {
    console.error(`Error getting table structure for ${tableName}:`, error);
    throw error;
  }
}

/**
 * Create new table
 */
export async function createTable(
  companyCode: string,
  tableInfo: {
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
      defaultValue?: string;
      references?: { table: string; column: string };
    }>;
  }
) {
  const pool = getCompanyDatabase(companyCode);
  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');
    
    // Build CREATE TABLE SQL
    let columnDefinitions = tableInfo.columns.map(col => {
      let colDef = `"${col.name}" ${col.type}`;
      
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
      .map(col => `"${col.name}"`);
    
    if (primaryKeys.length > 0) {
      columnDefinitions += `, PRIMARY KEY (${primaryKeys.join(', ')})`;
    }
    
    // Add foreign key constraints
    const foreignKeys = tableInfo.columns
      .filter(col => col.references)
      .map(col => 
        `FOREIGN KEY ("${col.name}") REFERENCES "${col.references!.table}"("${col.references!.column}")`
      );
    
    if (foreignKeys.length > 0) {
      columnDefinitions += `, ${foreignKeys.join(', ')}`;
    }
    
    const createTableSQL = `
      CREATE TABLE "${tableInfo.schema}"."${tableInfo.name}" (
        ${columnDefinitions}
      )
    `;
    
    await client.query(createTableSQL);
    
    // Add table comment
    if (tableInfo.description) {
      await client.query(
        `COMMENT ON TABLE "${tableInfo.schema}"."${tableInfo.name}" IS $1`,
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
}

/**
 * Close all database connections
 */
export async function closeAllConnections(): Promise<void> {
  const promises: Promise<void>[] = [];
  
  // แก้ไข: ใช้ forEach แทน for...of เพื่อหลีกเลี่ยงปัญหา downlevelIteration
  poolInstances.forEach((pool, companyCode) => {
    promises.push(
      pool.end().then(() => {
        console.log(`🔌 Database pool closed for ${companyCode}`);
      }).catch((error) => {
        console.error(`❌ Error closing pool for ${companyCode}:`, error);
      })
    );
  });
  
  await Promise.allSettled(promises);
  poolInstances.clear();
}

/**
 * Get connection info for debugging
 */
export function getConnectionInfo(companyCode: string) {
  const config = companyDatabaseConfig[companyCode];
  if (!config) return null;
  
  return {
    companyCode,
    host: config.host,
    port: config.port,
    database: config.database,
    user: config.user,
    // Don't expose password in logs
    hasPassword: !!config.password
  };
}