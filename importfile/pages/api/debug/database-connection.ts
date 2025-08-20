// pages/api/debug/database-connection.ts
import { NextApiRequest, NextApiResponse } from 'next';
import { Pool } from 'pg';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const results = [];

  // Test configuration for each company
  const testConfigs = [
    {
      name: 'Company A',
      config: {
        user: 'postgres',
        host: 'localhost',
        database: 'siamtemp_company_a',
        password: 'password123',
        port: 5432,
        connectionTimeoutMillis: 5000,
      }
    },
    {
      name: 'Company B',
      config: {
        user: 'postgres',
        host: 'localhost',
        database: 'siamtemp_company_b',
        password: 'password123',
        port: 5433,
        connectionTimeoutMillis: 5000,
      }
    },
    {
      name: 'Company C',
      config: {
        user: 'postgres',
        host: 'localhost',
        database: 'siamtemp_company_c',
        password: 'password123',
        port: 5434,
        connectionTimeoutMillis: 5000,
      }
    }
  ];

  for (const testConfig of testConfigs) {
    let pool: Pool | null = null;
    let client = null;
    
    try {
      console.log(`🧪 Testing connection to ${testConfig.name}...`);
      
      pool = new Pool(testConfig.config);
      client = await pool.connect();
      
      // Test basic query
      const result = await client.query('SELECT current_database(), current_user, version() as db_version, now() as current_time');
      
      // Test schema query
      const schemaResult = await client.query(`
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name
      `);
      
      // Test tables query
      const tablesResult = await client.query(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
      `);
      
      results.push({
        company: testConfig.name,
        status: 'success',
        connectionInfo: {
          host: testConfig.config.host,
          port: testConfig.config.port,
          database: testConfig.config.database,
          user: testConfig.config.user
        },
        databaseInfo: result.rows[0],
        schemas: schemaResult.rows.map(row => row.schema_name),
        tables: tablesResult.rows.map(row => row.table_name),
        schemasCount: schemaResult.rows.length,
        tablesCount: tablesResult.rows.length
      });
      
      console.log(`✅ ${testConfig.name} connection successful`);
      
    } catch (error) {
      console.error(`❌ ${testConfig.name} connection failed:`, error);
      
      results.push({
        company: testConfig.name,
        status: 'failed',
        connectionInfo: {
          host: testConfig.config.host,
          port: testConfig.config.port,
          database: testConfig.config.database,
          user: testConfig.config.user
        },
        error: error instanceof Error ? error.message : 'Unknown error',
        errorCode: (error as any)?.code || 'UNKNOWN',
        errorDetails: {
          name: (error as any)?.name,
          code: (error as any)?.code,
          errno: (error as any)?.errno,
          syscall: (error as any)?.syscall,
          address: (error as any)?.address,
          port: (error as any)?.port
        }
      });
      
    } finally {
      if (client) {
        try {
          client.release();
        } catch (releaseError) {
          console.error('Error releasing client:', releaseError);
        }
      }
      
      if (pool) {
        try {
          await pool.end();
        } catch (endError) {
          console.error('Error closing pool:', endError);
        }
      }
    }
  }

  // Summary
  const successCount = results.filter(r => r.status === 'success').length;
  const failedCount = results.filter(r => r.status === 'failed').length;

  return res.status(200).json({
    summary: {
      total: results.length,
      successful: successCount,
      failed: failedCount,
      timestamp: new Date().toISOString()
    },
    results,
    environment: {
      nodeEnv: process.env.NODE_ENV,
      platform: process.platform,
      nodeVersion: process.version
    }
  });
}