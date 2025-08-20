// pages/api/database/import.ts
import { NextApiRequest, NextApiResponse } from 'next';
import { Pool } from 'pg';

const pool = new Pool({
  user: process.env.DB_USER || 'postgres',
  host: process.env.DB_HOST || 'localhost',
  database: process.env.DB_NAME || 'siamtemp_company_a',
  password: process.env.DB_PASSWORD,
  port: parseInt(process.env.DB_PORT || '5432'),
});

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method === 'POST') {
    try {
      const { schemaName, tableName, data, options } = req.body;
      
      if (!schemaName || !tableName || !data || !Array.isArray(data)) {
        return res.status(400).json({ message: 'Schema name, table name, and data array are required' });
      }

      const client = await pool.connect();
      
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
        client.release();
        return res.status(404).json({ message: `Table ${schemaName}.${tableName} not found` });
      }
      
      await client.query('BEGIN');
      
      // Process and insert data
      let insertedRows = 0;
      const columnNames = columns.map((col: any) => col.column_name);
      
      for (const row of data) {
        if (!row || typeof row !== 'object') continue;
        
        const values = columnNames.map(colName => {
          const value = row[colName];
          if (value === undefined || value === null || value === '') {
            return null;
          }
          return value;
        });
        
        const placeholders = values.map((_: any, index: number) => `$${index + 1}`).join(', ');
        
        let insertSQL = `
          INSERT INTO "${schemaName}"."${tableName}" (${columnNames.map(n => `"${n}"`).join(', ')})
          VALUES (${placeholders})
        `;
        
        if (options.onDuplicate === 'skip') {
          insertSQL += ' ON CONFLICT DO NOTHING';
        }
        
        try {
          const result = await client.query(insertSQL, values);
          if (result.rowCount && result.rowCount > 0) {
            insertedRows++;
          }
        } catch (error: any) {
          if (options.onDuplicate === 'error') {
            throw error;
          }
          // Skip on conflict if onDuplicate is 'skip'
          console.log('Skipping row due to conflict:', error.message);
        }
      }
      
      await client.query('COMMIT');
      client.release();
      
      res.status(200).json({ 
        success: true, 
        message: `Imported ${insertedRows} rows successfully into ${schemaName}.${tableName}`,
        insertedRows 
      });
    } catch (error: any) {
      console.error('Error importing data:', error);
      res.status(500).json({ message: `Failed to import data: ${error.message}` });
    }
  } else {
    res.setHeader('Allow', ['POST']);
    res.status(405).end(`Method ${req.method} Not Allowed`);
  }
}