// pages/api/database/tables.ts
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
    // Create new table
    try {
      const { name, schema = 'public', description, columns } = req.body;
      
      if (!name || !columns || columns.length === 0) {
        return res.status(400).json({ message: 'Table name and columns are required' });
      }

      const client = await pool.connect();
      
      await client.query('BEGIN');
      
      // Build CREATE TABLE SQL
      let columnDefinitions = columns.map((col: any) => {
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
        
        if (col.defaultValue && col.defaultValue.trim() !== '') {
          if (col.type.toUpperCase().includes('SERIAL')) {
            // Don't add DEFAULT for SERIAL types
          } else if (isNaN(col.defaultValue)) {
            colDef += ` DEFAULT '${col.defaultValue}'`;
          } else {
            colDef += ` DEFAULT ${col.defaultValue}`;
          }
        }
        
        return colDef;
      }).join(', ');
      
      // Add primary key constraint
      const primaryKeys = columns
        .filter((col: any) => col.isPrimary)
        .map((col: any) => `"${col.name}"`);
      
      if (primaryKeys.length > 0) {
        columnDefinitions += `, PRIMARY KEY (${primaryKeys.join(', ')})`;
      }
      
      const createTableSQL = `
        CREATE TABLE "${schema}"."${name}" (
          ${columnDefinitions}
        )
      `;
      
      console.log('Executing SQL:', createTableSQL); // For debugging
      
      await client.query(createTableSQL);
      
      // Add table comment
      if (description) {
        await client.query(
          `COMMENT ON TABLE "${schema}"."${name}" IS $1`,
          [description]
        );
      }
      
      await client.query('COMMIT');
      client.release();
      
      res.status(201).json({ 
        success: true, 
        message: `Table "${name}" created successfully in schema "${schema}"` 
      });
    } catch (error: any) {
      console.error('Error creating table:', error);
      res.status(500).json({ message: `Failed to create table: ${error.message}` });
    }
  } else {
    res.setHeader('Allow', ['POST']);
    res.status(405).end(`Method ${req.method} Not Allowed`);
  }
}
