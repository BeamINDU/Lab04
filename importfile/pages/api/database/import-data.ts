// pages/api/database/import-data.ts
import { NextApiRequest, NextApiResponse } from 'next';
import { getServerSession } from 'next-auth';
import { authOptions } from '../../../lib/auth';
import { getCompanyDatabase } from '../../../lib/database';
import { DataImportProcessor } from '../../../lib/importProcessor';
import multer from 'multer';

// Configure multer for file upload
const upload = multer({
  dest: 'uploads/',
  limits: {
    fileSize: 50 * 1024 * 1024, // 50MB limit
  },
});

// Disable Next.js body parser for this route
export const config = {
  api: {
    bodyParser: false,
  },
};

function runMiddleware(req: any, res: any, fn: any) {
  return new Promise((resolve, reject) => {
    fn(req, res, (result: any) => {
      if (result instanceof Error) {
        return reject(result);
      }
      return resolve(result);
    });
  });
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

    // Run multer middleware
    await runMiddleware(req, res, upload.single('file'));

    const { file, body } = req as any;
    const { tableName, schema = 'public' } = body;

    if (!file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    if (!tableName) {
      return res.status(400).json({ error: 'Table name is required' });
    }

    console.log(`📥 Importing data to "${schema}"."${tableName}" for company ${companyCode}`);
    console.log(`📁 File: ${file.originalname} (${(file.size / 1024 / 1024).toFixed(2)} MB)`);

    // Verify table exists
    const pool = getCompanyDatabase(companyCode);
    const tableExistsQuery = `
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = $1 AND table_name = $2
      );
    `;
    const existsResult = await pool.query(tableExistsQuery, [schema, tableName]);
    
    if (!existsResult.rows[0].exists) {
      return res.status(404).json({ 
        error: `Table "${schema}"."${tableName}" does not exist` 
      });
    }

    // Process the import
    const processor = new DataImportProcessor(companyCode);
    const result = await processor.processFile(file.path, file.originalname, `${schema}.${tableName}`);

    // Clean up uploaded file
    const fs = require('fs').promises;
    await fs.unlink(file.path);

    if (result.success) {
      console.log(`✅ Import completed: ${result.successRows}/${result.totalRows} rows imported`);
      
      res.status(200).json({
        success: true,
        message: `Successfully imported ${result.successRows} rows into ${schema}.${tableName}`,
        importedRows: result.successRows,
        totalRows: result.totalRows,
        errorRows: result.errorRows,
        errors: result.errors.slice(0, 10), // Return first 10 errors only
        hasMoreErrors: result.errors.length > 10,
        summary: {
          fileName: file.originalname,
          fileSize: file.size,
          tableName: `${schema}.${tableName}`,
          companyCode,
          importedAt: new Date().toISOString(),
          user: session.user.email
        }
      });
    } else {
      console.error(`❌ Import failed: ${result.errors.length} errors`);
      
      res.status(400).json({
        success: false,
        message: 'Import failed with errors',
        totalRows: result.totalRows,
        errorRows: result.errorRows,
        errors: result.errors.slice(0, 20), // Return first 20 errors for diagnosis
        hasMoreErrors: result.errors.length > 20
      });
    }

  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    console.error('❌ Error importing data:', errorMessage);
    
    res.status(500).json({
      error: 'Failed to import data',
      details: errorMessage
    });
  }
}