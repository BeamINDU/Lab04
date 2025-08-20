import { NextApiRequest, NextApiResponse } from 'next';
import { getSession } from 'next-auth/react';
import multer from 'multer';
import { DataImportProcessor } from '../../../lib/importProcessor';
import { promises as fs } from 'fs';
import path from 'path';

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
    const session = await getSession({ req });
    if (!session) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Run multer middleware
    await runMiddleware(req, res, upload.single('file'));

    const { file, body } = req as any;
    const { tableName, companyCode } = body;

    if (!file || !tableName || !companyCode) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Verify company access
    if (session.user.companyCode !== companyCode) {
      return res.status(403).json({ error: 'Access denied' });
    }

    const processor = new DataImportProcessor(companyCode);
    
    // Generate preview data
    const preview = await generateFilePreview(file.path, file.originalname);
    
    // Clean up uploaded file
    await fs.unlink(file.path);

    res.status(200).json({ success: true, preview });
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    console.error('Error generating preview:', errorMessage);
    res.status(500).json({ error: 'Failed to generate preview' });
  }
}

async function generateFilePreview(filePath: string, fileName: string) {
  const csv = require('csv-parser');
  const XLSX = require('xlsx');
  const { createReadStream } = require('fs');
  
  const fileExtension = fileName.split('.').pop()?.toLowerCase();
  let data: any[] = [];

  try {
    switch (fileExtension) {
      case 'csv':
        data = await new Promise((resolve, reject) => {
          const results: any[] = [];
          createReadStream(filePath)
            .pipe(csv())
            .on('data', (row: any) => results.push(row))
            .on('end', () => resolve(results))
            .on('error', reject);
        });
        break;

      case 'xlsx':
      case 'xls':
        const workbook = XLSX.readFile(filePath);
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        data = XLSX.utils.sheet_to_json(worksheet);
        break;

      case 'json':
        const fs = require('fs').promises;
        const content = await fs.readFile(filePath, 'utf8');
        const jsonData = JSON.parse(content);
        data = Array.isArray(jsonData) ? jsonData : [jsonData];
        break;

      default:
        throw new Error(`Unsupported file type: ${fileExtension}`);
    }

    const headers = data.length > 0 ? Object.keys(data[0]) : [];
    
    return {
      headers,
      sampleData: data.slice(0, 10), // First 10 rows for preview
      totalRows: data.length,
      fileName,
      fileType: fileExtension
    };
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    throw new Error(`Failed to parse file: ${errorMessage}`);
  }
}