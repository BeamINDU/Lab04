// pages/api/services/preview.ts - File Preview API (Fixed)
import { NextApiRequest, NextApiResponse } from 'next';
import { getServerSession } from 'next-auth';
import { authOptions } from '../../../lib/auth';
import { DatabaseService } from '../../../lib/services/DatabaseService';
import { FileImportService } from '../../../lib/services/FileImportService';
import multer from 'multer';

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

// Middleware helper function
function runMiddleware(req: any, res: any, fn: any): Promise<any> {
  return new Promise((resolve, reject) => {
    fn(req, res, (result: any) => {
      if (result instanceof Error) {
        return reject(result);
      }
      return resolve(result);
    });
  });
}

// Configure multer for file upload
const upload = multer({
  dest: 'uploads/',
  limits: {
    fileSize: 100 * 1024 * 1024, // 100MB limit
    files: 1
  },
  fileFilter: (req, file, cb) => {
    const allowedTypes = [
      'text/csv',
      'application/vnd.ms-excel',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'text/plain',
      'application/json',
      'text/tab-separated-values'
    ];
    
    if (allowedTypes.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error('File type not supported'));
    }
  }
});

// Disable Next.js body parser for file upload
export const config = {
  api: {
    bodyParser: false,
  },
};

// Main handler function
export default async function previewHandler(req: NextApiRequest, res: NextApiResponse<ApiResponse>) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', ['POST']);
    return res.status(405).json(createErrorResponse('Method not allowed'));
  }

  try {
    // Authentication check
    const session = await getServerSession(req, res, authOptions);
    if (!session?.user?.companyCode) {
      return res.status(401).json(createErrorResponse('Unauthorized'));
    }

    const { companyCode } = session.user;

    // Handle file upload
    await runMiddleware(req, res, upload.single('file'));

    const { file } = req as any;
    
    if (!file) {
      return res.status(400).json(createErrorResponse('No file uploaded'));
    }

    console.log(`📖 Previewing file: ${file.originalname} (${(file.size / 1024 / 1024).toFixed(2)} MB)`);

    // Create services
    const dbService = new DatabaseService(companyCode);
    const importService = new FileImportService(dbService);

    // Preview file
    const preview = await importService.previewFile(
      file.path,
      file.originalname,
      file.mimetype
    );

    return res.status(200).json(createResponse(preview, 'File preview generated successfully'));

  } catch (error) {
    console.error('Preview Error:', error);
    
    const message = error instanceof Error ? error.message : 'Preview failed';
    return res.status(500).json(createErrorResponse(message));
  }
}