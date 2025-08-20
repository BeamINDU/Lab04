// pages/api/analyze-file.ts - Enhanced Version
import { NextApiRequest, NextApiResponse } from 'next';
import { getServerSession } from 'next-auth';
import { authOptions } from '../../lib/auth';
import { getCompanyDatabase } from '../../lib/database';
import multer from 'multer';
import * as XLSX from 'xlsx';
import csv from 'csv-parser';
import { createReadStream } from 'fs';
import { promises as fs } from 'fs';

// Configure multer for file upload
const upload = multer({
  dest: 'uploads/',
  limits: {
    fileSize: 50 * 1024 * 1024, // 50MB limit
  },
});

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

// Enhanced interfaces
interface DetectedColumn {
  name: string;
  originalName: string;
  type: string;
  length?: number;
  isPrimary: boolean;
  isRequired: boolean;
  isUnique: boolean;
  inferredType: string;
  sampleData: any[];
  confidence: 'high' | 'medium' | 'low';
}

interface ColumnMapping {
  csvColumn: string;
  originalCsvColumn: string;
  dbColumn: string;
  isMatched: boolean;
  confidence: 'high' | 'medium' | 'low' | 'none';
  suggestion?: string;
  dataType: string;
  required: boolean;
}

interface AnalysisResult {
  fileName: string;
  fileType: string;
  totalRows: number;
  detectedColumns: DetectedColumn[];
  sampleData: any[];
  encoding: string;
  
  // New mapping features
  mode: 'create_new' | 'map_existing';
  targetTable?: {
    schema: string;
    name: string;
    columns: any[];
  };
  columnMappings?: ColumnMapping[];
  headerTransformations?: {
    original: string;
    transformed: string;
    reason: string;
  }[];
  fixedCSV?: string;
  recommendedAction: string;
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

    await runMiddleware(req, res, upload.single('file'));

    const { file, body } = req as any;
    const { 
      mode = 'create_new', 
      targetSchema, 
      targetTable 
    } = body;

    if (!file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    console.log(`📁 Analyzing file: ${file.originalname} (Mode: ${mode})`);

    const companyCode = session.user.companyCode;
    const fileExtension = file.originalname.split('.').pop()?.toLowerCase();
    let analysis: AnalysisResult;

    // Analyze file structure
    switch (fileExtension) {
      case 'csv':
        analysis = await analyzeCsvFile(file.path, file.originalname, companyCode);
        break;
      case 'xlsx':
      case 'xls':
        analysis = await analyzeExcelFile(file.path, file.originalname, companyCode);
        break;
      case 'json':
        analysis = await analyzeJsonFile(file.path, file.originalname, companyCode);
        break;
      default:
        throw new Error(`Unsupported file type: ${fileExtension}`);
    }

    // Enhanced processing based on mode
    if (mode === 'map_existing' && targetSchema && targetTable) {
      analysis = await enhanceWithMapping(analysis, companyCode, targetSchema, targetTable, file.path);
    }

    // Clean up uploaded file
    await fs.unlink(file.path);

    console.log(`✅ Enhanced analysis complete: ${analysis.detectedColumns.length} columns, Mode: ${analysis.mode}`);

    res.status(200).json({
      success: true,
      ...analysis
    });

  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    console.error('❌ Error in enhanced analysis:', errorMessage);
    res.status(500).json({ 
      error: 'Failed to analyze file',
      details: errorMessage 
    });
  }
}

// Enhanced CSV analysis with header transformation
async function analyzeCsvFile(filePath: string, fileName: string, companyCode: string): Promise<AnalysisResult> {
  return new Promise((resolve, reject) => {
    const results: any[] = [];
    let originalHeaders: string[] = [];
    
    createReadStream(filePath)
      .pipe(csv())
      .on('headers', (headerList) => {
        originalHeaders = headerList;
      })
      .on('data', (data) => {
        if (results.length < 100) {
          results.push(data);
        }
      })
      .on('end', () => {
        try {
          const analysis = analyzeDataStructureEnhanced(results, fileName, 'csv', originalHeaders);
          resolve(analysis);
        } catch (error) {
          reject(error);
        }
      })
      .on('error', reject);
  });
}

async function analyzeExcelFile(filePath: string, fileName: string, companyCode: string): Promise<AnalysisResult> {
  const workbook = XLSX.readFile(filePath);
  const sheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[sheetName];
  
  const jsonData = XLSX.utils.sheet_to_json(worksheet, { raw: false });
  const originalHeaders = Object.keys(jsonData[0] || {});
  
  return analyzeDataStructureEnhanced(jsonData, fileName, 'excel', originalHeaders);
}

async function analyzeJsonFile(filePath: string, fileName: string, companyCode: string): Promise<AnalysisResult> {
  const content = await fs.readFile(filePath, 'utf8');
  const jsonData = JSON.parse(content);
  const dataArray = Array.isArray(jsonData) ? jsonData : [jsonData];
  const originalHeaders = Object.keys(dataArray[0] || {});
  
  return analyzeDataStructureEnhanced(dataArray, fileName, 'json', originalHeaders);
}

// Enhanced data structure analysis
function analyzeDataStructureEnhanced(
  data: any[], 
  fileName: string, 
  fileType: string, 
  originalHeaders: string[]
): AnalysisResult {
  if (data.length === 0) {
    throw new Error('ไฟล์ไม่มีข้อมูล');
  }

  const headerTransformations: { original: string; transformed: string; reason: string }[] = [];
  
  const detectedColumns: DetectedColumn[] = originalHeaders.map((originalName, index) => {
    const transformedName = transformColumnName(originalName);
    const columnData = data.map(row => row[originalName]).filter(val => val != null && val !== '' && val !== 'null');
    
    // Track header transformations
    if (originalName !== transformedName) {
      headerTransformations.push({
        original: originalName,
        transformed: transformedName,
        reason: getTransformationReason(originalName, transformedName)
      });
    }
    
    const inferredType = inferColumnTypeEnhanced(columnData);
    const sampleData = columnData.slice(0, 5);
    
    // Enhanced primary key detection
    const isPrimaryKey = detectPrimaryKey(originalName, transformedName, columnData, index);
    const isRequired = isPrimaryKey || inferredType.hasValues > data.length * 0.8;
    const isUnique = isPrimaryKey;
    
    return {
      name: transformedName,
      originalName: originalName,
      type: inferredType.primaryType,
      length: inferredType.length,
      isPrimary: isPrimaryKey,
      isRequired: isRequired,
      isUnique: isUnique,
      inferredType: inferredType.primaryType,
      sampleData: sampleData,
      confidence: inferredType.confidence
    };
  });

  // Ensure at least one primary key
  ensurePrimaryKey(detectedColumns);

  return {
    fileName,
    fileType,
    totalRows: data.length,
    detectedColumns,
    sampleData: data.slice(0, 5),
    encoding: 'UTF-8',
    mode: 'create_new',
    headerTransformations,
    recommendedAction: 'Create new table with detected structure'
  };
}

// Enhanced column name transformation
function transformColumnName(name: string): string {
  const transformed = name
    .toLowerCase()
    .trim()
    .replace(/\s+/g, '_')           // spaces to underscores
    .replace(/[^\w]/g, '_')         // special chars to underscores
    .replace(/_+/g, '_')            // multiple underscores to single
    .replace(/^_|_$/g, '')          // remove leading/trailing underscores
    .replace(/^(\d)/, '_$1');       // prefix numbers with underscore
    
  return transformed || 'unnamed_column';
}

function getTransformationReason(original: string, transformed: string): string {
  const reasons = [];
  
  if (original !== original.toLowerCase()) reasons.push('converted to lowercase');
  if (original.includes(' ')) reasons.push('spaces converted to underscores');
  if (/[^\w\s]/.test(original)) reasons.push('special characters removed');
  if (/^\d/.test(original)) reasons.push('prefixed with underscore (started with number)');
  
  return reasons.join(', ') || 'standardized format';
}

// Enhanced type inference with confidence scoring
function inferColumnTypeEnhanced(values: any[]): {
  primaryType: string;
  length: number | undefined;
  hasValues: number;
  confidence: 'high' | 'medium' | 'low';
} {
  if (values.length === 0) {
    return { primaryType: 'TEXT', length: undefined, hasValues: 0, confidence: 'low' as const };
  }

  const nonEmptyValues = values.filter(val => val != null && val !== '' && val !== 'null');
  const hasValues = nonEmptyValues.length;
  const confidence: 'high' | 'medium' | 'low' = hasValues > values.length * 0.9 ? 'high' : hasValues > values.length * 0.5 ? 'medium' : 'low';
  
  if (hasValues === 0) {
    return { primaryType: 'TEXT', length: undefined, hasValues: 0, confidence: 'low' as const };
  }
  
  // Enhanced type checking
  const typeChecks = {
    integer: checkInteger(nonEmptyValues),
    decimal: checkDecimal(nonEmptyValues),
    boolean: checkBoolean(nonEmptyValues),
    timestamp: checkTimestamp(nonEmptyValues),
    date: checkDate(nonEmptyValues),
    email: checkEmail(nonEmptyValues),
    uuid: checkUUID(nonEmptyValues),
    json: checkJSON(nonEmptyValues)
  };
  
  const maxLength = Math.max(...nonEmptyValues.map(v => String(v).length));
  
  // Determine best type with confidence
  let primaryType = 'VARCHAR';
  let length = undefined;
  
  if (typeChecks.boolean.score > 0.8) {
    primaryType = 'BOOLEAN';
  } else if (typeChecks.integer.score > 0.9) {
    const maxVal = Math.max(...nonEmptyValues.map(v => Math.abs(Number(v))));
    primaryType = maxVal > 2147483647 ? 'BIGINT' : 'INTEGER';
  } else if (typeChecks.decimal.score > 0.8) {
    primaryType = 'DECIMAL';
  } else if (typeChecks.timestamp.score > 0.8) {
    primaryType = 'TIMESTAMP';
  } else if (typeChecks.date.score > 0.8) {
    primaryType = 'DATE';
  } else if (typeChecks.email.score > 0.9) {
    primaryType = 'VARCHAR';
    length = 255;
  } else if (typeChecks.uuid.score > 0.9) {
    primaryType = 'UUID';
  } else if (typeChecks.json.score > 0.8) {
    primaryType = 'JSONB';
  } else {
    // String types with smart length detection
    if (maxLength > 2000) {
      primaryType = 'TEXT';
    } else {
      primaryType = 'VARCHAR';
      length = Math.max(255, Math.ceil(maxLength * 1.5));
    }
  }

  return { primaryType, length, hasValues, confidence };
}

// Type checking functions
function checkInteger(values: any[]): { score: number; matches: number } {
  let matches = 0;
  for (const val of values) {
    const num = Number(String(val).trim());
    if (!isNaN(num) && Number.isInteger(num) && !String(val).includes('.')) {
      matches++;
    }
  }
  return { score: matches / values.length, matches };
}

function checkDecimal(values: any[]): { score: number; matches: number } {
  let matches = 0;
  for (const val of values) {
    if (!isNaN(Number(String(val).trim()))) {
      matches++;
    }
  }
  return { score: matches / values.length, matches };
}

function checkBoolean(values: any[]): { score: number; matches: number } {
  let matches = 0;
  for (const val of values) {
    const str = String(val).toLowerCase().trim();
    if (['true', 'false', '1', '0', 'yes', 'no', 'y', 'n'].includes(str)) {
      matches++;
    }
  }
  return { score: matches / values.length, matches };
}

function checkTimestamp(values: any[]): { score: number; matches: number } {
  let matches = 0;
  for (const val of values) {
    const str = String(val).trim();
    const date = new Date(str);
    if (!isNaN(date.getTime()) && (str.includes(':') || str.includes('T'))) {
      matches++;
    }
  }
  return { score: matches / values.length, matches };
}

function checkDate(values: any[]): { score: number; matches: number } {
  let matches = 0;
  for (const val of values) {
    const str = String(val).trim();
    const date = new Date(str);
    if (!isNaN(date.getTime()) && !str.includes(':') && !str.includes('T')) {
      matches++;
    }
  }
  return { score: matches / values.length, matches };
}

function checkEmail(values: any[]): { score: number; matches: number } {
  let matches = 0;
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  for (const val of values) {
    if (emailRegex.test(String(val).trim())) {
      matches++;
    }
  }
  return { score: matches / values.length, matches };
}

function checkUUID(values: any[]): { score: number; matches: number } {
  let matches = 0;
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  for (const val of values) {
    if (uuidRegex.test(String(val).trim())) {
      matches++;
    }
  }
  return { score: matches / values.length, matches };
}

function checkJSON(values: any[]): { score: number; matches: number } {
  let matches = 0;
  for (const val of values) {
    try {
      const str = String(val).trim();
      if ((str.startsWith('{') && str.endsWith('}')) || (str.startsWith('[') && str.endsWith(']'))) {
        JSON.parse(str);
        matches++;
      }
    } catch {}
  }
  return { score: matches / values.length, matches };
}

// Enhanced primary key detection
function detectPrimaryKey(originalName: string, transformedName: string, values: any[], index: number): boolean {
  const name = transformedName.toLowerCase();
  
  // Name-based detection
  if (name === 'id' || name.endsWith('_id') || name === 'no' || name === 'number') {
    return true;
  }
  
  // First column with numeric sequential data
  if (index === 0 && values.length > 1) {
    const numericValues = values.map(v => Number(v)).filter(v => !isNaN(v));
    if (numericValues.length === values.length) {
      // Check if sequential (starting from 1)
      const sorted = [...numericValues].sort((a, b) => a - b);
      const isSequential = sorted.every((val, i) => val === i + 1);
      if (isSequential) return true;
    }
  }
  
  // Unique identifier patterns
  if (values.length > 0) {
    const uniqueValues = new Set(values);
    const isUnique = uniqueValues.size === values.length;
    const hasIdPattern = originalName.toLowerCase().includes('id') || 
                        originalName.toLowerCase().includes('key') ||
                        originalName.toLowerCase().includes('code');
    
    if (isUnique && hasIdPattern) return true;
  }
  
  return false;
}

function ensurePrimaryKey(columns: DetectedColumn[]) {
  const hasPrimaryKey = columns.some(col => col.isPrimary);
  
  if (!hasPrimaryKey && columns.length > 0) {
    // Try to make first INTEGER column primary
    const firstIntegerCol = columns.find(col => col.type === 'INTEGER');
    if (firstIntegerCol) {
      firstIntegerCol.isPrimary = true;
      firstIntegerCol.isRequired = true;
      firstIntegerCol.isUnique = true;
    } else {
      // Add auto-increment ID column
      columns.unshift({
        name: 'id',
        originalName: 'id',
        type: 'SERIAL',
        length: undefined,
        isPrimary: true,
        isRequired: true,
        isUnique: true,
        inferredType: 'SERIAL',
        sampleData: [],
        confidence: 'high'
      });
    }
  }
}

// NEW: Enhanced mapping functionality
async function enhanceWithMapping(
  analysis: AnalysisResult, 
  companyCode: string, 
  targetSchema: string, 
  targetTable: string,
  originalFilePath: string
): Promise<AnalysisResult> {
  try {
    // Fetch existing table structure
    const pool = getCompanyDatabase(companyCode);
    const query = `
      SELECT column_name, data_type, is_nullable, column_default
      FROM information_schema.columns 
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position;
    `;
    
    const result = await pool.query(query, [targetSchema, targetTable]);
    const dbColumns = result.rows;
    
    if (dbColumns.length === 0) {
      throw new Error(`Table ${targetSchema}.${targetTable} not found or has no columns`);
    }
    
    // Generate intelligent column mappings
    const columnMappings: ColumnMapping[] = analysis.detectedColumns.map(csvCol => {
      const bestMatch = findBestColumnMatch(csvCol, dbColumns);
      
      return {
        csvColumn: csvCol.name,
        originalCsvColumn: csvCol.originalName,
        dbColumn: bestMatch?.column_name || '',
        isMatched: !!bestMatch,
        confidence: bestMatch ? calculateMatchConfidence(csvCol, bestMatch) : 'none',
        suggestion: bestMatch ? '' : suggestAlternatives(csvCol, dbColumns),
        dataType: bestMatch?.data_type || 'unknown',
        required: bestMatch?.is_nullable === 'NO'
      };
    });
    
    // Generate fixed CSV content
    const fixedCSV = await generateFixedCSVContent(originalFilePath, columnMappings, analysis.fileType);
    
    // Enhanced analysis result
    return {
      ...analysis,
      mode: 'map_existing',
      targetTable: {
        schema: targetSchema,
        name: targetTable,
        columns: dbColumns
      },
      columnMappings,
      fixedCSV,
      recommendedAction: generateRecommendation(columnMappings)
    };
    
  } catch (error) {
    console.error('Error in mapping enhancement:', error);
    throw error;
  }
}

// Smart column matching algorithm
function findBestColumnMatch(csvColumn: DetectedColumn, dbColumns: any[]): any | null {
  const csvName = csvColumn.name.toLowerCase();
  const originalName = csvColumn.originalName.toLowerCase();
  
  // Exact match priority
  let bestMatch = dbColumns.find((db: any) => db.column_name.toLowerCase() === csvName);
  if (bestMatch) return bestMatch;
  
  // Original name exact match
  bestMatch = dbColumns.find((db: any) => db.column_name.toLowerCase() === originalName);
  if (bestMatch) return bestMatch;
  
  // Partial matching with common patterns
  const patterns = [
    // Direct contains
    (db: any) => db.column_name.toLowerCase().includes(csvName) || csvName.includes(db.column_name.toLowerCase()),
    // Common aliases
    (db: any) => checkCommonAliases(csvName, db.column_name.toLowerCase()),
    // Similar words
    (db: any) => calculateStringSimilarity(csvName, db.column_name.toLowerCase()) > 0.7
  ];
  
  for (const pattern of patterns) {
    bestMatch = dbColumns.find(pattern);
    if (bestMatch) return bestMatch;
  }
  
  return null;
}

function checkCommonAliases(csvName: string, dbName: string): boolean {
  const aliases = {
    'id': ['no', 'number', 'seq', 'sequence', 'key'],
    'name': ['title', 'label', 'description', 'desc'],
    'date': ['time', 'timestamp', 'created', 'updated', 'modified'],
    'status': ['state', 'condition', 'active', 'enabled'],
    'type': ['category', 'kind', 'class', 'group'],
    'email': ['mail', 'e_mail', 'email_address'],
    'phone': ['telephone', 'mobile', 'contact'],
    'address': ['location', 'addr', 'place']
  };
  
  for (const [key, values] of Object.entries(aliases)) {
    if ((csvName.includes(key) || values.some(v => csvName.includes(v))) &&
        (dbName.includes(key) || values.some(v => dbName.includes(v)))) {
      return true;
    }
  }
  
  return false;
}

function calculateStringSimilarity(str1: string, str2: string): number {
  const longer = str1.length > str2.length ? str1 : str2;
  const shorter = str1.length > str2.length ? str2 : str1;
  
  if (longer.length === 0) return 1.0;
  
  const distance = levenshteinDistance(longer, shorter);
  return (longer.length - distance) / longer.length;
}

function levenshteinDistance(str1: string, str2: string): number {
  const matrix = [];
  
  for (let i = 0; i <= str2.length; i++) {
    matrix[i] = [i];
  }
  
  for (let j = 0; j <= str1.length; j++) {
    matrix[0][j] = j;
  }
  
  for (let i = 1; i <= str2.length; i++) {
    for (let j = 1; j <= str1.length; j++) {
      if (str2.charAt(i - 1) === str1.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          matrix[i][j - 1] + 1,
          matrix[i - 1][j] + 1
        );
      }
    }
  }
  
  return matrix[str2.length][str1.length];
}

function calculateMatchConfidence(csvColumn: DetectedColumn, dbColumn: any): 'high' | 'medium' | 'low' {
  const nameMatch = csvColumn.name === dbColumn.column_name;
  const typeCompatible = areTypesCompatible(csvColumn.type, dbColumn.data_type);
  
  if (nameMatch && typeCompatible) return 'high';
  if (nameMatch || typeCompatible) return 'medium';
  return 'low';
}

function areTypesCompatible(csvType: string, dbType: string): boolean {
  const compatibilityMap: { [key: string]: string[] } = {
    'INTEGER': ['integer', 'bigint', 'smallint'],
    'BIGINT': ['bigint', 'integer'],
    'DECIMAL': ['numeric', 'decimal', 'real', 'double precision'],
    'VARCHAR': ['character varying', 'varchar', 'text', 'char'],
    'TEXT': ['text', 'character varying', 'varchar'],
    'BOOLEAN': ['boolean'],
    'DATE': ['date'],
    'TIMESTAMP': ['timestamp', 'timestamp with time zone', 'timestamp without time zone'],
    'UUID': ['uuid'],
    'JSONB': ['jsonb', 'json']
  };
  
  const compatible = compatibilityMap[csvType] || [];
  return compatible.includes(dbType.toLowerCase());
}

function suggestAlternatives(csvColumn: DetectedColumn, dbColumns: any[]): string {
  const similar = dbColumns
    .map((db: any) => ({
      name: db.column_name,
      similarity: calculateStringSimilarity(csvColumn.name, db.column_name)
    }))
    .filter(item => item.similarity > 0.3)
    .sort((a, b) => b.similarity - a.similarity)
    .slice(0, 3)
    .map(item => item.name);
    
  return similar.length > 0 ? `Similar: ${similar.join(', ')}` : 'No similar columns found';
}

async function generateFixedCSVContent(filePath: string, mappings: ColumnMapping[], fileType: string): Promise<string> {
  // This would read the original file and generate fixed CSV
  // For now, return a placeholder
  const mappedHeaders = mappings.map(m => m.dbColumn || m.csvColumn).join(',');
  return `${mappedHeaders}\n// Fixed CSV content would be generated here`;
}

function generateRecommendation(mappings: ColumnMapping[]): string {
  const matched = mappings.filter(m => m.isMatched).length;
  const total = mappings.length;
  const percentage = Math.round((matched / total) * 100);
  
  if (percentage >= 90) {
    return `Excellent match! ${matched}/${total} columns mapped. Ready for import.`;
  } else if (percentage >= 70) {
    return `Good match! ${matched}/${total} columns mapped. Review unmapped columns.`;
  } else if (percentage >= 50) {
    return `Partial match! ${matched}/${total} columns mapped. Manual review required.`;
  } else {
    return `Poor match! ${matched}/${total} columns mapped. Consider creating new table.`;
  }
}