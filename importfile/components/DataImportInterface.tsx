import React, { useState, useEffect } from 'react';
import { useSession } from 'next-auth/react';
import { toast } from 'react-hot-toast';
import { 
  Upload, 
  FileText, 
  Database, 
  CheckCircle, 
  XCircle, 
  AlertCircle,
  Eye,
  Play,
  RotateCcw
} from 'lucide-react';

interface TableInfo {
  table_name: string;
  table_comment?: string;
  column_count?: number;
}

interface ImportPreview {
  headers: string[];
  sampleData: any[];
  totalRows: number;
  fileName: string;
  fileType: string;
}

interface ImportResult {
  success: boolean;
  totalRows: number;
  successRows: number;
  errorRows: number;
  errors: Array<{ row: number; error: string; data?: any }>;
}

export default function DataImportInterface() {
  const { data: session } = useSession();
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [uploadedFile, setUploadedFile] = useState<File | null>(null);
  const [importPreview, setImportPreview] = useState<ImportPreview | null>(null);
  const [importResult, setImportResult] = useState<ImportResult | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [step, setStep] = useState<'upload' | 'preview' | 'result'>('upload');
  const [isDragActive, setIsDragActive] = useState(false);

  // Fetch available tables on component mount
  useEffect(() => {
    if (session?.user?.companyCode) {
      fetchTables();
    }
  }, [session]);

  const fetchTables = async () => {
    try {
      const response = await fetch(`/api/tables?company=${session?.user?.companyCode}`);
      const data = await response.json();
      setTables(data.tables || []);
    } catch (error) {
      toast.error('ไม่สามารถโหลดรายชื่อตารางได้');
    }
  };

  // Handle file drop
  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragActive(false);
    
    const files = Array.from(e.dataTransfer.files);
    const file = files[0];
    
    if (validateFile(file)) {
      setUploadedFile(file);
    }
  };

  // Handle file input change
  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file && validateFile(file)) {
      setUploadedFile(file);
    }
  };

  // Validate file type and size
  const validateFile = (file: File): boolean => {
    const allowedTypes = [
      'text/csv',
      'application/vnd.ms-excel',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'application/json'
    ];

    if (!allowedTypes.includes(file.type)) {
      toast.error('รองรับเฉพาะไฟล์ CSV, Excel หรือ JSON เท่านั้น');
      return false;
    }

    if (file.size > 50 * 1024 * 1024) { // 50MB
      toast.error('ขนาดไฟล์ต้องไม่เกิน 50MB');
      return false;
    }

    return true;
  };

  const generatePreview = async () => {
    if (!uploadedFile || !selectedTable) {
      toast.error('กรุณาเลือกตารางและไฟล์');
      return;
    }

    setIsLoading(true);
    const formData = new FormData();
    formData.append('file', uploadedFile);
    formData.append('tableName', selectedTable);
    formData.append('companyCode', session?.user?.companyCode || '');

    try {
      const response = await fetch('/api/import/preview', {
        method: 'POST',
        body: formData,
      });

      const data = await response.json();
      
      if (data.success) {
        setImportPreview(data.preview);
        setStep('preview');
      } else {
        toast.error(data.error || 'ไม่สามารถสร้างตัวอย่างได้');
      }
    } catch (error) {
      toast.error('เกิดข้อผิดพลาดในการสร้างตัวอย่าง');
    } finally {
      setIsLoading(false);
    }
  };

  const executeImport = async () => {
    if (!uploadedFile || !selectedTable) return;

    setIsLoading(true);
    const formData = new FormData();
    formData.append('file', uploadedFile);
    formData.append('tableName', selectedTable);
    formData.append('companyCode', session?.user?.companyCode || '');

    try {
      const response = await fetch('/api/import/execute', {
        method: 'POST',
        body: formData,
      });

      const data = await response.json();
      
      if (data.success) {
        setImportResult(data.result);
        setStep('result');
        toast.success(`นำเข้าข้อมูลสำเร็จ ${data.result.successRows} รายการ`);
      } else {
        toast.error(data.error || 'การนำเข้าข้อมูลล้มเหลว');
      }
    } catch (error) {
      toast.error('เกิดข้อผิดพลาดในการนำเข้าข้อมูล');
    } finally {
      setIsLoading(false);
    }
  };

  const resetImport = () => {
    setUploadedFile(null);
    setImportPreview(null);
    setImportResult(null);
    setStep('upload');
    setSelectedTable('');
  };

  return (
    <div className="max-w-6xl mx-auto p-6 space-y-6">
      {/* Header */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900 flex items-center">
              <Database className="h-7 w-7 text-blue-600 mr-3" />
              นำเข้าข้อมูล - {session?.user?.companyName}
            </h1>
            <p className="text-gray-600 mt-1">
              อัพโหลดและนำเข้าข้อมูลจากไฟล์ CSV, Excel หรือ JSON
            </p>
          </div>
          {step !== 'upload' && (
            <button
              onClick={resetImport}
              className="flex items-center px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <RotateCcw className="h-4 w-4 mr-2" />
              เริ่มใหม่
            </button>
          )}
        </div>
      </div>

      {/* Step 1: Table Selection & File Upload */}
      {step === 'upload' && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Table Selection */}
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h2 className="text-lg font-semibold text-gray-900 mb-4">
              1. เลือกตารางข้อมูล
            </h2>
            <div className="space-y-3 max-h-64 overflow-y-auto">
              {tables.map((table) => (
                <label
                  key={table.table_name}
                  className="flex items-center p-3 border border-gray-200 rounded-lg cursor-pointer hover:bg-gray-50 transition-colors"
                >
                  <input
                    type="radio"
                    name="table"
                    value={table.table_name}
                    checked={selectedTable === table.table_name}
                    onChange={(e) => setSelectedTable(e.target.value)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300"
                  />
                  <div className="ml-3">
                    <div className="text-sm font-medium text-gray-900">
                      {table.table_name}
                    </div>
                    {table.table_comment && (
                      <div className="text-sm text-gray-500">
                        {table.table_comment}
                      </div>
                    )}
                  </div>
                </label>
              ))}
            </div>
          </div>

          {/* File Upload */}
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h2 className="text-lg font-semibold text-gray-900 mb-4">
              2. อัพโหลดไฟล์
            </h2>
            <div
              onDrop={handleDrop}
              onDragOver={(e) => { e.preventDefault(); setIsDragActive(true); }}
              onDragLeave={() => setIsDragActive(false)}
              className={`border-2 border-dashed rounded-lg p-8 text-center cursor-pointer transition-colors ${
                isDragActive
                  ? 'border-blue-400 bg-blue-50'
                  : 'border-gray-300 hover:border-gray-400'
              }`}
            >
              <input
                type="file"
                onChange={handleFileChange}
                accept=".csv,.xls,.xlsx,.json"
                className="hidden"
                id="file-upload"
              />
              <label htmlFor="file-upload" className="cursor-pointer">
                <Upload className="h-12 w-12 text-gray-400 mx-auto mb-4" />
                {uploadedFile ? (
                  <div>
                    <p className="text-sm font-medium text-gray-900">
                      {uploadedFile.name}
                    </p>
                    <p className="text-xs text-gray-500 mt-1">
                      ขนาด: {(uploadedFile.size / 1024 / 1024).toFixed(2)} MB
                    </p>
                  </div>
                ) : (
                  <div>
                    <p className="text-sm text-gray-600">
                      ลากไฟล์มาวางที่นี่ หรือคลิกเพื่อเลือกไฟล์
                    </p>
                    <p className="text-xs text-gray-500 mt-1">
                      รองรับ CSV, Excel (XLS/XLSX), JSON (สูงสุด 50MB)
                    </p>
                  </div>
                )}
              </label>
            </div>

            {uploadedFile && selectedTable && (
              <button
                onClick={generatePreview}
                disabled={isLoading}
                className="w-full mt-4 flex items-center justify-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50"
              >
                {isLoading ? (
                  <>
                    <div className="animate-spin rounded-full h-4 w-4 border-2 border-white border-t-transparent mr-2"></div>
                    กำลังสร้างตัวอย่าง...
                  </>
                ) : (
                  <>
                    <Eye className="h-4 w-4 mr-2" />
                    ดูตัวอย่างข้อมูล
                  </>
                )}
              </button>
            )}
          </div>
        </div>
      )}

      {/* Step 2: Preview */}
      {step === 'preview' && importPreview && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-lg font-semibold text-gray-900">
              ตัวอย่างข้อมูลที่จะนำเข้า
            </h2>
            <div className="text-sm text-gray-600">
              ไฟล์: {importPreview.fileName} | 
              ประเภท: {importPreview.fileType} | 
              จำนวนรวม: {importPreview.totalRows} รายการ
            </div>
          </div>

          {/* Data Preview Table */}
          <div className="overflow-x-auto mb-6">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  {importPreview.headers.map((header, index) => (
                    <th
                      key={index}
                      className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                    >
                      {header}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {importPreview.sampleData.slice(0, 5).map((row, rowIndex) => (
                  <tr key={rowIndex} className="hover:bg-gray-50">
                    {importPreview.headers.map((header, colIndex) => (
                      <td
                        key={colIndex}
                        className="px-6 py-4 whitespace-nowrap text-sm text-gray-900"
                      >
                        {row[header] || '-'}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="flex justify-center">
            <button
              onClick={executeImport}
              disabled={isLoading}
              className="flex items-center px-6 py-3 border border-transparent text-base font-medium rounded-md text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-green-500 disabled:opacity-50"
            >
              {isLoading ? (
                <>
                  <div className="animate-spin rounded-full h-5 w-5 border-2 border-white border-t-transparent mr-2"></div>
                  กำลังนำเข้า...
                </>
              ) : (
                <>
                  <Play className="h-5 w-5 mr-2" />
                  เริ่มนำเข้าข้อมูล
                </>
              )}
            </button>
          </div>
        </div>
      )}

      {/* Step 3: Results */}
      {step === 'result' && importResult && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-6">
            ผลการนำเข้าข้อมูล
          </h2>

          {/* Summary Cards */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
            <div className="bg-blue-50 rounded-lg p-4">
              <div className="flex items-center">
                <FileText className="h-8 w-8 text-blue-600" />
                <div className="ml-3">
                  <p className="text-sm font-medium text-blue-600">รวมทั้งหมด</p>
                  <p className="text-2xl font-bold text-blue-900">{importResult.totalRows}</p>
                </div>
              </div>
            </div>

            <div className="bg-green-50 rounded-lg p-4">
              <div className="flex items-center">
                <CheckCircle className="h-8 w-8 text-green-600" />
                <div className="ml-3">
                  <p className="text-sm font-medium text-green-600">สำเร็จ</p>
                  <p className="text-2xl font-bold text-green-900">{importResult.successRows}</p>
                </div>
              </div>
            </div>

            <div className="bg-red-50 rounded-lg p-4">
              <div className="flex items-center">
                <XCircle className="h-8 w-8 text-red-600" />
                <div className="ml-3">
                  <p className="text-sm font-medium text-red-600">ล้มเหลว</p>
                  <p className="text-2xl font-bold text-red-900">{importResult.errorRows}</p>
                </div>
              </div>
            </div>
          </div>

          {/* Error Details */}
          {importResult.errors.length > 0 && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-4">
              <div className="flex items-center mb-3">
                <AlertCircle className="h-5 w-5 text-red-600 mr-2" />
                <h3 className="text-sm font-medium text-red-800">
                  รายการที่มีข้อผิดพลาด ({importResult.errors.length} รายการ)
                </h3>
              </div>
              <div className="max-h-60 overflow-y-auto">
                {importResult.errors.slice(0, 10).map((error, index) => (
                  <div key={index} className="text-sm text-red-700 mb-2">
                    <strong>แถวที่ {error.row}:</strong> {error.error}
                  </div>
                ))}
                {importResult.errors.length > 10 && (
                  <p className="text-sm text-red-600 font-medium">
                    และอีก {importResult.errors.length - 10} รายการ...
                  </p>
                )}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}