import React, { useState, useEffect } from 'react';
import { useSession } from 'next-auth/react';
import { toast } from 'react-hot-toast';
import { 
  Database, 
  Plus, 
  Trash2, 
  Upload, 
  FileText,
  RefreshCw,
  Search,
  FolderPlus,
  Table2,
  Settings,
  Eye,
  CheckCircle,
  XCircle,
  AlertCircle
} from 'lucide-react';

// Types
interface SchemaInfo {
  name: string;
  description?: string;
  tables: TableInfo[];
}

interface TableInfo {
  name: string;
  schema: string;
  comment?: string;
  columnCount: number;
  hasData: boolean;
}

interface DatabaseColumn {
  name: string;
  type: string;
  length?: number;
  isPrimary?: boolean;
  isRequired?: boolean;
  isUnique?: boolean;
  defaultValue?: string;
  comment?: string;
}

interface FilePreview {
  headers: string[];
  sampleData: any[];
  totalRows: number;
  fileName: string;
  fileType: string;
  suggestedColumns: DatabaseColumn[];
}

interface ImportResult {
  success: boolean;
  totalRows: number;
  successRows: number;
  errorRows: number;
  errors: Array<{ row: number; error: string }>;
  executionTime: number;
}

interface ImportOptions {
  createTable: boolean;
  truncateBeforeImport: boolean;
  skipErrors: boolean;
  batchSize: number;
}

interface ApiResponse<T = any> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
}

// Main Component
export default function CompleteSchemaManagementSystem() {
  const { data: session } = useSession();
  
  // หลัก State สำหรับการจัดการข้อมูลทั้งระบบ
  const [schemas, setSchemas] = useState<SchemaInfo[]>([]);
  const [selectedSchema, setSelectedSchema] = useState<string>('public');
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState<'schemas' | 'import' | 'history'>('schemas');
  
  // Schema Management States - จัดการการสร้างและลบ schema
  const [showCreateSchema, setShowCreateSchema] = useState(false);
  const [newSchemaName, setNewSchemaName] = useState('');
  const [newSchemaDescription, setNewSchemaDescription] = useState('');
  
  // Table Management States - จัดการการสร้างตารางและ columns
  const [showCreateTable, setShowCreateTable] = useState(false);
  const [newTableName, setNewTableName] = useState('');
  const [newTableDescription, setNewTableDescription] = useState('');
  const [columns, setColumns] = useState<DatabaseColumn[]>([
    { name: 'id', type: 'SERIAL', isPrimary: true, isRequired: true, comment: 'Primary key' }
  ]);
  
  // File Import States - จัดการการ upload และ import ไฟล์
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [filePreview, setFilePreview] = useState<FilePreview | null>(null);
  const [importResult, setImportResult] = useState<ImportResult | null>(null);
  const [importOptions, setImportOptions] = useState<ImportOptions>({
    createTable: false,
    truncateBeforeImport: false,
    skipErrors: true,
    batchSize: 1000
  });
  
  // UI Control States - จัดการ interface และการค้นหา
  const [searchTerm, setSearchTerm] = useState('');
  const [filterSchema, setFilterSchema] = useState('all');
  const [isDragActive, setIsDragActive] = useState(false);

  // Effects - โหลดข้อมูลเมื่อ component mount
  useEffect(() => {
    if (session?.user?.companyCode) {
      loadSchemas();
    }
  }, [session]);

  // Helper function สำหรับเรียก API แบบมี type safety
  const apiCall = async <T,>(url: string, options: RequestInit = {}): Promise<T> => {
    try {
      const response = await fetch(url, {
        ...options,
        headers: {
          'Content-Type': 'application/json',
          ...options.headers,
        },
      });

      const result: ApiResponse<T> = await response.json();

      if (!result.success) {
        throw new Error(result.error || 'API call failed');
      }

      return result.data as T;
    } catch (error) {
      console.error('API Error:', error);
      throw error;
    }
  };

  // Schema Management Functions
  const loadSchemas = async () => {
    setLoading(true);
    try {
      const data = await apiCall<SchemaInfo[]>('/api/services/schemas');
      setSchemas(data);
      toast.success('โหลดข้อมูล schema สำเร็จ');
    } catch (error) {
      toast.error('ไม่สามารถโหลดข้อมูล schema ได้');
      console.error('Load schemas error:', error);
    } finally {
      setLoading(false);
    }
  };

  const createSchema = async () => {
    if (!newSchemaName.trim()) {
      toast.error('กรุณาใส่ชื่อ schema');
      return;
    }

    setLoading(true);
    try {
      await apiCall('/api/services/schemas', {
        method: 'POST',
        body: JSON.stringify({
          name: newSchemaName,
          description: newSchemaDescription
        })
      });

      toast.success('สร้าง schema สำเร็จ');
      setNewSchemaName('');
      setNewSchemaDescription('');
      setShowCreateSchema(false);
      loadSchemas();
    } catch (error) {
      toast.error('ไม่สามารถสร้าง schema ได้');
      console.error('Create schema error:', error);
    } finally {
      setLoading(false);
    }
  };

  const deleteSchema = async (schemaName: string) => {
    if (!confirm(`คุณแน่ใจหรือไม่ที่จะลบ schema "${schemaName}"?`)) {
      return;
    }

    setLoading(true);
    try {
      await apiCall('/api/services/schemas', {
        method: 'DELETE',
        body: JSON.stringify({ name: schemaName, cascade: false })
      });

      toast.success('ลบ schema สำเร็จ');
      loadSchemas();
    } catch (error) {
      toast.error('ไม่สามารถลบ schema ได้');
      console.error('Delete schema error:', error);
    } finally {
      setLoading(false);
    }
  };

  // Table Management Functions
  const createTable = async () => {
    if (!newTableName.trim()) {
      toast.error('กรุณาใส่ชื่อตาราง');
      return;
    }

    if (columns.length === 0) {
      toast.error('ต้องมีอย่างน้อย 1 column');
      return;
    }

    setLoading(true);
    try {
      await apiCall('/api/services/tables', {
        method: 'POST',
        body: JSON.stringify({
          schema: selectedSchema,
          tableName: newTableName,
          description: newTableDescription,
          columns: columns
        })
      });

      toast.success('สร้างตารางสำเร็จ');
      resetTableForm();
      setShowCreateTable(false);
      loadSchemas();
    } catch (error) {
      toast.error('ไม่สามารถสร้างตารางได้');
      console.error('Create table error:', error);
    } finally {
      setLoading(false);
    }
  };

  const deleteTable = async (schema: string, tableName: string) => {
    if (!confirm(`คุณแน่ใจหรือไม่ที่จะลบตาราง "${schema}"."${tableName}"?`)) {
      return;
    }

    setLoading(true);
    try {
      await apiCall('/api/services/tables', {
        method: 'DELETE',
        body: JSON.stringify({ 
          schema: schema,
          tableName: tableName,
          cascade: false 
        })
      });

      toast.success('ลบตารางสำเร็จ');
      loadSchemas();
    } catch (error) {
      toast.error('ไม่สามารถลบตารางได้');
      console.error('Delete table error:', error);
    } finally {
      setLoading(false);
    }
  };

  // Helper function สำหรับ reset form
  const resetTableForm = () => {
    setNewTableName('');
    setNewTableDescription('');
    setColumns([{ 
      name: 'id', 
      type: 'SERIAL', 
      isPrimary: true, 
      isRequired: true, 
      comment: 'Primary key' 
    }]);
  };

  // File Handling Functions
  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      setSelectedFile(file);
      previewFile(file);
    }
  };

  const handleFileDrop = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    setIsDragActive(false);
    const file = event.dataTransfer.files[0];
    if (file) {
      setSelectedFile(file);
      previewFile(file);
    }
  };

  const handleDragOver = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    setIsDragActive(true);
  };

  const handleDragLeave = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    setIsDragActive(false);
  };

  const previewFile = async (file: File) => {
    const formData = new FormData();
    formData.append('file', file);

    try {
      const response = await fetch('/api/services/preview', {
        method: 'POST',
        body: formData
      });

      const result: ApiResponse<FilePreview> = await response.json();
      
      if (result.success && result.data) {
        setFilePreview(result.data);
        // ตั้งชื่อตารางเป็นชื่อไฟล์โดยอัตโนมัติ
        if (!newTableName) {
          setNewTableName(result.data.fileName.split('.')[0]);
        }
        toast.success('Preview ไฟล์สำเร็จ');
      }
    } catch (error) {
      toast.error('ไม่สามารถ preview ไฟล์ได้');
      console.error('Preview error:', error);
    }
  };

  const importFile = async () => {
    if (!selectedFile || !filePreview) {
      toast.error('กรุณาเลือกไฟล์');
      return;
    }

    if (!newTableName.trim()) {
      toast.error('กรุณาใส่ชื่อตาราง');
      return;
    }

    setLoading(true);
    const formData = new FormData();
    formData.append('file', selectedFile);
    formData.append('schema', selectedSchema);
    formData.append('tableName', newTableName);
    formData.append('createTable', importOptions.createTable.toString());
    formData.append('truncateBeforeImport', importOptions.truncateBeforeImport.toString());
    formData.append('skipErrors', importOptions.skipErrors.toString());
    formData.append('batchSize', importOptions.batchSize.toString());

    try {
      const response = await fetch('/api/services/import', {
        method: 'POST',
        body: formData
      });

      const result: ApiResponse<ImportResult> = await response.json();
      
      if (result.success && result.data) {
        setImportResult(result.data);
        toast.success(`Import สำเร็จ: ${result.data.successRows}/${result.data.totalRows} แถว`);
        loadSchemas(); // Refresh data
      }
    } catch (error) {
      toast.error('ไม่สามารถ import ไฟล์ได้');
      console.error('Import error:', error);
    } finally {
      setLoading(false);
    }
  };

  // Column Management Functions
  const addColumn = () => {
    setColumns([...columns, {
      name: '',
      type: 'VARCHAR',
      length: 255,
      isPrimary: false,
      isRequired: false,
      isUnique: false,
      comment: ''
    }]);
  };

  const removeColumn = (index: number) => {
    if (columns.length > 1) {
      setColumns(columns.filter((_, i) => i !== index));
    }
  };

  const updateColumn = (index: number, field: keyof DatabaseColumn, value: any) => {
    const updated = [...columns];
    updated[index] = { ...updated[index], [field]: value };
    setColumns(updated);
  };

  // Utility Functions
  const getTypeColor = (type: string) => {
    const colors: { [key: string]: string } = {
      'SERIAL': 'bg-blue-100 text-blue-800',
      'INTEGER': 'bg-green-100 text-green-800',
      'VARCHAR': 'bg-yellow-100 text-yellow-800',
      'TEXT': 'bg-purple-100 text-purple-800',
      'BOOLEAN': 'bg-red-100 text-red-800',
      'DATE': 'bg-indigo-100 text-indigo-800',
      'TIMESTAMP': 'bg-pink-100 text-pink-800',
      'DECIMAL': 'bg-orange-100 text-orange-800'
    };
    return colors[type] || 'bg-gray-100 text-gray-800';
  };

  // การกรองข้อมูลตามการค้นหา
  const filteredSchemas = schemas.filter(schema => {
    const matchesSearch = schema.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         schema.description?.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesFilter = filterSchema === 'all' || schema.name === filterSchema;
    return matchesSearch && matchesFilter;
  });

  return (
    <div className="max-w-7xl mx-auto px-4 py-6">
      {/* Header Section */}
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">
          ระบบจัดการข้อมูล
        </h1>
        <p className="text-gray-600">
          จัดการ Schema, Tables และ Import ข้อมูลสำหรับบริษัท {session?.user?.companyName}
        </p>
      </div>

      {/* Navigation Tabs */}
      <div className="mb-6">
        <div className="border-b border-gray-200">
          <nav className="-mb-px flex space-x-8">
            {[
              { key: 'schemas', label: 'จัดการ Schema & Tables', icon: Database },
              { key: 'import', label: 'Import ข้อมูล', icon: Upload },
              { key: 'history', label: 'ประวัติการ Import', icon: FileText }
            ].map(({ key, label, icon: Icon }) => (
              <button
                key={key}
                onClick={() => setActiveTab(key as any)}
                className={`flex items-center py-2 px-1 border-b-2 font-medium text-sm transition-colors ${
                  activeTab === key
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                <Icon className="w-5 h-5 mr-2" />
                {label}
              </button>
            ))}
          </nav>
        </div>
      </div>

      {/* Schema Management Tab */}
      {activeTab === 'schemas' && (
        <div className="space-y-6">
          {/* Control Panel */}
          <div className="flex flex-col sm:flex-row gap-4 bg-white p-4 rounded-lg shadow">
            <div className="flex-1">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                <input
                  type="text"
                  placeholder="ค้นหา schema หรือ table..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10 pr-4 py-2 w-full border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
              </div>
            </div>
            
            <div className="flex gap-2">
              <button
                onClick={() => setShowCreateSchema(true)}
                className="flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
              >
                <FolderPlus className="w-4 h-4 mr-2" />
                สร้าง Schema
              </button>
              
              <button
                onClick={() => setShowCreateTable(true)}
                className="flex items-center px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors"
              >
                <Plus className="w-4 h-4 mr-2" />
                สร้าง Table
              </button>
              
              <button
                onClick={loadSchemas}
                disabled={loading}
                className="flex items-center px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors disabled:opacity-50"
              >
                <RefreshCw className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
                รีเฟรช
              </button>
            </div>
          </div>

          {/* Schema Cards */}
          <div className="grid gap-6">
            {filteredSchemas.map((schema) => (
              <div key={schema.name} className="bg-white rounded-lg shadow-lg overflow-hidden">
                <div className="bg-gray-50 px-6 py-4 border-b">
                  <div className="flex items-center justify-between">
                    <div>
                      <h3 className="text-lg font-semibold text-gray-900 flex items-center">
                        <Database className="w-5 h-5 mr-2 text-blue-600" />
                        {schema.name}
                      </h3>
                      {schema.description && (
                        <p className="text-sm text-gray-600 mt-1">{schema.description}</p>
                      )}
                    </div>
                    
                    <div className="flex items-center gap-2">
                      <span className="px-3 py-1 bg-blue-100 text-blue-800 rounded-full text-sm font-medium">
                        {schema.tables.length} tables
                      </span>
                      
                      {schema.name !== 'public' && (
                        <button
                          onClick={() => deleteSchema(schema.name)}
                          className="p-2 text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                          title="ลบ schema"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      )}
                    </div>
                  </div>
                </div>

                {/* Tables List */}
                {schema.tables.length > 0 ? (
                  <div className="divide-y divide-gray-200">
                    {schema.tables.map((table) => (
                      <div key={`${table.schema}.${table.name}`} className="px-6 py-4 hover:bg-gray-50 transition-colors">
                        <div className="flex items-center justify-between">
                          <div className="flex items-center">
                            <Table2 className="w-4 h-4 mr-3 text-gray-400" />
                            <div>
                              <h4 className="font-medium text-gray-900">{table.name}</h4>
                              {table.comment && (
                                <p className="text-sm text-gray-600">{table.comment}</p>
                              )}
                            </div>
                          </div>
                          
                          <div className="flex items-center gap-3">
                            <span className="text-sm text-gray-500">
                              {table.columnCount} columns
                            </span>
                            
                            {table.hasData && (
                              <span className="px-2 py-1 bg-green-100 text-green-800 rounded text-xs">
                                มีข้อมูล
                              </span>
                            )}
                            
                            <button
                              onClick={() => deleteTable(table.schema, table.name)}
                              className="p-1 text-red-600 hover:bg-red-50 rounded transition-colors"
                              title="ลบตาราง"
                            >
                              <Trash2 className="w-3 h-3" />
                            </button>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="px-6 py-8 text-center text-gray-500">
                    <Table2 className="w-12 h-12 mx-auto mb-3 text-gray-300" />
                    <p>ไม่มีตารางใน schema นี้</p>
                  </div>
                )}
              </div>
            ))}
          </div>

          {/* Empty State */}
          {filteredSchemas.length === 0 && (
            <div className="text-center py-12">
              <Database className="w-16 h-16 mx-auto mb-4 text-gray-300" />
              <p className="text-gray-500 text-lg">ไม่พบ schema ที่ตรงกับการค้นหา</p>
            </div>
          )}
        </div>
      )}

      {/* Import Tab */}
      {activeTab === 'import' && (
        <FileImportSection
          schemas={schemas}
          selectedSchema={selectedSchema}
          setSelectedSchema={setSelectedSchema}
          selectedFile={selectedFile}
          filePreview={filePreview}
          importResult={importResult}
          importOptions={importOptions}
          setImportOptions={setImportOptions}
          newTableName={newTableName}
          setNewTableName={setNewTableName}
          loading={loading}
          isDragActive={isDragActive}
          onFileSelect={handleFileSelect}
          onFileDrop={handleFileDrop}
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
          onImport={importFile}
        />
      )}

      {/* History Tab */}
      {activeTab === 'history' && (
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4">ประวัติการ Import</h3>
          <div className="text-center py-12 text-gray-500">
            <FileText className="w-16 h-16 mx-auto mb-4 text-gray-300" />
            <p>ฟีเจอร์ประวัติการ Import จะเพิ่มในเร็วๆ นี้</p>
          </div>
        </div>
      )}

      {/* Modals */}
      <CreateSchemaModal
        show={showCreateSchema}
        onClose={() => setShowCreateSchema(false)}
        newSchemaName={newSchemaName}
        setNewSchemaName={setNewSchemaName}
        newSchemaDescription={newSchemaDescription}
        setNewSchemaDescription={setNewSchemaDescription}
        onCreate={createSchema}
        loading={loading}
      />

      <CreateTableModal
        show={showCreateTable}
        onClose={() => {
          setShowCreateTable(false);
          resetTableForm();
        }}
        schemas={schemas}
        selectedSchema={selectedSchema}
        setSelectedSchema={setSelectedSchema}
        newTableName={newTableName}
        setNewTableName={setNewTableName}
        newTableDescription={newTableDescription}
        setNewTableDescription={setNewTableDescription}
        columns={columns}
        onAddColumn={addColumn}
        onRemoveColumn={removeColumn}
        onUpdateColumn={updateColumn}
        onCreate={createTable}
        loading={loading}
        getTypeColor={getTypeColor}
      />

      {/* Loading Overlay */}
      {loading && (
        <div className="fixed inset-0 bg-black bg-opacity-25 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 flex items-center">
            <RefreshCw className="w-6 h-6 mr-3 animate-spin text-blue-600" />
            <span className="text-gray-700">กำลังประมวลผล...</span>
          </div>
        </div>
      )}
    </div>
  );
}

// Sub-components สำหรับแยกความซับซ้อน

interface FileImportSectionProps {
  schemas: SchemaInfo[];
  selectedSchema: string;
  setSelectedSchema: (schema: string) => void;
  selectedFile: File | null;
  filePreview: FilePreview | null;
  importResult: ImportResult | null;
  importOptions: ImportOptions;
  setImportOptions: (options: ImportOptions | ((prev: ImportOptions) => ImportOptions)) => void;
  newTableName: string;
  setNewTableName: (name: string) => void;
  loading: boolean;
  isDragActive: boolean;
  onFileSelect: (event: React.ChangeEvent<HTMLInputElement>) => void;
  onFileDrop: (event: React.DragEvent<HTMLDivElement>) => void;
  onDragOver: (event: React.DragEvent<HTMLDivElement>) => void;
  onDragLeave: (event: React.DragEvent<HTMLDivElement>) => void;
  onImport: () => void;
}

function FileImportSection({
  schemas,
  selectedSchema,
  setSelectedSchema,
  selectedFile,
  filePreview,
  importResult,
  importOptions,
  setImportOptions,
  newTableName,
  setNewTableName,
  loading,
  isDragActive,
  onFileSelect,
  onFileDrop,
  onDragOver,
  onDragLeave,
  onImport
}: FileImportSectionProps) {
  return (
    <div className="space-y-6">
      {/* File Upload Area */}
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold mb-4">เลือกไฟล์สำหรับ Import</h3>
        
        <div
          onDrop={onFileDrop}
          onDragOver={onDragOver}
          onDragLeave={onDragLeave}
          className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors ${
            isDragActive 
              ? 'border-blue-400 bg-blue-50' 
              : 'border-gray-300 hover:border-blue-400'
          }`}
        >
          <Upload className="w-12 h-12 mx-auto mb-4 text-gray-400" />
          <p className="text-gray-600 mb-2">
            ลากไฟล์มาวางที่นี่ หรือ คลิกเพื่อเลือกไฟล์
          </p>
          <input
            type="file"
            onChange={onFileSelect}
            accept=".csv,.xlsx,.xls,.json,.txt,.tsv"
            className="hidden"
            id="file-upload"
          />
          <label
            htmlFor="file-upload"
            className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 cursor-pointer transition-colors"
          >
            เลือกไฟล์
          </label>
          <p className="text-xs text-gray-500 mt-2">
            รองรับ: CSV, Excel (.xlsx, .xls), JSON, TXT, TSV
          </p>
        </div>

        {selectedFile && (
          <div className="mt-4 p-4 bg-blue-50 rounded-lg">
            <div className="flex items-center">
              <FileText className="w-5 h-5 mr-2 text-blue-600" />
              <span className="font-medium">{selectedFile.name}</span>
              <span className="ml-2 text-sm text-gray-600">
                ({(selectedFile.size / 1024 / 1024).toFixed(2)} MB)
              </span>
            </div>
          </div>
        )}
      </div>

      {/* File Preview */}
      {filePreview && (
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4">Preview ข้อมูล</h3>
          
          <div className="mb-4 text-sm text-gray-600">
            <p>ไฟล์: {filePreview.fileName} ({filePreview.fileType})</p>
            <p>จำนวนแถว: {filePreview.totalRows.toLocaleString()} แถว</p>
            <p>จำนวน Columns: {filePreview.headers.length} columns</p>
          </div>

          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  {filePreview.headers.map((header, index) => (
                    <th key={index} className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      {header}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {filePreview.sampleData.slice(0, 5).map((row, rowIndex) => (
                  <tr key={rowIndex}>
                    {filePreview.headers.map((header, colIndex) => (
                      <td key={colIndex} className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {String(row[header] || '-')}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Import Options */}
      {filePreview && (
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4">ตัวเลือกการ Import</h3>
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Schema ปลายทาง
              </label>
              <select
                value={selectedSchema}
                onChange={(e) => setSelectedSchema(e.target.value)}
                className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                {schemas.map(schema => (
                  <option key={schema.name} value={schema.name}>
                    {schema.name}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                ชื่อตาราง
              </label>
              <input
                type="text"
                value={newTableName}
                onChange={(e) => setNewTableName(e.target.value)}
                placeholder={filePreview.fileName.split('.')[0]}
                className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
          </div>

          <div className="mt-6 space-y-4">
            <div className="flex items-center">
              <input
                type="checkbox"
                id="create-table"
                checked={importOptions.createTable}
                onChange={(e) => setImportOptions((prev: ImportOptions) => ({ ...prev, createTable: e.target.checked }))}
                className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
              />
              <label htmlFor="create-table" className="ml-2 text-sm text-gray-700">
                สร้างตารางใหม่อัตโนมัติ (ถ้าไม่มี)
              </label>
            </div>

            <div className="flex items-center">
              <input
                type="checkbox"
                id="truncate-table"
                checked={importOptions.truncateBeforeImport}
                onChange={(e) => setImportOptions((prev: ImportOptions) => ({ ...prev, truncateBeforeImport: e.target.checked }))}
                className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
              />
              <label htmlFor="truncate-table" className="ml-2 text-sm text-gray-700">
                ล้างข้อมูลเดิมก่อน import
              </label>
            </div>

            <div className="flex items-center">
              <input
                type="checkbox"
                id="skip-errors"
                checked={importOptions.skipErrors}
                onChange={(e) => setImportOptions((prev: ImportOptions) => ({ ...prev, skipErrors: e.target.checked }))}
                className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
              />
              <label htmlFor="skip-errors" className="ml-2 text-sm text-gray-700">
                ข้าม row ที่มี error
              </label>
            </div>
          </div>

          <div className="mt-6">
            <button
              onClick={onImport}
              disabled={loading || !selectedFile}
              className="w-full flex items-center justify-center px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {loading ? (
                <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
              ) : (
                <Upload className="w-4 h-4 mr-2" />
              )}
              เริ่ม Import ข้อมูล
            </button>
          </div>
        </div>
      )}

      {/* Import Result */}
      {importResult && (
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4">ผลการ Import</h3>
          
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
            <div className="bg-blue-50 p-4 rounded-lg">
              <div className="text-2xl font-bold text-blue-600">{importResult.totalRows.toLocaleString()}</div>
              <div className="text-sm text-blue-600">Total Rows</div>
            </div>
            
            <div className="bg-green-50 p-4 rounded-lg">
              <div className="text-2xl font-bold text-green-600">{importResult.successRows.toLocaleString()}</div>
              <div className="text-sm text-green-600">Success</div>
            </div>
            
            <div className="bg-red-50 p-4 rounded-lg">
              <div className="text-2xl font-bold text-red-600">{importResult.errorRows.toLocaleString()}</div>
              <div className="text-sm text-red-600">Errors</div>
            </div>
            
            <div className="bg-gray-50 p-4 rounded-lg">
              <div className="text-2xl font-bold text-gray-600">{importResult.executionTime.toLocaleString()}ms</div>
              <div className="text-sm text-gray-600">Duration</div>
            </div>
          </div>

          {importResult.errors.length > 0 && (
            <div className="mt-4">
              <h4 className="font-medium text-red-600 mb-2">Errors ({importResult.errors.length})</h4>
              <div className="max-h-40 overflow-y-auto bg-red-50 rounded-lg p-3">
                {importResult.errors.slice(0, 10).map((error, index) => (
                  <div key={index} className="text-sm text-red-700 mb-1">
                    Row {error.row}: {error.error}
                  </div>
                ))}
                {importResult.errors.length > 10 && (
                  <div className="text-sm text-red-600 italic">
                    ... และอีก {importResult.errors.length - 10} errors
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// Create Schema Modal Component
interface CreateSchemaModalProps {
  show: boolean;
  onClose: () => void;
  newSchemaName: string;
  setNewSchemaName: (name: string) => void;
  newSchemaDescription: string;
  setNewSchemaDescription: (desc: string) => void;
  onCreate: () => void;
  loading: boolean;
}

function CreateSchemaModal({
  show,
  onClose,
  newSchemaName,
  setNewSchemaName,
  newSchemaDescription,
  setNewSchemaDescription,
  onCreate,
  loading
}: CreateSchemaModalProps) {
  if (!show) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg p-6 w-full max-w-md mx-4">
        <h3 className="text-lg font-semibold mb-4">สร้าง Schema ใหม่</h3>
        
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              ชื่อ Schema *
            </label>
            <input
              type="text"
              value={newSchemaName}
              onChange={(e) => setNewSchemaName(e.target.value)}
              className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              placeholder="เช่น sales, inventory"
            />
          </div>
          
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              คำอธิบาย
            </label>
            <textarea
              value={newSchemaDescription}
              onChange={(e) => setNewSchemaDescription(e.target.value)}
              className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              rows={3}
              placeholder="คำอธิบายเกี่ยวกับ schema นี้"
            />
          </div>
        </div>
        
        <div className="flex gap-3 mt-6">
          <button
            onClick={onClose}
            className="flex-1 px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
          >
            ยกเลิก
          </button>
          <button
            onClick={onCreate}
            disabled={loading || !newSchemaName.trim()}
            className="flex-1 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
          >
            {loading ? 'กำลังสร้าง...' : 'สร้าง Schema'}
          </button>
        </div>
      </div>
    </div>
  );
}

// Create Table Modal Component
interface CreateTableModalProps {
  show: boolean;
  onClose: () => void;
  schemas: SchemaInfo[];
  selectedSchema: string;
  setSelectedSchema: (schema: string) => void;
  newTableName: string;
  setNewTableName: (name: string) => void;
  newTableDescription: string;
  setNewTableDescription: (desc: string) => void;
  columns: DatabaseColumn[];
  onAddColumn: () => void;
  onRemoveColumn: (index: number) => void;
  onUpdateColumn: (index: number, field: keyof DatabaseColumn, value: any) => void;
  onCreate: () => void;
  loading: boolean;
  getTypeColor: (type: string) => string;
}

function CreateTableModal({
  show,
  onClose,
  schemas,
  selectedSchema,
  setSelectedSchema,
  newTableName,
  setNewTableName,
  newTableDescription,
  setNewTableDescription,
  columns,
  onAddColumn,
  onRemoveColumn,
  onUpdateColumn,
  onCreate,
  loading,
  getTypeColor
}: CreateTableModalProps) {
  if (!show) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 overflow-y-auto">
      <div className="bg-white rounded-lg p-6 w-full max-w-6xl mx-4 my-8">
        <h3 className="text-lg font-semibold mb-4">สร้างตารางใหม่</h3>
        
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Schema
            </label>
            <select
              value={selectedSchema}
              onChange={(e) => setSelectedSchema(e.target.value)}
              className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              {schemas.map(schema => (
                <option key={schema.name} value={schema.name}>
                  {schema.name}
                </option>
              ))}
            </select>
          </div>
          
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              ชื่อตาราง *
            </label>
            <input
              type="text"
              value={newTableName}
              onChange={(e) => setNewTableName(e.target.value)}
              className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              placeholder="เช่น customers, products"
            />
          </div>
        </div>

        <div className="mb-6">
          <label className="block text-sm font-medium text-gray-700 mb-2">
            คำอธิบาย
          </label>
          <input
            type="text"
            value={newTableDescription}
            onChange={(e) => setNewTableDescription(e.target.value)}
            className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            placeholder="คำอธิบายเกี่ยวกับตารางนี้"
          />
        </div>

        {/* Column Management */}
        <div className="mb-6">
          <div className="flex items-center justify-between mb-4">
            <h4 className="font-medium text-gray-900">Columns</h4>
            <button
              onClick={onAddColumn}
              className="flex items-center px-3 py-1 bg-green-600 text-white rounded text-sm hover:bg-green-700 transition-colors"
            >
              <Plus className="w-4 h-4 mr-1" />
              เพิ่ม Column
            </button>
          </div>

          <div className="space-y-3 max-h-96 overflow-y-auto">
            {columns.map((column, index) => (
              <div key={index} className="grid grid-cols-12 gap-2 items-center p-3 bg-gray-50 rounded-lg">
                <div className="col-span-3">
                  <input
                    type="text"
                    value={column.name}
                    onChange={(e) => onUpdateColumn(index, 'name', e.target.value)}
                    placeholder="ชื่อ column"
                    className="w-full border rounded px-2 py-1 text-sm focus:ring-1 focus:ring-blue-500 focus:border-transparent"
                  />
                </div>

                <div className="col-span-2">
                  <select
                    value={column.type}
                    onChange={(e) => onUpdateColumn(index, 'type', e.target.value)}
                    className="w-full border rounded px-2 py-1 text-sm focus:ring-1 focus:ring-blue-500 focus:border-transparent"
                  >
                    <option value="SERIAL">SERIAL</option>
                    <option value="INTEGER">INTEGER</option>
                    <option value="BIGINT">BIGINT</option>
                    <option value="VARCHAR">VARCHAR</option>
                    <option value="TEXT">TEXT</option>
                    <option value="BOOLEAN">BOOLEAN</option>
                    <option value="DATE">DATE</option>
                    <option value="TIMESTAMP">TIMESTAMP</option>
                    <option value="DECIMAL">DECIMAL</option>
                    <option value="JSON">JSON</option>
                    <option value="UUID">UUID</option>
                  </select>
                </div>

                {(column.type === 'VARCHAR' || column.type === 'DECIMAL') && (
                  <div className="col-span-1">
                    <input
                      type="number"
                      value={column.length || ''}
                      onChange={(e) => onUpdateColumn(index, 'length', parseInt(e.target.value) || undefined)}
                      placeholder="Length"
                      className="w-full border rounded px-2 py-1 text-sm focus:ring-1 focus:ring-blue-500 focus:border-transparent"
                    />
                  </div>
                )}

                <div className="col-span-4 flex items-center gap-3">
                  <label className="flex items-center text-xs">
                    <input
                      type="checkbox"
                      checked={column.isPrimary || false}
                      onChange={(e) => onUpdateColumn(index, 'isPrimary', e.target.checked)}
                      className="mr-1"
                    />
                    PK
                  </label>
                  
                  <label className="flex items-center text-xs">
                    <input
                      type="checkbox"
                      checked={column.isRequired || false}
                      onChange={(e) => onUpdateColumn(index, 'isRequired', e.target.checked)}
                      className="mr-1"
                    />
                    Required
                  </label>
                  
                  <label className="flex items-center text-xs">
                    <input
                      type="checkbox"
                      checked={column.isUnique || false}
                      onChange={(e) => onUpdateColumn(index, 'isUnique', e.target.checked)}
                      className="mr-1"
                    />
                    Unique
                  </label>
                </div>

                <div className="col-span-1 flex justify-end">
                  {columns.length > 1 && (
                    <button
                      onClick={() => onRemoveColumn(index)}
                      className="p-1 text-red-600 hover:bg-red-50 rounded transition-colors"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="flex gap-3">
          <button
            onClick={onClose}
            className="flex-1 px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
          >
            ยกเลิก
          </button>
          <button
            onClick={onCreate}
            disabled={loading || !newTableName.trim() || columns.length === 0}
            className="flex-1 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
          >
            {loading ? 'กำลังสร้าง...' : 'สร้างตาราง'}
          </button>
        </div>
      </div>
    </div>
  );
}