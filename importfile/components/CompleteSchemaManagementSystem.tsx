// CompleteSchemaManagementSystem.tsx - Main container component ที่เป็นตัวกลางจัดการ sub-components
import React, { useState, useEffect } from 'react';
import { useSession } from 'next-auth/react';
import { toast } from 'react-hot-toast';

// Import types
import type { SchemaInfo, DatabaseColumn, ImportResult, TabType, ApiResponse } from './types';

// Import utility functions
import { apiCall, validateIdentifier, validateColumns, showSuccessToast, showErrorToast } from './utils';

// Import sub-components
import SchemaManagement from './SchemaManagement';
import TableManagement from './TableManagement';
import FileImport from './FileImport';
import ImportHistory from './ImportHistory';

// Tab configuration
const TABS = [
  { 
    id: 'schemas' as TabType, 
    label: 'จัดการ Schema', 
    icon: '🗃️',
    description: 'สร้าง แก้ไข และจัดการ database schemas'
  },
  { 
    id: 'import' as TabType, 
    label: 'Import ข้อมูล', 
    icon: '📤',
    description: 'Import ข้อมูลจากไฟล์ CSV, Excel หรือ JSON'
  },
  { 
    id: 'history' as TabType, 
    label: 'ประวัติ Import', 
    icon: '📊',
    description: 'ดูประวัติและผลลัพธ์การ import ข้อมูล'
  }
];

// Main CompleteSchemaManagementSystem component
export default function CompleteSchemaManagementSystem() {
  const { data: session } = useSession();
  
  // Core state management
  const [schemas, setSchemas] = useState<SchemaInfo[]>([]);
  const [selectedSchema, setSelectedSchema] = useState<string>('public');
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState<TabType>('schemas');
  const [lastImportResult, setLastImportResult] = useState<ImportResult | null>(null);

  // เช็ค permission ของผู้ใช้
  const hasPermission = () => {
    if (!session?.user?.role) return false;
    return ['ADMIN', 'MANAGER'].includes(session.user.role);
  };

  // โหลดข้อมูล schemas ทั้งหมด
  const loadSchemas = async () => {
    if (!hasPermission()) {
      showErrorToast('คุณไม่มีสิทธิ์เข้าใช้งานส่วนนี้');
      return;
    }

    setLoading(true);
    try {
      const result: ApiResponse<SchemaInfo[]> = await apiCall('/api/services/schemas');
      
      if (result.success && result.data) {
        setSchemas(result.data);
        
        // ตรวจสอบว่า selectedSchema ยังมีอยู่หรือไม่
        const schemaExists = result.data.some(schema => schema.name === selectedSchema);
        if (!schemaExists && result.data.length > 0) {
          setSelectedSchema(result.data[0].name);
        }
      }
    } catch (error) {
      showErrorToast('ไม่สามารถโหลดข้อมูล schema ได้');
      console.error('Load schemas error:', error);
    } finally {
      setLoading(false);
    }
  };

  // โหลดข้อมูลเมื่อ component mount
  useEffect(() => {
    if (session) {
      loadSchemas();
    }
  }, [session]);

  // Functions สำหรับ Schema Management
  const handleSchemaCreate = () => {
    loadSchemas();
  };

  const handleSchemaDelete = async (schemaName: string) => {
    // ป้องกันการลบ schema สำคัญ
    const protectedSchemas = ['public', 'information_schema', 'pg_catalog'];
    if (protectedSchemas.includes(schemaName)) {
      showErrorToast('ไม่สามารถลบ schema นี้ได้เนื่องจากเป็น system schema');
      return;
    }

    if (!confirm(`คุณแน่ใจหรือไม่ที่จะลบ schema "${schemaName}"?\n\nการลบ schema จะทำให้ตารางและข้อมูลทั้งหมดใน schema นี้หายไปด้วย`)) {
      return;
    }

    setLoading(true);
    try {
      await apiCall('/api/services/schemas', {
        method: 'DELETE',
        body: JSON.stringify({ name: schemaName, cascade: true })
      });

      showSuccessToast('ลบ schema สำเร็จ');
      
      // หาก schema ที่ลบคือ schema ที่เลือกอยู่ ให้เปลี่ยนไปที่ public
      if (selectedSchema === schemaName) {
        setSelectedSchema('public');
      }
      
      loadSchemas();
    } catch (error) {
      showErrorToast('ไม่สามารถลบ schema ได้');
      console.error('Delete schema error:', error);
    } finally {
      setLoading(false);
    }
  };

  // Functions สำหรับ Table Management
  const handleTableCreate = async (
    schema: string,
    tableName: string,
    description: string,
    columns: DatabaseColumn[]
  ) => {
    // Validate table name
    const tableValidation = validateIdentifier(tableName);
    if (!tableValidation.isValid) {
      throw new Error(`ชื่อตาราง: ${tableValidation.error}`);
    }

    // Validate columns
    const columnsValidation = validateColumns(columns);
    if (!columnsValidation.isValid) {
      throw new Error(columnsValidation.error!);
    }

    setLoading(true);
    try {
      await apiCall('/api/services/tables', {
        method: 'POST',
        body: JSON.stringify({
          schema: schema,
          tableName: tableName,
          description: description.trim() || undefined,
          columns: columns
        })
      });

      showSuccessToast('สร้างตารางสำเร็จ');
      loadSchemas(); // Refresh data
    } catch (error) {
      showErrorToast('ไม่สามารถสร้างตารางได้');
      console.error('Create table error:', error);
      throw error; // Re-throw เพื่อให้ component จัดการ loading state
    } finally {
      setLoading(false);
    }
  };

  const handleTableDelete = async (schema: string, tableName: string) => {
    if (!confirm(`คุณแน่ใจหรือไม่ที่จะลบตาราง "${schema}"."${tableName}"?\n\nการลบตารางจะทำให้ข้อมูลทั้งหมดในตารางนี้หายไปด้วย`)) {
      return;
    }

    setLoading(true);
    try {
      await apiCall('/api/services/tables', {
        method: 'DELETE',
        body: JSON.stringify({ 
          schema: schema,
          tableName: tableName,
          cascade: true 
        })
      });

      showSuccessToast('ลบตารางสำเร็จ');
      loadSchemas();
    } catch (error) {
      showErrorToast('ไม่สามารถลบตารางได้');
      console.error('Delete table error:', error);
    } finally {
      setLoading(false);
    }
  };

  // Functions สำหรับ File Import
  const handleImportComplete = (result: ImportResult) => {
    setLastImportResult(result);
    
    // หากมีการสร้างตารางใหม่ ให้ refresh schemas
    if (result.success && result.successRows > 0) {
      loadSchemas();
    }
  };

  // แสดง loading หรือ permission error
  if (!session) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600">กำลังโหลด...</p>
        </div>
      </div>
    );
  }

  if (!hasPermission()) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-center">
          <div className="w-16 h-16 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <span className="text-2xl">🚫</span>
          </div>
          <h2 className="text-xl font-semibold text-gray-900 mb-2">ไม่มีสิทธิ์เข้าใช้งาน</h2>
          <p className="text-gray-600">คุณไม่มีสิทธิ์เข้าใช้งานระบบจัดการ Schema</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-100">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900 mb-2">
            ระบบจัดการ Database Schema
          </h1>
          <p className="text-gray-600">
            จัดการ schemas, ตาราง และ import ข้อมูลอย่างครบวงจร
          </p>
          
          {/* Schema indicator */}
          <div className="mt-4 inline-flex items-center px-3 py-1 bg-blue-100 text-blue-800 rounded-full text-sm">
            <span className="font-medium">Schema ปัจจุบัน:</span>
            <span className="ml-2 font-semibold">{selectedSchema}</span>
          </div>
        </div>

        {/* Tab Navigation */}
        <div className="mb-8">
          <div className="border-b border-gray-200">
            <nav className="-mb-px flex space-x-8">
              {TABS.map((tab) => (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`group inline-flex items-center py-4 px-1 border-b-2 font-medium text-sm transition-colors ${
                    activeTab === tab.id
                      ? 'border-blue-500 text-blue-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                  }`}
                >
                  <span className="text-lg mr-2">{tab.icon}</span>
                  <span>{tab.label}</span>
                </button>
              ))}
            </nav>
          </div>
          
          {/* Tab description */}
          <div className="mt-4">
            <p className="text-sm text-gray-600">
              {TABS.find(tab => tab.id === activeTab)?.description}
            </p>
          </div>
        </div>

        {/* Content Area */}
        <div className="space-y-6">
          {activeTab === 'schemas' && (
            <SchemaManagement
              schemas={schemas}
              selectedSchema={selectedSchema}
              loading={loading}
              onSchemaSelect={setSelectedSchema}
              onLoadingChange={setLoading}
              onRefresh={loadSchemas}
              onSchemaCreate={handleSchemaCreate}
              onSchemaDelete={handleSchemaDelete}
            />
          )}

          {activeTab === 'import' && (
            <div className="space-y-6">
              <TableManagement
                schemas={schemas}
                selectedSchema={selectedSchema}
                loading={loading}
                onLoadingChange={setLoading}
                onRefresh={loadSchemas}
                onTableCreate={handleTableCreate}
                onTableDelete={handleTableDelete}
              />
              
              <FileImport
                schemas={schemas}
                selectedSchema={selectedSchema}
                loading={loading}
                onSchemaSelect={setSelectedSchema}
                onLoadingChange={setLoading}
                onRefresh={loadSchemas}
                onImportComplete={handleImportComplete}
              />
            </div>
          )}

          {activeTab === 'history' && (
            <ImportHistory
              loading={loading}
              onLoadingChange={setLoading}
              onRefresh={loadSchemas}
            />
          )}
        </div>

        {/* Global Loading Overlay */}
        {loading && (
          <div className="fixed inset-0 bg-black bg-opacity-25 flex items-center justify-center z-40">
            <div className="bg-white rounded-lg shadow-xl p-6 flex items-center">
              <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600 mr-3"></div>
              <span className="text-gray-700">กำลังประมวลผล...</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}