// SchemaManagement.tsx - Component สำหรับจัดการ schemas
import React, { useState } from 'react';
import { Database, Plus, Trash2, RefreshCw, Search, FolderPlus, Table2, Eye } from 'lucide-react';
import type { SchemaManagementProps, SchemaInfo } from './types';
import { apiCall, validateIdentifier, filterSchemas, showSuccessToast, showErrorToast } from './utils';

interface CreateSchemaModalProps {
  isOpen: boolean;
  onClose: () => void;
  onCreateSuccess: () => void;
  loading: boolean;
  setLoading: (loading: boolean) => void;
}

// Modal component สำหรับสร้าง schema ใหม่
function CreateSchemaModal({ isOpen, onClose, onCreateSuccess, loading, setLoading }: CreateSchemaModalProps) {
  const [schemaName, setSchemaName] = useState('');
  const [description, setDescription] = useState('');

  const handleCreate = async () => {
    // Validate ชื่อ schema ก่อนส่ง
    const validation = validateIdentifier(schemaName);
    if (!validation.isValid) {
      showErrorToast(validation.error!);
      return;
    }

    setLoading(true);
    try {
      await apiCall('/api/services/schemas', {
        method: 'POST',
        body: JSON.stringify({
          name: schemaName,
          description: description.trim() || undefined
        })
      });

      showSuccessToast('สร้าง schema สำเร็จ');
      setSchemaName('');
      setDescription('');
      onClose();
      onCreateSuccess();
    } catch (error) {
      showErrorToast('ไม่สามารถสร้าง schema ได้');
      console.error('Create schema error:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleClose = () => {
    if (!loading) {
      setSchemaName('');
      setDescription('');
      onClose();
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl p-6 w-full max-w-md mx-4">
        <h3 className="text-lg font-semibold mb-4 flex items-center">
          <FolderPlus className="w-5 h-5 mr-2 text-blue-600" />
          สร้าง Schema ใหม่
        </h3>
        
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              ชื่อ Schema *
            </label>
            <input
              type="text"
              value={schemaName}
              onChange={(e) => setSchemaName(e.target.value)}
              className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              placeholder="เช่น sales, inventory, user_management"
              disabled={loading}
            />
            <p className="text-xs text-gray-500 mt-1">
              ชื่อต้องขึ้นต้นด้วยตัวอักษร และประกอบด้วยตัวอักษร ตัวเลข หรือ _ เท่านั้น
            </p>
          </div>
          
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              คำอธิบาย
            </label>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              rows={3}
              placeholder="คำอธิบายเกี่ยวกับ schema นี้"
              disabled={loading}
            />
          </div>
        </div>
        
        <div className="flex gap-3 mt-6">
          <button
            onClick={handleClose}
            disabled={loading}
            className="flex-1 px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50 transition-colors"
          >
            ยกเลิก
          </button>
          <button
            onClick={handleCreate}
            disabled={loading || !schemaName.trim()}
            className="flex-1 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors flex items-center justify-center"
          >
            {loading ? (
              <>
                <RefreshCw className="w-4 h-4 animate-spin mr-2" />
                กำลังสร้าง...
              </>
            ) : (
              'สร้าง Schema'
            )}
          </button>
        </div>
      </div>
    </div>
  );
}

// Main SchemaManagement component
export default function SchemaManagement({
  schemas,
  selectedSchema,
  loading,
  onSchemaSelect,
  onLoadingChange,
  onRefresh
}: SchemaManagementProps) {
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');

  // ฟังก์ชันสำหรับลบ schema พร้อมการ confirm
  const handleDeleteSchema = async (schemaName: string) => {
    // ป้องกันการลบ schema หลักที่สำคัญ
    const protectedSchemas = ['public', 'information_schema', 'pg_catalog'];
    if (protectedSchemas.includes(schemaName)) {
      showErrorToast('ไม่สามารถลบ schema นี้ได้เนื่องจากเป็น system schema');
      return;
    }

    if (!confirm(`คุณแน่ใจหรือไม่ที่จะลบ schema "${schemaName}"?\n\nการลบ schema จะทำให้ตารางและข้อมูลทั้งหมดใน schema นี้หายไปด้วย`)) {
      return;
    }

    onLoadingChange(true);
    try {
      await apiCall('/api/services/schemas', {
        method: 'DELETE',
        body: JSON.stringify({ name: schemaName, cascade: true })
      });

      showSuccessToast('ลบ schema สำเร็จ');
      
      // หาก schema ที่ลบคือ schema ที่เลือกอยู่ ให้เปลี่ยนไปที่ public
      if (selectedSchema === schemaName) {
        onSchemaSelect('public');
      }
      
      onRefresh();
    } catch (error) {
      showErrorToast('ไม่สามารถลบ schema ได้');
      console.error('Delete schema error:', error);
    } finally {
      onLoadingChange(false);
    }
  };

  // Filter schemas ตามคำค้นหา
  const filteredSchemas = filterSchemas(schemas, searchTerm);

  return (
    <div className="space-y-6">
      {/* Header section พร้อม controls */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-2xl font-bold text-gray-900 flex items-center">
              <Database className="w-8 h-8 mr-3 text-blue-600" />
              จัดการ Database Schemas
            </h2>
            <p className="text-gray-600 mt-1">
              สร้าง แก้ไข และจัดการ schemas ใน database
            </p>
          </div>
          
          <div className="flex items-center gap-3">
            <button
              onClick={() => setShowCreateModal(true)}
              className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg flex items-center transition-colors"
              disabled={loading}
            >
              <Plus className="w-4 h-4 mr-2" />
              สร้าง Schema ใหม่
            </button>
            
            <button
              onClick={onRefresh}
              disabled={loading}
              className="border border-gray-300 hover:bg-gray-50 px-4 py-2 rounded-lg flex items-center transition-colors disabled:opacity-50"
            >
              <RefreshCw className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
              รีเฟรช
            </button>
          </div>
        </div>

        {/* Search bar */}
        <div className="relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
          <input
            type="text"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-10 pr-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            placeholder="ค้นหา schema หรือตาราง..."
          />
        </div>
      </div>

      {/* Schema cards */}
      <div className="grid gap-6">
        {filteredSchemas.length === 0 ? (
          <div className="bg-white rounded-lg shadow p-12 text-center">
            <Database className="w-16 h-16 text-gray-300 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-gray-900 mb-2">
              {searchTerm ? 'ไม่พบ Schema' : 'ยังไม่มี Schema'}
            </h3>
            <p className="text-gray-600 mb-6">
              {searchTerm 
                ? `ไม่พบ schema ที่ตรงกับ "${searchTerm}"`
                : 'เริ่มต้นด้วยการสร้าง schema แรกของคุณ'
              }
            </p>
            {!searchTerm && (
              <button
                onClick={() => setShowCreateModal(true)}
                className="bg-blue-600 hover:bg-blue-700 text-white px-6 py-2 rounded-lg inline-flex items-center"
              >
                <Plus className="w-4 h-4 mr-2" />
                สร้าง Schema ใหม่
              </button>
            )}
          </div>
        ) : (
          filteredSchemas.map((schema) => (
            <div 
              key={schema.name} 
              className={`bg-white rounded-lg shadow-lg overflow-hidden transition-all duration-200 ${
                selectedSchema === schema.name ? 'ring-2 ring-blue-500 shadow-xl' : 'hover:shadow-xl'
              }`}
            >
              <div className="bg-gradient-to-r from-gray-50 to-blue-50 px-6 py-4 border-b">
                <div className="flex items-center justify-between">
                  <div className="flex-1">
                    <h3 className="text-lg font-semibold text-gray-900 flex items-center">
                      <Database className="w-5 h-5 mr-2 text-blue-600" />
                      {schema.name}
                      {selectedSchema === schema.name && (
                        <span className="ml-2 px-2 py-1 bg-blue-600 text-white text-xs rounded-full">
                          เลือกอยู่
                        </span>
                      )}
                    </h3>
                    {schema.description && (
                      <p className="text-sm text-gray-600 mt-1">{schema.description}</p>
                    )}
                  </div>
                  
                  <div className="flex items-center gap-2">
                    <span className="px-3 py-1 bg-blue-100 text-blue-800 rounded-full text-sm font-medium">
                      {schema.tables.length} ตาราง
                    </span>
                    
                    {selectedSchema !== schema.name && (
                      <button
                        onClick={() => onSchemaSelect(schema.name)}
                        className="p-2 text-blue-600 hover:bg-blue-50 rounded-lg transition-colors"
                        title="เลือก schema นี้"
                      >
                        <Eye className="w-4 h-4" />
                      </button>
                    )}
                    
                    {schema.name !== 'public' && (
                      <button
                        onClick={() => handleDeleteSchema(schema.name)}
                        className="p-2 text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                        title="ลบ schema"
                      >
                        <Trash2 className="w-4 h-4" />
                      </button>
                    )}
                  </div>
                </div>
              </div>

              {/* Tables list */}
              {schema.tables.length > 0 ? (
                <div className="p-6">
                  <h4 className="text-sm font-medium text-gray-700 mb-3 flex items-center">
                    <Table2 className="w-4 h-4 mr-2" />
                    ตารางใน Schema นี้
                  </h4>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                    {schema.tables.map((table) => (
                      <div 
                        key={table.name}
                        className="border rounded-lg p-3 hover:bg-gray-50 transition-colors"
                      >
                        <div className="flex items-center justify-between">
                          <div className="flex-1 min-w-0">
                            <h5 className="font-medium text-gray-900 truncate">
                              {table.name}
                            </h5>
                            {table.comment && (
                              <p className="text-xs text-gray-600 truncate mt-1">
                                {table.comment}
                              </p>
                            )}
                          </div>
                          <div className="ml-2 flex items-center gap-1">
                            <span className="text-xs text-gray-500">
                              {table.columnCount} cols
                            </span>
                            {table.hasData && (
                              <div 
                                className="w-2 h-2 bg-green-400 rounded-full" 
                                title="มีข้อมูล"
                              />
                            )}
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              ) : (
                <div className="p-6 text-center text-gray-500">
                  <Table2 className="w-8 h-8 text-gray-300 mx-auto mb-2" />
                  <p className="text-sm">ยังไม่มีตารางใน schema นี้</p>
                </div>
              )}
            </div>
          ))
        )}
      </div>

      {/* Create Schema Modal */}
      <CreateSchemaModal
        isOpen={showCreateModal}
        onClose={() => setShowCreateModal(false)}
        onCreateSuccess={onRefresh}
        loading={loading}
        setLoading={onLoadingChange}
      />
    </div>
  );
}