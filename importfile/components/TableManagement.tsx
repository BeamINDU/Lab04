// TableManagement.tsx - Component สำหรับจัดการการสร้างตาราง
import React, { useState } from 'react';
import { Table2, Plus, Trash2, RefreshCw, Edit3, Key, AlertCircle } from 'lucide-react';
import type { TableManagementProps, DatabaseColumn } from './types';
import { apiCall, validateIdentifier, validateColumns, createDefaultColumns, showSuccessToast, showErrorToast } from './utils';

// Data type options สำหรับ PostgreSQL
const DATA_TYPES = [
  { value: 'SERIAL', label: 'SERIAL (Auto-increment Integer)' },
  { value: 'INTEGER', label: 'INTEGER' },
  { value: 'BIGINT', label: 'BIGINT' },
  { value: 'VARCHAR', label: 'VARCHAR (Variable Length Text)', hasLength: true },
  { value: 'TEXT', label: 'TEXT (Long Text)' },
  { value: 'BOOLEAN', label: 'BOOLEAN' },
  { value: 'DATE', label: 'DATE' },
  { value: 'TIME', label: 'TIME' },
  { value: 'TIMESTAMP', label: 'TIMESTAMP' },
  { value: 'DECIMAL', label: 'DECIMAL (Precision Numbers)', hasLength: true },
  { value: 'FLOAT', label: 'FLOAT' },
  { value: 'DOUBLE PRECISION', label: 'DOUBLE PRECISION' },
  { value: 'JSON', label: 'JSON' },
  { value: 'JSONB', label: 'JSONB (Binary JSON)' },
  { value: 'UUID', label: 'UUID' }
];

interface ColumnEditorProps {
  column: DatabaseColumn;
  index: number;
  onUpdate: (index: number, updatedColumn: DatabaseColumn) => void;
  onDelete: (index: number) => void;
  canDelete: boolean;
}

// Component สำหรับแก้ไข column แต่ละ column
function ColumnEditor({ column, index, onUpdate, onDelete, canDelete }: ColumnEditorProps) {
  const dataType = DATA_TYPES.find(type => type.value === column.type);
  
  const handleChange = (field: keyof DatabaseColumn, value: any) => {
    const updatedColumn = { ...column, [field]: value };
    
    // ถ้าเป็น SERIAL ให้ auto-set เป็น primary key และ required
    if (field === 'type' && value === 'SERIAL') {
      updatedColumn.isPrimary = true;
      updatedColumn.isRequired = true;
    }
    
    // ถ้าไม่ใช่ primary key แล้วให้ออก isPrimary
    if (field === 'isPrimary' && !value) {
      // ตรวจสอบว่ายังมี primary key อื่นหรือไม่
    }
    
    onUpdate(index, updatedColumn);
  };

  return (
    <div className="border rounded-lg p-4 bg-gray-50">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Column name */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            ชื่อ Column *
          </label>
          <input
            type="text"
            value={column.name}
            onChange={(e) => handleChange('name', e.target.value)}
            className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            placeholder="เช่น user_name, email"
          />
        </div>

        {/* Data type */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            ชนิดข้อมูล *
          </label>
          <select
            value={column.type}
            onChange={(e) => handleChange('type', e.target.value)}
            className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          >
            {DATA_TYPES.map((type) => (
              <option key={type.value} value={type.value}>
                {type.label}
              </option>
            ))}
          </select>
        </div>

        {/* Length (for VARCHAR, DECIMAL, etc.) */}
        {dataType?.hasLength && (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              ความยาว/ความแม่นยำ
            </label>
            <input
              type="number"
              value={column.length || ''}
              onChange={(e) => handleChange('length', e.target.value ? parseInt(e.target.value) : undefined)}
              className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              placeholder="เช่น 255, 10,2"
            />
          </div>
        )}

        {/* Default value */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            ค่าเริ่มต้น
          </label>
          <input
            type="text"
            value={column.defaultValue || ''}
            onChange={(e) => handleChange('defaultValue', e.target.value || undefined)}
            className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            placeholder="เช่น NOW(), 0, 'default'"
          />
        </div>

        {/* Comment */}
        <div className="md:col-span-2">
          <label className="block text-sm font-medium text-gray-700 mb-1">
            คำอธิบาย
          </label>
          <input
            type="text"
            value={column.comment || ''}
            onChange={(e) => handleChange('comment', e.target.value || undefined)}
            className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            placeholder="คำอธิบายเกี่ยวกับ column นี้"
          />
        </div>
      </div>

      {/* Column options */}
      <div className="mt-4 flex flex-wrap gap-4">
        <label className="flex items-center">
          <input
            type="checkbox"
            checked={column.isPrimary || false}
            onChange={(e) => handleChange('isPrimary', e.target.checked)}
            className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
          />
          <Key className="w-4 h-4 ml-2 mr-1 text-yellow-600" />
          <span className="text-sm text-gray-700">Primary Key</span>
        </label>

        <label className="flex items-center">
          <input
            type="checkbox"
            checked={column.isRequired || false}
            onChange={(e) => handleChange('isRequired', e.target.checked)}
            className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
          />
          <span className="text-sm text-gray-700 ml-2">Required (NOT NULL)</span>
        </label>

        <label className="flex items-center">
          <input
            type="checkbox"
            checked={column.isUnique || false}
            onChange={(e) => handleChange('isUnique', e.target.checked)}
            className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
          />
          <span className="text-sm text-gray-700 ml-2">Unique</span>
        </label>
      </div>

      {/* Delete button */}
      {canDelete && (
        <div className="mt-4 flex justify-end">
          <button
            onClick={() => onDelete(index)}
            className="text-red-600 hover:bg-red-50 p-2 rounded-lg transition-colors flex items-center"
          >
            <Trash2 className="w-4 h-4 mr-1" />
            <span className="text-sm">ลบ Column</span>
          </button>
        </div>
      )}
    </div>
  );
}

// Main TableManagement component
export default function TableManagement({
  schemas,
  selectedSchema,
  loading,
  onLoadingChange,
  onRefresh,
  onTableCreate,
  onTableDelete
}: TableManagementProps) {
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [tableName, setTableName] = useState('');
  const [tableDescription, setTableDescription] = useState('');
  const [columns, setColumns] = useState<DatabaseColumn[]>(createDefaultColumns());

  // Reset form
  const resetForm = () => {
    setTableName('');
    setTableDescription('');
    setColumns(createDefaultColumns());
    setShowCreateForm(false);
  };

  // ฟังก์ชันสำหรับเพิ่ม column ใหม่
  const addColumn = () => {
    const newColumn: DatabaseColumn = {
      name: '',
      type: 'VARCHAR',
      length: 255,
      isRequired: false,
      comment: ''
    };
    setColumns([...columns, newColumn]);
  };

  // ฟังก์ชันสำหรับอัปเดต column
  const updateColumn = (index: number, updatedColumn: DatabaseColumn) => {
    const newColumns = [...columns];
    newColumns[index] = updatedColumn;
    setColumns(newColumns);
  };

  // ฟังก์ชันสำหรับลบ column
  const deleteColumn = (index: number) => {
    if (columns.length <= 1) {
      showErrorToast('ต้องมีอย่างน้อย 1 column');
      return;
    }
    
    const newColumns = columns.filter((_, i) => i !== index);
    setColumns(newColumns);
  };

  // ฟังก์ชันสำหรับสร้างตาราง
  const handleCreateTable = async () => {
    // Validate table name
    const tableValidation = validateIdentifier(tableName);
    if (!tableValidation.isValid) {
      showErrorToast(`ชื่อตาราง: ${tableValidation.error}`);
      return;
    }

    // Validate columns
    const columnsValidation = validateColumns(columns);
    if (!columnsValidation.isValid) {
      showErrorToast(columnsValidation.error!);
      return;
    }

    onLoadingChange(true);
    try {
      await onTableCreate(selectedSchema, tableName, tableDescription, columns);
      showSuccessToast('สร้างตารางสำเร็จ');
      resetForm();
    } catch (error) {
      showErrorToast('ไม่สามารถสร้างตารางได้');
      console.error('Create table error:', error);
    } finally {
      onLoadingChange(false);
    }
  };

  // Get current schema info
  const currentSchema = schemas.find(s => s.name === selectedSchema);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-2xl font-bold text-gray-900 flex items-center">
              <Table2 className="w-8 h-8 mr-3 text-green-600" />
              จัดการตาราง
            </h2>
            <p className="text-gray-600 mt-1">
              สร้างและจัดการตารางใน Schema: <span className="font-semibold">{selectedSchema}</span>
            </p>
          </div>
          
          <div className="flex items-center gap-3">
            {!showCreateForm && (
              <button
                onClick={() => setShowCreateForm(true)}
                className="bg-green-600 hover:bg-green-700 text-white px-4 py-2 rounded-lg flex items-center transition-colors"
                disabled={loading}
              >
                <Plus className="w-4 h-4 mr-2" />
                สร้างตารางใหม่
              </button>
            )}
            
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

        {/* Schema info */}
        {currentSchema && (
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="font-semibold text-blue-900">
                  Schema: {currentSchema.name}
                </h3>
                {currentSchema.description && (
                  <p className="text-blue-700 text-sm mt-1">{currentSchema.description}</p>
                )}
              </div>
              <span className="px-3 py-1 bg-blue-100 text-blue-800 rounded-full text-sm font-medium">
                {currentSchema.tables.length} ตาราง
              </span>
            </div>
          </div>
        )}
      </div>

      {/* Create table form */}
      {showCreateForm && (
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center justify-between mb-6">
            <h3 className="text-lg font-semibold flex items-center">
              <Edit3 className="w-5 h-5 mr-2 text-green-600" />
              สร้างตารางใหม่
            </h3>
            <button
              onClick={resetForm}
              className="text-gray-500 hover:text-gray-700 p-2"
              disabled={loading}
            >
              ✕
            </button>
          </div>

          {/* Table basic info */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                ชื่อตาราง *
              </label>
              <input
                type="text"
                value={tableName}
                onChange={(e) => setTableName(e.target.value)}
                className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-green-500 focus:border-transparent"
                placeholder="เช่น users, products, orders"
                disabled={loading}
              />
            </div>
            
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                คำอธิบายตาราง
              </label>
              <input
                type="text"
                value={tableDescription}
                onChange={(e) => setTableDescription(e.target.value)}
                className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-green-500 focus:border-transparent"
                placeholder="คำอธิบายเกี่ยวกับตารางนี้"
                disabled={loading}
              />
            </div>
          </div>

          {/* Columns section */}
          <div className="mb-6">
            <div className="flex items-center justify-between mb-4">
              <h4 className="text-md font-semibold text-gray-900">
                Columns ({columns.length})
              </h4>
              <button
                onClick={addColumn}
                className="text-green-600 hover:bg-green-50 px-3 py-2 rounded-lg flex items-center text-sm transition-colors"
                disabled={loading}
              >
                <Plus className="w-4 h-4 mr-1" />
                เพิ่ม Column
              </button>
            </div>

            <div className="space-y-4">
              {columns.map((column, index) => (
                <ColumnEditor
                  key={index}
                  column={column}
                  index={index}
                  onUpdate={updateColumn}
                  onDelete={deleteColumn}
                  canDelete={columns.length > 1}
                />
              ))}
            </div>
          </div>

          {/* Action buttons */}
          <div className="flex justify-end gap-3 pt-6 border-t">
            <button
              onClick={resetForm}
              disabled={loading}
              className="px-6 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors disabled:opacity-50"
            >
              ยกเลิก
            </button>
            <button
              onClick={handleCreateTable}
              disabled={loading || !tableName.trim() || columns.length === 0}
              className="px-6 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 transition-colors flex items-center"
            >
              {loading ? (
                <>
                  <RefreshCw className="w-4 h-4 animate-spin mr-2" />
                  กำลังสร้าง...
                </>
              ) : (
                <>
                  <Table2 className="w-4 h-4 mr-2" />
                  สร้างตาราง
                </>
              )}
            </button>
          </div>
        </div>
      )}

      {/* Existing tables */}
      {currentSchema && currentSchema.tables.length > 0 && !showCreateForm && (
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4 flex items-center">
            <Table2 className="w-5 h-5 mr-2 text-gray-600" />
            ตารางที่มีอยู่
          </h3>
          
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {currentSchema.tables.map((table) => (
              <div key={table.name} className="border rounded-lg p-4 hover:shadow-md transition-shadow">
                <div className="flex items-center justify-between">
                  <div className="flex-1 min-w-0">
                    <h4 className="font-medium text-gray-900 truncate">{table.name}</h4>
                    {table.comment && (
                      <p className="text-sm text-gray-600 truncate mt-1">{table.comment}</p>
                    )}
                    <div className="flex items-center gap-2 mt-2">
                      <span className="text-xs text-gray-500">
                        {table.columnCount} columns
                      </span>
                      {table.hasData && (
                        <span className="flex items-center text-xs text-green-600">
                          <div className="w-2 h-2 bg-green-400 rounded-full mr-1" />
                          มีข้อมูล
                        </span>
                      )}
                    </div>
                  </div>
                  
                  <button
                    onClick={() => onTableDelete(table.schema, table.name)}
                    className="text-red-600 hover:bg-red-50 p-2 rounded-lg transition-colors"
                    title="ลบตาราง"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Empty state */}
      {currentSchema && currentSchema.tables.length === 0 && !showCreateForm && (
        <div className="bg-white rounded-lg shadow p-12 text-center">
          <Table2 className="w-16 h-16 text-gray-300 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-gray-900 mb-2">ยังไม่มีตาราง</h3>
          <p className="text-gray-600 mb-6">
            เริ่มต้นสร้างตารางแรกใน schema "{selectedSchema}"
          </p>
          <button
            onClick={() => setShowCreateForm(true)}
            className="bg-green-600 hover:bg-green-700 text-white px-6 py-2 rounded-lg inline-flex items-center"
          >
            <Plus className="w-4 h-4 mr-2" />
            สร้างตารางใหม่
          </button>
        </div>
      )}
    </div>
  );
}