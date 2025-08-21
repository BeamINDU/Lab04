// FileImport.tsx - Component สำหรับการ import ไฟล์ข้อมูล
import React, { useState, useRef } from 'react';
import { toast } from 'react-hot-toast';
import { Upload, FileText, RefreshCw, Settings, CheckCircle, XCircle, AlertCircle, Download, Eye, Edit3, Trash2, Plus, Key } from 'lucide-react';
import type { FileImportProps, FilePreview, ImportResult, ImportOptions, ApiResponse, DatabaseColumn } from './types';
import { formatFileSize, getFileType, generateTableNameFromFile, showSuccessToast, showErrorToast, showLoadingToast } from './utils';

interface FilePreviewProps {
  preview: FilePreview;
  onTableNameChange: (name: string) => void;
  tableName: string;
  onColumnMappingChange?: (mapping: DatabaseColumn[]) => void;
}

// Component สำหรับแก้ไข column mapping
function ColumnMappingEditor({ 
  suggestedColumns, 
  onMappingChange 
}: { 
  suggestedColumns: DatabaseColumn[];
  onMappingChange: (columns: DatabaseColumn[]) => void;
}) {
  const [columns, setColumns] = useState<DatabaseColumn[]>(() => {
    // เพิ่ม primary key column ถ้ายังไม่มี
    const hasIdColumn = suggestedColumns.some(col => 
      col.name.toLowerCase() === 'id' || 
      col.isPrimary === true
    );
    
    if (!hasIdColumn) {
      // เพิ่ม ID column ที่ต้นของ array
      const idColumn: DatabaseColumn = {
        name: 'id',
        type: 'SERIAL',
        isPrimary: true,
        isRequired: true,
        comment: 'Auto-generated primary key'
      };
      return [idColumn, ...suggestedColumns];
    }
    
    return suggestedColumns;
  });
  const [showEditor, setShowEditor] = useState(false);

  const dataTypes = [
    'SERIAL', 'VARCHAR', 'TEXT', 'INTEGER', 'BIGINT', 'DECIMAL', 'FLOAT', 'BOOLEAN',
    'DATE', 'TIME', 'TIMESTAMP', 'JSON', 'JSONB', 'UUID'
  ];

  const updateColumn = (index: number, field: keyof DatabaseColumn, value: any) => {
    const updatedColumns = [...columns];
    const currentColumn = { ...updatedColumns[index] };
    
    // จัดการ primary key logic
    if (field === 'isPrimary' && value === true) {
      // ถ้าเลือกเป็น primary key ให้ลบ primary key ออกจาก columns อื่น
      updatedColumns.forEach((col, i) => {
        if (i !== index && col.isPrimary) {
          updatedColumns[i] = { ...col, isPrimary: false };
        }
      });
      currentColumn.isRequired = true; // Primary key ต้อง required
    }
    
    // จัดการ SERIAL type logic  
    if (field === 'type' && value === 'SERIAL') {
      currentColumn.isPrimary = true;
      currentColumn.isRequired = true;
      // ลบ primary key ออกจาก columns อื่น
      updatedColumns.forEach((col, i) => {
        if (i !== index && col.isPrimary) {
          updatedColumns[i] = { ...col, isPrimary: false };
        }
      });
    }
    
    // ใช้ type assertion เพื่อแก้ไข TypeScript error
    (currentColumn as any)[field] = value;
    updatedColumns[index] = currentColumn;
    setColumns(updatedColumns);
    onMappingChange(updatedColumns);
  };

  const deleteColumn = (index: number) => {
    const columnToDelete = columns[index];
    
    // ป้องกันการลบ primary key column สุดท้าย
    const primaryKeys = columns.filter(col => col.isPrimary);
    if (primaryKeys.length === 1 && columnToDelete.isPrimary) {
      showErrorToast('ไม่สามารถลบ primary key column สุดท้ายได้');
      return;
    }
    
    if (columns.length <= 1) {
      showErrorToast('ต้องมีอย่างน้อย 1 column');
      return;
    }
    
    const updatedColumns = columns.filter((_, i) => i !== index);
    
    // ถ้าลบ primary key ไปแล้วไม่มี primary key เหลือ ให้เลือก column แรกเป็น primary key
    const hasPrimaryKey = updatedColumns.some(col => col.isPrimary);
    if (!hasPrimaryKey && updatedColumns.length > 0) {
      updatedColumns[0] = { ...updatedColumns[0], isPrimary: true, isRequired: true };
    }
    
    setColumns(updatedColumns);
    onMappingChange(updatedColumns);
    showSuccessToast('ลบ column สำเร็จ');
  };

  const addColumn = () => {
    const newColumn: DatabaseColumn = {
      name: `new_column_${columns.length + 1}`,
      type: 'VARCHAR',
      length: 255,
      isRequired: false,
      comment: ''
    };
    const updatedColumns = [...columns, newColumn];
    setColumns(updatedColumns);
    onMappingChange(updatedColumns);
  };

  // ตรวจสอบว่ามี primary key หรือไม่
  const hasPrimaryKey = columns.some(col => col.isPrimary);

  if (!showEditor) {
    return (
      <div className="mt-4">
        <div className="flex items-center justify-between mb-3">
          <h4 className="text-sm font-medium text-gray-700">
            โครงสร้าง Column ({columns.length})
            {!hasPrimaryKey && (
              <span className="ml-2 text-red-600 text-xs">⚠️ ไม่มี Primary Key</span>
            )}
          </h4>
          <button
            onClick={() => setShowEditor(true)}
            className="text-blue-600 hover:text-blue-800 text-sm flex items-center"
          >
            <Edit3 className="w-4 h-4 mr-1" />
            แก้ไข Column Mapping
          </button>
        </div>
        
        {!hasPrimaryKey && (
          <div className="mb-3 p-3 bg-red-50 border border-red-200 rounded-lg">
            <p className="text-sm text-red-700">
              ⚠️ ไม่มี Primary Key Column - กรุณาคลิก "แก้ไข Column Mapping" เพื่อกำหนด Primary Key
            </p>
          </div>
        )}
        
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
          {columns.map((col, index) => (
            <div key={index} className={`flex items-center text-sm text-gray-600 rounded px-3 py-2 ${
              col.isPrimary ? 'bg-yellow-50 border border-yellow-200' : 'bg-gray-50'
            }`}>
              {col.isPrimary && <Key className="w-3 h-3 mr-1 text-yellow-600" />}
              <span className="font-medium">{col.name}</span>
              <span className="mx-2">:</span>
              <span>{col.type}{col.length ? `(${col.length})` : ''}</span>
              {col.isRequired && <span className="ml-2 text-red-500">*</span>}
            </div>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="mt-4">
      <div className="flex items-center justify-between mb-4">
        <h4 className="text-sm font-medium text-gray-700">
          จัดการ Column Mapping
        </h4>
        <div className="flex gap-2">
          <button
            onClick={addColumn}
            className="px-3 py-1 text-sm text-green-600 hover:text-green-800 border border-green-300 rounded hover:bg-green-50"
          >
            + เพิ่ม Column
          </button>
          <button
            onClick={() => setShowEditor(false)}
            className="px-3 py-1 text-sm text-gray-600 hover:text-gray-800"
          >
            ยกเลิก
          </button>
          <button
            onClick={() => setShowEditor(false)}
            className="px-3 py-1 bg-blue-600 text-white text-sm rounded hover:bg-blue-700"
          >
            บันทึก
          </button>
        </div>
      </div>

      <div className="space-y-3 max-h-80 overflow-y-auto">
        {!hasPrimaryKey && (
          <div className="p-4 bg-yellow-50 border border-yellow-200 rounded-lg">
            <div className="flex items-center">
              <AlertCircle className="w-5 h-5 text-yellow-600 mr-2" />
              <div>
                <h5 className="font-medium text-yellow-800">ต้องการ Primary Key</h5>
                <p className="text-sm text-yellow-700">
                  ตารางจำเป็นต้องมี Primary Key อย่างน้อย 1 column โปรดเลือก column ที่เหมาะสมเป็น Primary Key
                </p>
              </div>
            </div>
          </div>
        )}
        
        {columns.map((column, index) => (
          <div key={index} className={`border rounded-lg p-3 ${
            column.isPrimary ? 'bg-yellow-50 border-yellow-300' : 'bg-white'
          }`}>
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center">
                <span className="text-sm font-medium text-gray-700">
                  Column #{index + 1}
                </span>
                {column.isPrimary && (
                  <span className="ml-2 px-2 py-1 bg-yellow-100 text-yellow-800 text-xs rounded-full flex items-center">
                    <Key className="w-3 h-3 mr-1" />
                    Primary Key
                  </span>
                )}
              </div>
              <button
                onClick={() => deleteColumn(index)}
                disabled={columns.length <= 1 || (column.isPrimary && columns.filter(c => c.isPrimary).length === 1)}
                className="text-red-600 hover:text-red-800 disabled:text-gray-400 disabled:cursor-not-allowed p-1"
                title={
                  columns.length <= 1 
                    ? "ต้องมีอย่างน้อย 1 column" 
                    : (column.isPrimary && columns.filter(c => c.isPrimary).length === 1)
                      ? "ไม่สามารถลบ primary key สุดท้ายได้"
                      : "ลบ column นี้"
                }
              >
                <Trash2 className="w-4 h-4" />
              </button>
            </div>
            
            <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
              {/* Column name */}
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">
                  ชื่อ Column *
                </label>
                <input
                  type="text"
                  value={column.name}
                  onChange={(e) => updateColumn(index, 'name', e.target.value)}
                  className="w-full text-sm border rounded px-2 py-1 focus:ring-1 focus:ring-blue-500 focus:border-transparent"
                />
              </div>

              {/* Data type */}
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">
                  ชนิดข้อมูล *
                </label>
                <select
                  value={column.type}
                  onChange={(e) => updateColumn(index, 'type', e.target.value)}
                  className="w-full text-sm border rounded px-2 py-1 focus:ring-1 focus:ring-blue-500 focus:border-transparent"
                >
                  {dataTypes.map((type) => (
                    <option key={type} value={type}>{type}</option>
                  ))}
                </select>
              </div>

              {/* Length */}
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">
                  ความยาว
                </label>
                <input
                  type="number"
                  value={column.length || ''}
                  onChange={(e) => updateColumn(index, 'length', e.target.value ? parseInt(e.target.value) : undefined)}
                  className="w-full text-sm border rounded px-2 py-1 focus:ring-1 focus:ring-blue-500 focus:border-transparent"
                  placeholder="เช่น 255"
                />
              </div>

              {/* Options */}
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">
                  ตัวเลือก
                </label>
                <div className="space-y-1">
                  <label className="flex items-center text-xs">
                    <input
                      type="checkbox"
                      checked={column.isPrimary || false}
                      onChange={(e) => updateColumn(index, 'isPrimary', e.target.checked)}
                      className="mr-1 rounded"
                    />
                    <Key className="w-3 h-3 mr-1 text-yellow-600" />
                    Primary Key
                  </label>
                  <label className="flex items-center text-xs">
                    <input
                      type="checkbox"
                      checked={column.isRequired || false}
                      onChange={(e) => updateColumn(index, 'isRequired', e.target.checked)}
                      className="mr-1 rounded"
                      disabled={column.isPrimary} // Primary key ต้อง required เสมอ
                    />
                    Required
                  </label>
                  <label className="flex items-center text-xs">
                    <input
                      type="checkbox"
                      checked={column.isUnique || false}
                      onChange={(e) => updateColumn(index, 'isUnique', e.target.checked)}
                      className="mr-1 rounded"
                    />
                    Unique
                  </label>
                </div>
              </div>
            </div>

            {/* Comment */}
            <div className="mt-2">
              <label className="block text-xs font-medium text-gray-700 mb-1">
                คำอธิบาย
              </label>
              <input
                type="text"
                value={column.comment || ''}
                onChange={(e) => updateColumn(index, 'comment', e.target.value || undefined)}
                className="w-full text-sm border rounded px-2 py-1 focus:ring-1 focus:ring-blue-500 focus:border-transparent"
                placeholder="คำอธิบายเกี่ยวกับ column นี้"
              />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// Component สำหรับแสดง preview ข้อมูลไฟล์
function FilePreviewComponent({ preview, onTableNameChange, tableName, onColumnMappingChange }: FilePreviewProps) {
  const [columnMapping, setColumnMapping] = useState<DatabaseColumn[]>(() => {
    // เพิ่ม primary key column ถ้ายังไม่มี
    const suggestedColumns = preview.suggestedColumns || [];
    const hasIdColumn = suggestedColumns.some(col => 
      col.name.toLowerCase() === 'id' || 
      col.isPrimary === true
    );
    
    if (!hasIdColumn) {
      // เพิ่ม ID column ที่ต้นของ array
      const idColumn: DatabaseColumn = {
        name: 'id',
        type: 'SERIAL',
        isPrimary: true,
        isRequired: true,
        comment: 'Auto-generated primary key'
      };
      const updatedColumns = [idColumn, ...suggestedColumns];
      // แจ้ง parent component ทันที
      onColumnMappingChange?.(updatedColumns);
      return updatedColumns;
    }
    
    // แจ้ง parent component ทันที
    onColumnMappingChange?.(suggestedColumns);
    return suggestedColumns;
  });

  const handleColumnMappingChange = (mapping: DatabaseColumn[]) => {
    setColumnMapping(mapping);
    onColumnMappingChange?.(mapping);
  };
  return (
    <div className="bg-white rounded-lg shadow p-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-semibold flex items-center">
          <Eye className="w-5 h-5 mr-2 text-blue-600" />
          Preview ข้อมูล
        </h3>
        <span className="px-3 py-1 bg-blue-100 text-blue-800 rounded-full text-sm">
          {preview.totalRows.toLocaleString()} แถว
        </span>
      </div>

      {/* File info */}
      <div className="bg-gray-50 rounded-lg p-4 mb-4">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
          <div>
            <span className="font-medium text-gray-700">ชื่อไฟล์:</span>
            <span className="ml-2 text-gray-600">{preview.fileName}</span>
          </div>
          <div>
            <span className="font-medium text-gray-700">ประเภท:</span>
            <span className="ml-2 text-gray-600">{preview.fileType}</span>
          </div>
          <div>
            <span className="font-medium text-gray-700">จำนวนแถว:</span>
            <span className="ml-2 text-gray-600">{preview.totalRows.toLocaleString()}</span>
          </div>
        </div>
      </div>

      {/* Table name input */}
      <div className="mb-4">
        <label className="block text-sm font-medium text-gray-700 mb-2">
          ชื่อตารางปลายทาง *
        </label>
        <input
          type="text"
          value={tableName}
          onChange={(e) => onTableNameChange(e.target.value)}
          className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          placeholder="ระบุชื่อตารางที่ต้องการสร้างหรือ import เข้า"
        />
        <p className="text-xs text-gray-500 mt-1">
          ถ้าตารางยังไม่มี ระบบจะสร้างตารางใหม่อัตโนมัติ
        </p>
      </div>

      {/* Data preview table */}
      <div className="border rounded-lg overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                {preview.headers.map((header, index) => (
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
              {preview.sampleData.slice(0, 5).map((row, rowIndex) => (
                <tr key={rowIndex} className={rowIndex % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                  {preview.headers.map((header, colIndex) => (
                    <td
                      key={colIndex}
                      className="px-6 py-4 whitespace-nowrap text-sm text-gray-900"
                    >
                      {row[header] !== null && row[header] !== undefined ? 
                        String(row[header]).substring(0, 50) + 
                        (String(row[header]).length > 50 ? '...' : '') 
                        : '-'
                      }
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {preview.sampleData.length > 5 && (
          <div className="bg-gray-50 px-6 py-3 text-sm text-gray-500 text-center">
            แสดง 5 แถวแรกจากทั้งหมด {preview.totalRows.toLocaleString()} แถว
          </div>
        )}
      </div>

      {/* Suggested columns */}
      {preview.suggestedColumns && preview.suggestedColumns.length > 0 && (
        <ColumnMappingEditor
          suggestedColumns={preview.suggestedColumns}
          onMappingChange={handleColumnMappingChange}
        />
      )}
    </div>
  );
}

interface ImportOptionsProps {
  options: ImportOptions;
  onOptionsChange: (options: ImportOptions) => void;
  schemas: string[];
  selectedSchema: string;
  onSchemaChange: (schema: string) => void;
}

// Component สำหรับตั้งค่า import options
function ImportOptionsComponent({ 
  options, 
  onOptionsChange, 
  schemas, 
  selectedSchema, 
  onSchemaChange 
}: ImportOptionsProps) {
  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h3 className="text-lg font-semibold mb-4 flex items-center">
        <Settings className="w-5 h-5 mr-2 text-gray-600" />
        ตั้งค่าการ Import
      </h3>

      <div className="space-y-4">
        {/* Schema selection */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            เลือก Schema ปลายทาง
          </label>
          <select
            value={selectedSchema}
            onChange={(e) => onSchemaChange(e.target.value)}
            className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          >
            {schemas.map((schema) => (
              <option key={schema} value={schema}>
                {schema}
              </option>
            ))}
          </select>
        </div>

        {/* Import options */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={options.createTable}
                onChange={(e) => onOptionsChange({...options, createTable: e.target.checked})}
                className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
              />
              <span className="ml-2 text-sm text-gray-700">สร้างตารางใหม่อัตโนมัติ</span>
            </label>
            <p className="text-xs text-gray-500 ml-6">
              ถ้าตารางยังไม่มี ระบบจะสร้างตารางใหม่ตาม structure ของไฟล์
            </p>
          </div>

          <div>
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={options.truncateBeforeImport}
                onChange={(e) => onOptionsChange({...options, truncateBeforeImport: e.target.checked})}
                className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
              />
              <span className="ml-2 text-sm text-gray-700">ลบข้อมูลเดิมก่อน Import</span>
            </label>
            <p className="text-xs text-gray-500 ml-6">
              ลบข้อมูลทั้งหมดในตารางก่อนทำการ import ข้อมูลใหม่
            </p>
          </div>

          <div>
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={options.skipErrors}
                onChange={(e) => onOptionsChange({...options, skipErrors: e.target.checked})}
                className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
              />
              <span className="ml-2 text-sm text-gray-700">ข้ามข้อผิดพลาด</span>
            </label>
            <p className="text-xs text-gray-500 ml-6">
              หากมี error ในบางแถว จะข้ามไปและ import แถวถัดไป
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Batch Size
            </label>
            <input
              type="number"
              value={options.batchSize}
              onChange={(e) => onOptionsChange({...options, batchSize: parseInt(e.target.value) || 1000})}
              className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              min="100"
              max="10000"
              step="100"
            />
            <p className="text-xs text-gray-500 mt-1">
              จำนวนแถวที่ประมวลผลในแต่ละครั้ง (100-10,000)
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

interface ImportResultProps {
  result: ImportResult;
  onClose: () => void;
}

// Component สำหรับแสดงผลลัพธ์การ import
function ImportResultComponent({ result, onClose }: ImportResultProps) {
  const successRate = result.totalRows > 0 ? (result.successRows / result.totalRows) * 100 : 0;
  
  return (
    <div className="bg-white rounded-lg shadow p-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-semibold flex items-center">
          {result.success ? (
            <CheckCircle className="w-5 h-5 mr-2 text-green-600" />
          ) : (
            <XCircle className="w-5 h-5 mr-2 text-red-600" />
          )}
          ผลลัพธ์การ Import
        </h3>
        <button
          onClick={onClose}
          className="text-gray-500 hover:text-gray-700 p-2"
        >
          ✕
        </button>
      </div>

      {/* Summary stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <div className="bg-blue-50 rounded-lg p-4 text-center">
          <div className="text-2xl font-bold text-blue-600">{result.totalRows.toLocaleString()}</div>
          <div className="text-sm text-blue-700">แถวทั้งหมด</div>
        </div>
        
        <div className="bg-green-50 rounded-lg p-4 text-center">
          <div className="text-2xl font-bold text-green-600">{result.successRows.toLocaleString()}</div>
          <div className="text-sm text-green-700">สำเร็จ</div>
        </div>
        
        <div className="bg-red-50 rounded-lg p-4 text-center">
          <div className="text-2xl font-bold text-red-600">{result.errorRows.toLocaleString()}</div>
          <div className="text-sm text-red-700">ผิดพลาด</div>
        </div>
        
        <div className="bg-gray-50 rounded-lg p-4 text-center">
          <div className="text-2xl font-bold text-gray-600">{result.executionTime.toFixed(2)}s</div>
          <div className="text-sm text-gray-700">เวลาที่ใช้</div>
        </div>
      </div>

      {/* Progress bar */}
      <div className="mb-6">
        <div className="flex justify-between text-sm text-gray-600 mb-2">
          <span>อัตราความสำเร็จ</span>
          <span>{successRate.toFixed(1)}%</span>
        </div>
        <div className="w-full bg-gray-200 rounded-full h-2">
          <div 
            className="bg-green-600 h-2 rounded-full transition-all duration-300"
            style={{ width: `${successRate}%` }}
          />
        </div>
      </div>

      {/* Errors list */}
      {result.errors && result.errors.length > 0 && (
        <div>
          <h4 className="text-md font-semibold text-red-700 mb-3 flex items-center">
            <AlertCircle className="w-4 h-4 mr-2" />
            ข้อผิดพลาด ({result.errors.length})
          </h4>
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 max-h-60 overflow-y-auto">
            <div className="space-y-2">
              {result.errors.slice(0, 10).map((error, index) => (
                <div key={index} className="text-sm">
                  <span className="font-medium text-red-700">แถวที่ {error.row}:</span>
                  <span className="text-red-600 ml-2">{error.error}</span>
                </div>
              ))}
              {result.errors.length > 10 && (
                <div className="text-sm text-red-600 font-medium">
                  ... และอีก {result.errors.length - 10} ข้อผิดพลาด
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// Main FileImport component
export default function FileImport({
  schemas,
  selectedSchema,
  loading,
  onSchemaSelect,
  onLoadingChange,
  onRefresh,
  onImportComplete
}: FileImportProps) {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [filePreview, setFilePreview] = useState<FilePreview | null>(null);
  const [importResult, setImportResult] = useState<ImportResult | null>(null);
  const [tableName, setTableName] = useState('');
  const [isDragActive, setIsDragActive] = useState(false);
  const [columnMapping, setColumnMapping] = useState<DatabaseColumn[]>([]);
  const [importOptions, setImportOptions] = useState<ImportOptions>({
    createTable: true,
    truncateBeforeImport: false,
    skipErrors: true,
    batchSize: 1000
  });
  
  const fileInputRef = useRef<HTMLInputElement>(null);

  // ฟังก์ชันสำหรับการเลือกไฟล์
  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      processFile(file);
    }
  };

  // ฟังก์ชันสำหรับ drag and drop
  const handleFileDrop = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    setIsDragActive(false);
    const file = event.dataTransfer.files[0];
    if (file) {
      processFile(file);
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

  // ฟังก์ชันสำหรับประมวลผลไฟล์
  const processFile = async (file: File) => {
    setSelectedFile(file);
    setFilePreview(null);
    setImportResult(null);
    
    // สร้างชื่อตารางจากชื่อไฟล์
    const suggestedTableName = generateTableNameFromFile(file.name);
    setTableName(suggestedTableName);

    // Preview ไฟล์
    const loadingToast = showLoadingToast('กำลัง preview ไฟล์...');
    
    try {
      const formData = new FormData();
      formData.append('file', file);

      const response = await fetch('/api/services/preview', {
        method: 'POST',
        body: formData
      });

      const result: ApiResponse<FilePreview> = await response.json();
      
      if (result.success && result.data) {
        setFilePreview(result.data);
        // ไม่ต้อง set columnMapping ที่นี่ เพราะจะถูก set ใน FilePreviewComponent แล้ว
        // setColumnMapping(result.data.suggestedColumns || []);
        showSuccessToast('Preview ไฟล์สำเร็จ');
      } else {
        showErrorToast(result.error || 'ไม่สามารถ preview ไฟล์ได้');
      }
    } catch (error) {
      showErrorToast('เกิดข้อผิดพลาดในการ preview ไฟล์');
      console.error('Preview error:', error);
    } finally {
      toast.dismiss(loadingToast);
    }
  };

  // ฟังก์ชันสำหรับ import ข้อมูล
  const handleImport = async () => {
    if (!selectedFile || !filePreview) {
      showErrorToast('กรุณาเลือกไฟล์');
      return;
    }

    if (!tableName.trim()) {
      showErrorToast('กรุณาระบุชื่อตาราง');
      return;
    }

    onLoadingChange(true);
    const loadingToast = showLoadingToast('กำลัง import ข้อมูล...');

    try {
      const formData = new FormData();
      formData.append('file', selectedFile);
      formData.append('schema', selectedSchema);
      formData.append('tableName', tableName);
      formData.append('createTable', importOptions.createTable.toString());
      formData.append('truncateBeforeImport', importOptions.truncateBeforeImport.toString());
      formData.append('skipErrors', importOptions.skipErrors.toString());
      formData.append('batchSize', importOptions.batchSize.toString());
      
      // ส่ง column mapping ไปด้วยถ้ามี
      if (columnMapping.length > 0) {
        formData.append('columnMapping', JSON.stringify(columnMapping));
      }

      const response = await fetch('/api/services/import', {
        method: 'POST',
        body: formData
      });

      const result: ApiResponse<ImportResult> = await response.json();
      
      if (result.success && result.data) {
        setImportResult(result.data);
        onImportComplete(result.data);
        showSuccessToast(`Import สำเร็จ: ${result.data.successRows}/${result.data.totalRows} แถว`);
        onRefresh(); // Refresh schema data
      } else {
        showErrorToast(result.error || 'Import ไม่สำเร็จ');
      }
    } catch (error) {
      showErrorToast('เกิดข้อผิดพลาดในการ import');
      console.error('Import error:', error);
    } finally {
      toast.dismiss(loadingToast);
      onLoadingChange(false);
    }
  };

  // Reset ทุกอย่าง
  const resetAll = () => {
    setSelectedFile(null);
    setFilePreview(null);
    setImportResult(null);
    setTableName('');
    setColumnMapping([]);
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const schemaNames = schemas.map(s => s.name);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-2xl font-bold text-gray-900 flex items-center">
              <Upload className="w-8 h-8 mr-3 text-purple-600" />
              Import ข้อมูลจากไฟล์
            </h2>
            <p className="text-gray-600 mt-1">
              รองรับไฟล์ CSV, Excel (XLSX, XLS) และ JSON
            </p>
          </div>
          
          {selectedFile && (
            <button
              onClick={resetAll}
              className="text-gray-500 hover:text-gray-700 px-4 py-2 rounded-lg border border-gray-300 hover:bg-gray-50 transition-colors"
            >
              เริ่มใหม่
            </button>
          )}
        </div>
      </div>

      {/* File upload area */}
      {!selectedFile && (
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4">เลือกไฟล์สำหรับ Import</h3>
          
          <div
            onDrop={handleFileDrop}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors ${
              isDragActive 
                ? 'border-purple-400 bg-purple-50' 
                : 'border-gray-300 hover:border-purple-400 hover:bg-gray-50'
            }`}
          >
            <Upload className="w-12 h-12 text-gray-400 mx-auto mb-4" />
            <p className="text-lg font-medium text-gray-700 mb-2">
              ลากไฟล์มาวางที่นี่ หรือคลิกเพื่อเลือกไฟล์
            </p>
            <p className="text-gray-600 mb-4">
              รองรับ CSV, XLSX, XLS, JSON (ขนาดไม่เกิน 50MB)
            </p>
            
            <input
              ref={fileInputRef}
              type="file"
              onChange={handleFileSelect}
              accept=".csv,.xlsx,.xls,.json"
              className="hidden"
            />
            
            <button
              onClick={() => fileInputRef.current?.click()}
              className="bg-purple-600 hover:bg-purple-700 text-white px-6 py-2 rounded-lg inline-flex items-center transition-colors"
            >
              <FileText className="w-4 h-4 mr-2" />
              เลือกไฟล์
            </button>
          </div>
        </div>
      )}

      {/* File info */}
      {selectedFile && (
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center">
              <FileText className="w-8 h-8 text-purple-600 mr-3" />
              <div>
                <h3 className="font-semibold text-gray-900">{selectedFile.name}</h3>
                <p className="text-gray-600 text-sm">
                  {getFileType(selectedFile.name)} • {formatFileSize(selectedFile.size)}
                </p>
              </div>
            </div>
            <button
              onClick={resetAll}
              className="text-red-600 hover:bg-red-50 p-2 rounded-lg transition-colors"
            >
              <XCircle className="w-5 h-5" />
            </button>
          </div>
        </div>
      )}

      {/* Import options */}
      {selectedFile && !importResult && (
        <ImportOptionsComponent
          options={importOptions}
          onOptionsChange={setImportOptions}
          schemas={schemaNames}
          selectedSchema={selectedSchema}
          onSchemaChange={onSchemaSelect}
        />
      )}

      {/* File preview */}
      {filePreview && !importResult && (
        <FilePreviewComponent
          preview={filePreview}
          tableName={tableName}
          onTableNameChange={setTableName}
          onColumnMappingChange={setColumnMapping}
        />
      )}

      {/* Import button */}
      {filePreview && !importResult && (
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex justify-end">
            <button
              onClick={handleImport}
              disabled={loading || !tableName.trim()}
              className="bg-purple-600 hover:bg-purple-700 text-white px-8 py-3 rounded-lg flex items-center disabled:opacity-50 transition-colors"
            >
              {loading ? (
                <>
                  <RefreshCw className="w-5 h-5 animate-spin mr-2" />
                  กำลัง Import...
                </>
              ) : (
                <>
                  <Upload className="w-5 h-5 mr-2" />
                  เริ่ม Import ข้อมูล
                </>
              )}
            </button>
          </div>
        </div>
      )}

      {/* Import result */}
      {importResult && (
        <ImportResultComponent
          result={importResult}
          onClose={() => setImportResult(null)}
        />
      )}
    </div>
  );
}