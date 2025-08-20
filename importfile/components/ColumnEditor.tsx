// Enhanced Column Editor Component - เพิ่มใน CompleteSchemaManagementSystem.tsx
import React, { useState, useEffect } from 'react';
import { DatabaseColumn } from '../lib/services/DatabaseService';
import { Edit3, Check, X, AlertCircle, Info } from 'lucide-react';

interface ColumnEditorProps {
  columns: DatabaseColumn[];
  onColumnsChange: (columns: DatabaseColumn[]) => void;
  previewData?: any[];
  disabled?: boolean;
}

interface ColumnValidation {
  isValid: boolean;
  errors: string[];
  warnings: string[];
}

const SUPPORTED_DATA_TYPES = [
  { value: 'SERIAL', label: 'SERIAL (Auto-increment)', category: 'numeric' },
  { value: 'INTEGER', label: 'INTEGER', category: 'numeric' },
  { value: 'BIGINT', label: 'BIGINT', category: 'numeric' },
  { value: 'DECIMAL', label: 'DECIMAL', category: 'numeric' },
  { value: 'NUMERIC', label: 'NUMERIC', category: 'numeric' },
  { value: 'REAL', label: 'REAL', category: 'numeric' },
  { value: 'VARCHAR', label: 'VARCHAR (Text)', category: 'text' },
  { value: 'TEXT', label: 'TEXT (Long text)', category: 'text' },
  { value: 'CHAR', label: 'CHAR (Fixed length)', category: 'text' },
  { value: 'BOOLEAN', label: 'BOOLEAN (True/False)', category: 'boolean' },
  { value: 'DATE', label: 'DATE', category: 'datetime' },
  { value: 'TIMESTAMP', label: 'TIMESTAMP', category: 'datetime' },
  { value: 'TIME', label: 'TIME', category: 'datetime' },
  { value: 'JSON', label: 'JSON', category: 'special' },
  { value: 'JSONB', label: 'JSONB (Binary JSON)', category: 'special' },
  { value: 'UUID', label: 'UUID', category: 'special' }
];

const TYPE_CATEGORIES = {
  numeric: { color: 'bg-blue-100 text-blue-800', icon: '🔢' },
  text: { color: 'bg-green-100 text-green-800', icon: '📝' },
  boolean: { color: 'bg-purple-100 text-purple-800', icon: '✓' },
  datetime: { color: 'bg-orange-100 text-orange-800', icon: '📅' },
  special: { color: 'bg-gray-100 text-gray-800', icon: '⚙️' }
};

export const ColumnEditor: React.FC<ColumnEditorProps> = ({
  columns,
  onColumnsChange,
  previewData = [],
  disabled = false
}) => {
  const [editingColumn, setEditingColumn] = useState<string | null>(null);
  const [tempColumns, setTempColumns] = useState<DatabaseColumn[]>(columns);
  const [validations, setValidations] = useState<Record<string, ColumnValidation>>({});

  // Sync with parent when columns change
  useEffect(() => {
    setTempColumns(columns);
  }, [columns]);

  // Validate columns whenever they change
  useEffect(() => {
    const newValidations: Record<string, ColumnValidation> = {};
    tempColumns.forEach(col => {
      newValidations[col.name] = validateColumn(col, tempColumns);
    });
    setValidations(newValidations);
  }, [tempColumns]);

  const validateColumn = (column: DatabaseColumn, allColumns: DatabaseColumn[]): ColumnValidation => {
    const errors: string[] = [];
    const warnings: string[] = [];

    // Name validation
    if (!column.name.trim()) {
      errors.push('ชื่อ column ไม่สามารถเป็นค่าว่างได้');
    } else if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(column.name)) {
      errors.push('ชื่อ column ต้องขึ้นต้นด้วยตัวอักษร และมีเฉพาะตัวอักษร ตัวเลข และ _');
    }

    // Duplicate name check
    const duplicateCount = allColumns.filter(col => col.name.toLowerCase() === column.name.toLowerCase()).length;
    if (duplicateCount > 1) {
      errors.push('ชื่อ column ซ้ำกับ column อื่น');
    }

    // Type-specific validations
    if (column.type === 'VARCHAR' || column.type === 'CHAR') {
      if (!column.length || column.length <= 0) {
        errors.push(`${column.type} ต้องระบุความยาว`);
      } else if (column.length > 65535) {
        warnings.push('ความยาวมากเกินไป อาจส่งผลต่อประสิทธิภาพ');
      }
    }

    // Primary key validation
    const primaryKeyCount = allColumns.filter(col => col.isPrimary).length;
    if (primaryKeyCount === 0) {
      warnings.push('ควรมี Primary Key อย่างน้อย 1 column');
    } else if (primaryKeyCount > 1 && column.isPrimary) {
      warnings.push('มี Primary Key มากกว่า 1 column (Composite Key)');
    }

    // SERIAL validation
    if (column.type === 'SERIAL' && !column.isPrimary) {
      warnings.push('SERIAL column ควรเป็น Primary Key');
    }

    return {
      isValid: errors.length === 0,
      errors,
      warnings
    };
  };

  const updateColumn = (columnName: string, updates: Partial<DatabaseColumn>) => {
    const newColumns = tempColumns.map(col => 
      col.name === columnName 
        ? { ...col, ...updates }
        : col
    );
    setTempColumns(newColumns);
  };

  const applyChanges = () => {
    onColumnsChange(tempColumns);
    setEditingColumn(null);
  };

  const cancelChanges = () => {
    setTempColumns(columns);
    setEditingColumn(null);
  };

  const addNewColumn = () => {
    const newColumn: DatabaseColumn = {
      name: `new_column_${tempColumns.length + 1}`,
      type: 'VARCHAR',
      length: 255,
      isPrimary: false,
      isRequired: false,
      isUnique: false,
      comment: 'New column'
    };
    setTempColumns([...tempColumns, newColumn]);
    setEditingColumn(newColumn.name);
  };

  const removeColumn = (columnName: string) => {
    if (tempColumns.length <= 1) {
      alert('ต้องมีอย่างน้อย 1 column');
      return;
    }
    const newColumns = tempColumns.filter(col => col.name !== columnName);
    setTempColumns(newColumns);
  };

  const getDataTypeIcon = (type: string) => {
    const dataType = SUPPORTED_DATA_TYPES.find(dt => dt.value === type);
    if (dataType) {
      return TYPE_CATEGORIES[dataType.category as keyof typeof TYPE_CATEGORIES]?.icon || '📄';
    }
    return '📄';
  };

  const getDataTypeColor = (type: string) => {
    const dataType = SUPPORTED_DATA_TYPES.find(dt => dt.value === type);
    if (dataType) {
      return TYPE_CATEGORIES[dataType.category as keyof typeof TYPE_CATEGORIES]?.color || 'bg-gray-100 text-gray-800';
    }
    return 'bg-gray-100 text-gray-800';
  };

  const getSampleData = (columnName: string): any[] => {
    return previewData
      .map(row => row[columnName])
      .filter(val => val != null && val !== '')
      .slice(0, 3);
  };

  const hasChanges = JSON.stringify(tempColumns) !== JSON.stringify(columns);

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-medium text-gray-900 flex items-center">
            <Edit3 className="h-5 w-5 mr-2 text-blue-600" />
            แก้ไขโครงสร้าง Columns
          </h3>
          <p className="text-sm text-gray-600 mt-1">
            ปรับแต่งชื่อ ประเภทข้อมูล และคุณสมบัติของแต่ละ column
          </p>
        </div>
        
        {hasChanges && (
          <div className="flex space-x-2">
            <button
              onClick={cancelChanges}
              disabled={disabled}
              className="flex items-center px-3 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50"
            >
              <X className="h-4 w-4 mr-1" />
              ยกเลิก
            </button>
            <button
              onClick={applyChanges}
              disabled={disabled || Object.values(validations).some(v => !v.isValid)}
              className="flex items-center px-3 py-2 text-sm font-medium text-white bg-blue-600 border border-transparent rounded-md hover:bg-blue-700 disabled:opacity-50"
            >
              <Check className="h-4 w-4 mr-1" />
              บันทึกการเปลี่ยนแปลง
            </button>
          </div>
        )}
      </div>

      {/* Columns List */}
      <div className="space-y-3">
        {tempColumns.map((column, index) => {
          const isEditing = editingColumn === column.name;
          const validation = validations[column.name] || { isValid: true, errors: [], warnings: [] };
          const sampleData = getSampleData(column.name);

          return (
            <div
              key={column.name}
              className={`border rounded-lg p-4 transition-all ${
                isEditing ? 'border-blue-300 bg-blue-50' : 
                !validation.isValid ? 'border-red-300 bg-red-50' : 
                'border-gray-200 bg-white hover:border-gray-300'
              }`}
            >
              <div className="flex items-start justify-between">
                <div className="flex-1">
                  {isEditing ? (
                    // Edit Mode
                    <div className="space-y-4">
                      {/* Column Name */}
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                          ชื่อ Column
                        </label>
                        <input
                          type="text"
                          value={column.name}
                          onChange={(e) => updateColumn(column.name, { name: e.target.value })}
                          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                          placeholder="กรอกชื่อ column"
                        />
                      </div>

                      {/* Data Type */}
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                          ประเภทข้อมูล
                        </label>
                        <select
                          value={column.type}
                          onChange={(e) => {
                            const newType = e.target.value;
                            const updates: Partial<DatabaseColumn> = { type: newType };
                            
                            // Set default length for text types
                            if (newType === 'VARCHAR') {
                              updates.length = column.length || 255;
                            } else if (newType === 'CHAR') {
                              updates.length = column.length || 10;
                            } else if (!['VARCHAR', 'CHAR'].includes(newType)) {
                              updates.length = undefined;
                            }
                            
                            updateColumn(column.name, updates);
                          }}
                          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                        >
                          {Object.entries(
                            SUPPORTED_DATA_TYPES.reduce((acc, type) => {
                              if (!acc[type.category]) acc[type.category] = [];
                              acc[type.category].push(type);
                              return acc;
                            }, {} as Record<string, typeof SUPPORTED_DATA_TYPES>)
                          ).map(([category, types]) => (
                            <optgroup key={category} label={category.toUpperCase()}>
                              {types.map(type => (
                                <option key={type.value} value={type.value}>
                                  {type.label}
                                </option>
                              ))}
                            </optgroup>
                          ))}
                        </select>
                      </div>

                      {/* Length (for VARCHAR/CHAR) */}
                      {(column.type === 'VARCHAR' || column.type === 'CHAR') && (
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-1">
                            ความยาว
                          </label>
                          <input
                            type="number"
                            value={column.length || ''}
                            onChange={(e) => updateColumn(column.name, { length: parseInt(e.target.value) || undefined })}
                            min="1"
                            max="65535"
                            className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                            placeholder="เช่น 255"
                          />
                        </div>
                      )}

                      {/* Column Properties */}
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-2">
                          คุณสมบัติ
                        </label>
                        <div className="space-y-2">
                          <label className="flex items-center">
                            <input
                              type="checkbox"
                              checked={column.isPrimary || false}
                              onChange={(e) => updateColumn(column.name, { 
                                isPrimary: e.target.checked,
                                isRequired: e.target.checked || column.isRequired
                              })}
                              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                            />
                            <span className="ml-2 text-sm text-gray-700">Primary Key</span>
                          </label>
                          
                          <label className="flex items-center">
                            <input
                              type="checkbox"
                              checked={column.isRequired || false}
                              onChange={(e) => updateColumn(column.name, { isRequired: e.target.checked })}
                              disabled={column.isPrimary}
                              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded disabled:opacity-50"
                            />
                            <span className="ml-2 text-sm text-gray-700">Required (NOT NULL)</span>
                          </label>
                          
                          <label className="flex items-center">
                            <input
                              type="checkbox"
                              checked={column.isUnique || false}
                              onChange={(e) => updateColumn(column.name, { isUnique: e.target.checked })}
                              disabled={column.isPrimary}
                              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded disabled:opacity-50"
                            />
                            <span className="ml-2 text-sm text-gray-700">Unique</span>
                          </label>
                        </div>
                      </div>

                      {/* Comment */}
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                          คำอธิบาย
                        </label>
                        <input
                          type="text"
                          value={column.comment || ''}
                          onChange={(e) => updateColumn(column.name, { comment: e.target.value })}
                          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                          placeholder="คำอธิบายเพิ่มเติม"
                        />
                      </div>
                    </div>
                  ) : (
                    // Display Mode
                    <div className="flex items-start space-x-4">
                      {/* Column Info */}
                      <div className="flex-1">
                        <div className="flex items-center space-x-2 mb-2">
                          <span className="text-lg font-medium text-gray-900">{column.name}</span>
                          <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${getDataTypeColor(column.type)}`}>
                            {getDataTypeIcon(column.type)} {column.type}
                            {column.length && `(${column.length})`}
                          </span>
                          {column.isPrimary && (
                            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">
                              🔑 PK
                            </span>
                          )}
                          {column.isRequired && (
                            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-red-100 text-red-800">
                              ✱ Required
                            </span>
                          )}
                          {column.isUnique && (
                            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-purple-100 text-purple-800">
                              ◇ Unique
                            </span>
                          )}
                        </div>
                        
                        {column.comment && (
                          <p className="text-sm text-gray-600 mb-2">{column.comment}</p>
                        )}
                        
                        {/* Sample Data */}
                        {sampleData.length > 0 && (
                          <div className="text-sm text-gray-500">
                            <span className="font-medium">ตัวอย่างข้อมูล: </span>
                            {sampleData.map((val, i) => (
                              <span key={i} className="inline-block bg-gray-100 px-2 py-1 rounded mr-1 mb-1">
                                {String(val).substring(0, 20)}{String(val).length > 20 ? '...' : ''}
                              </span>
                            ))}
                          </div>
                        )}
                      </div>
                    </div>
                  )}

                  {/* Validation Messages */}
                  {(validation.errors.length > 0 || validation.warnings.length > 0) && (
                    <div className="mt-3 space-y-1">
                      {validation.errors.map((error, i) => (
                        <div key={i} className="flex items-center text-sm text-red-600">
                          <AlertCircle className="h-4 w-4 mr-1" />
                          {error}
                        </div>
                      ))}
                      {validation.warnings.map((warning, i) => (
                        <div key={i} className="flex items-center text-sm text-orange-600">
                          <Info className="h-4 w-4 mr-1" />
                          {warning}
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                {/* Actions */}
                <div className="flex items-center space-x-2 ml-4">
                  {!isEditing ? (
                    <>
                      <button
                        onClick={() => setEditingColumn(column.name)}
                        disabled={disabled}
                        className="p-2 text-gray-400 hover:text-gray-600 disabled:opacity-50"
                        title="แก้ไข"
                      >
                        <Edit3 className="h-4 w-4" />
                      </button>
                      {tempColumns.length > 1 && (
                        <button
                          onClick={() => removeColumn(column.name)}
                          disabled={disabled}
                          className="p-2 text-red-400 hover:text-red-600 disabled:opacity-50"
                          title="ลบ"
                        >
                          <X className="h-4 w-4" />
                        </button>
                      )}
                    </>
                  ) : (
                    <button
                      onClick={() => setEditingColumn(null)}
                      className="p-2 text-gray-400 hover:text-gray-600"
                      title="เสร็จสิ้นการแก้ไข"
                    >
                      <Check className="h-4 w-4" />
                    </button>
                  )}
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Add New Column */}
      <button
        onClick={addNewColumn}
        disabled={disabled || editingColumn !== null}
        className="w-full flex items-center justify-center px-4 py-3 border-2 border-dashed border-gray-300 rounded-lg text-sm font-medium text-gray-600 hover:border-gray-400 hover:text-gray-700 disabled:opacity-50 transition-colors"
      >
        <span className="text-lg mr-2">+</span>
        เพิ่ม Column ใหม่
      </button>

      {/* Summary */}
      <div className="bg-gray-50 rounded-lg p-4">
        <h4 className="text-sm font-medium text-gray-900 mb-2">สรุปโครงสร้าง</h4>
        <div className="text-sm text-gray-600 space-y-1">
          <p>• จำนวน Columns: {tempColumns.length}</p>
          <p>• Primary Keys: {tempColumns.filter(col => col.isPrimary).length}</p>
          <p>• Required Columns: {tempColumns.filter(col => col.isRequired).length}</p>
          <p>• Unique Columns: {tempColumns.filter(col => col.isUnique).length}</p>
          {hasChanges && (
            <p className="text-orange-600 font-medium">• มีการเปลี่ยนแปลงที่ยังไม่ได้บันทึก</p>
          )}
        </div>
      </div>
    </div>
  );
};