// ImportHistory.tsx - Component สำหรับแสดงประวัติการ import
import React, { useState, useEffect } from 'react';
import { History, Search, Filter, Download, Eye, CheckCircle, XCircle, AlertTriangle, Calendar, Database, Table2, FileText } from 'lucide-react';
import { apiCall, showErrorToast, formatFileSize } from './utils';
import type { BaseComponentProps, ApiResponse } from './types';

// Types สำหรับ import history
interface ImportHistoryRecord {
  id: string;
  fileName: string;
  fileSize: number;
  fileType: string;
  schema: string;
  tableName: string;
  totalRows: number;
  successRows: number;
  errorRows: number;
  executionTime: number;
  status: 'SUCCESS' | 'PARTIAL' | 'FAILED';
  createdAt: string;
  createdBy: string;
  errors?: Array<{ row: number; error: string }>;
}

interface ImportHistoryProps extends BaseComponentProps {
  // Additional props if needed
}

// Status badge component
function StatusBadge({ status }: { status: string }) {
  const config = {
    SUCCESS: { 
      color: 'bg-green-100 text-green-800 border-green-200',
      icon: CheckCircle,
      text: 'สำเร็จ'
    },
    PARTIAL: { 
      color: 'bg-yellow-100 text-yellow-800 border-yellow-200',
      icon: AlertTriangle,
      text: 'บางส่วน'
    },
    FAILED: { 
      color: 'bg-red-100 text-red-800 border-red-200',
      icon: XCircle,
      text: 'ไม่สำเร็จ'
    }
  };
  
  const { color, icon: Icon, text } = config[status as keyof typeof config] || config.FAILED;
  
  return (
    <span className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-medium border ${color}`}>
      <Icon className="w-4 h-4 mr-1" />
      {text}
    </span>
  );
}

// Detail modal component
function ImportDetailModal({ 
  record, 
  isOpen, 
  onClose 
}: { 
  record: ImportHistoryRecord | null;
  isOpen: boolean;
  onClose: () => void;
}) {
  if (!isOpen || !record) return null;

  const successRate = record.totalRows > 0 ? (record.successRows / record.totalRows) * 100 : 0;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl p-6 w-full max-w-4xl mx-4 max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between mb-6">
          <h3 className="text-xl font-semibold flex items-center">
            <Eye className="w-6 h-6 mr-2 text-blue-600" />
            รายละเอียดการ Import
          </h3>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-gray-700 p-2"
          >
            ✕
          </button>
        </div>

        {/* Basic info */}
        <div className="bg-gray-50 rounded-lg p-6 mb-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h4 className="font-semibold text-gray-900 mb-3">ข้อมูลไฟล์</h4>
              <div className="space-y-2 text-sm">
                <div className="flex">
                  <span className="font-medium text-gray-700 w-20">ชื่อไฟล์:</span>
                  <span className="text-gray-600">{record.fileName}</span>
                </div>
                <div className="flex">
                  <span className="font-medium text-gray-700 w-20">ขนาด:</span>
                  <span className="text-gray-600">{formatFileSize(record.fileSize)}</span>
                </div>
                <div className="flex">
                  <span className="font-medium text-gray-700 w-20">ประเภท:</span>
                  <span className="text-gray-600">{record.fileType}</span>
                </div>
              </div>
            </div>
            
            <div>
              <h4 className="font-semibold text-gray-900 mb-3">ปลายทาง</h4>
              <div className="space-y-2 text-sm">
                <div className="flex">
                  <span className="font-medium text-gray-700 w-20">Schema:</span>
                  <span className="text-gray-600">{record.schema}</span>
                </div>
                <div className="flex">
                  <span className="font-medium text-gray-700 w-20">ตาราง:</span>
                  <span className="text-gray-600">{record.tableName}</span>
                </div>
                <div className="flex">
                  <span className="font-medium text-gray-700 w-20">สถานะ:</span>
                  <StatusBadge status={record.status} />
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Statistics */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
          <div className="bg-blue-50 rounded-lg p-4 text-center">
            <div className="text-2xl font-bold text-blue-600">{record.totalRows.toLocaleString()}</div>
            <div className="text-sm text-blue-700">แถวทั้งหมด</div>
          </div>
          
          <div className="bg-green-50 rounded-lg p-4 text-center">
            <div className="text-2xl font-bold text-green-600">{record.successRows.toLocaleString()}</div>
            <div className="text-sm text-green-700">สำเร็จ</div>
          </div>
          
          <div className="bg-red-50 rounded-lg p-4 text-center">
            <div className="text-2xl font-bold text-red-600">{record.errorRows.toLocaleString()}</div>
            <div className="text-sm text-red-700">ผิดพลาด</div>
          </div>
          
          <div className="bg-gray-50 rounded-lg p-4 text-center">
            <div className="text-2xl font-bold text-gray-600">{record.executionTime.toFixed(2)}s</div>
            <div className="text-sm text-gray-700">เวลาที่ใช้</div>
          </div>
        </div>

        {/* Progress bar */}
        <div className="mb-6">
          <div className="flex justify-between text-sm text-gray-600 mb-2">
            <span>อัตราความสำเร็จ</span>
            <span>{successRate.toFixed(1)}%</span>
          </div>
          <div className="w-full bg-gray-200 rounded-full h-3">
            <div 
              className="bg-green-600 h-3 rounded-full transition-all duration-300"
              style={{ width: `${successRate}%` }}
            />
          </div>
        </div>

        {/* Errors */}
        {record.errors && record.errors.length > 0 && (
          <div>
            <h4 className="font-semibold text-red-700 mb-3 flex items-center">
              <AlertTriangle className="w-5 h-5 mr-2" />
              ข้อผิดพลาด ({record.errors.length})
            </h4>
            <div className="bg-red-50 border border-red-200 rounded-lg p-4 max-h-60 overflow-y-auto">
              <div className="space-y-2">
                {record.errors.map((error, index) => (
                  <div key={index} className="text-sm">
                    <span className="font-medium text-red-700">แถวที่ {error.row}:</span>
                    <span className="text-red-600 ml-2">{error.error}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* Meta info */}
        <div className="mt-6 pt-6 border-t border-gray-200">
          <div className="flex justify-between items-center text-sm text-gray-600">
            <span>Import โดย: {record.createdBy}</span>
            <span>วันที่: {new Date(record.createdAt).toLocaleString('th-TH')}</span>
          </div>
        </div>
      </div>
    </div>
  );
}

// Main ImportHistory component
export default function ImportHistory({ loading, onLoadingChange }: ImportHistoryProps) {
  const [records, setRecords] = useState<ImportHistoryRecord[]>([]);
  const [filteredRecords, setFilteredRecords] = useState<ImportHistoryRecord[]>([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState<string>('ALL');
  const [dateFilter, setDateFilter] = useState<string>('ALL');
  const [selectedRecord, setSelectedRecord] = useState<ImportHistoryRecord | null>(null);
  const [showDetailModal, setShowDetailModal] = useState(false);

  // Load import history
  const loadHistory = async () => {
    onLoadingChange(true);
    try {
      const result: ApiResponse<ImportHistoryRecord[]> = await apiCall('/api/services/import-history');
      
      if (result.success && result.data) {
        setRecords(result.data);
        setFilteredRecords(result.data);
      }
    } catch (error) {
      showErrorToast('ไม่สามารถโหลดประวัติการ import ได้');
      console.error('Load history error:', error);
    } finally {
      onLoadingChange(false);
    }
  };

  // Load data on mount
  useEffect(() => {
    loadHistory();
  }, []);

  // Filter records based on search and filters
  useEffect(() => {
    let filtered = [...records];

    // Search filter
    if (searchTerm) {
      filtered = filtered.filter(record => 
        record.fileName.toLowerCase().includes(searchTerm.toLowerCase()) ||
        record.schema.toLowerCase().includes(searchTerm.toLowerCase()) ||
        record.tableName.toLowerCase().includes(searchTerm.toLowerCase()) ||
        record.createdBy.toLowerCase().includes(searchTerm.toLowerCase())
      );
    }

    // Status filter
    if (statusFilter !== 'ALL') {
      filtered = filtered.filter(record => record.status === statusFilter);
    }

    // Date filter
    if (dateFilter !== 'ALL') {
      const now = new Date();
      const filterDate = new Date();
      
      switch (dateFilter) {
        case 'TODAY':
          filterDate.setHours(0, 0, 0, 0);
          filtered = filtered.filter(record => new Date(record.createdAt) >= filterDate);
          break;
        case 'WEEK':
          filterDate.setDate(now.getDate() - 7);
          filtered = filtered.filter(record => new Date(record.createdAt) >= filterDate);
          break;
        case 'MONTH':
          filterDate.setMonth(now.getMonth() - 1);
          filtered = filtered.filter(record => new Date(record.createdAt) >= filterDate);
          break;
      }
    }

    setFilteredRecords(filtered);
  }, [records, searchTerm, statusFilter, dateFilter]);

  const showDetails = (record: ImportHistoryRecord) => {
    setSelectedRecord(record);
    setShowDetailModal(true);
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-2xl font-bold text-gray-900 flex items-center">
              <History className="w-8 h-8 mr-3 text-indigo-600" />
              ประวัติการ Import
            </h2>
            <p className="text-gray-600 mt-1">
              ติดตามและจัดการประวัติการ import ข้อมูลทั้งหมด
            </p>
          </div>
          
          <div className="text-sm text-gray-600">
            ทั้งหมด {filteredRecords.length} รายการ
            {filteredRecords.length !== records.length && ` (จาก ${records.length})`}
          </div>
        </div>

        {/* Filters */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          {/* Search */}
          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
            <input
              type="text"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
              placeholder="ค้นหาไฟล์, schema, ตาราง..."
            />
          </div>

          {/* Status filter */}
          <div>
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
              className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
            >
              <option value="ALL">สถานะทั้งหมด</option>
              <option value="SUCCESS">สำเร็จ</option>
              <option value="PARTIAL">บางส่วน</option>
              <option value="FAILED">ไม่สำเร็จ</option>
            </select>
          </div>

          {/* Date filter */}
          <div>
            <select
              value={dateFilter}
              onChange={(e) => setDateFilter(e.target.value)}
              className="w-full border rounded-lg px-3 py-2 focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
            >
              <option value="ALL">ช่วงเวลาทั้งหมด</option>
              <option value="TODAY">วันนี้</option>
              <option value="WEEK">7 วันที่แล้ว</option>
              <option value="MONTH">30 วันที่แล้ว</option>
            </select>
          </div>

          {/* Actions */}
          <div className="flex gap-2">
            <button
              onClick={loadHistory}
              disabled={loading}
              className="flex-1 border border-gray-300 hover:bg-gray-50 px-3 py-2 rounded-lg flex items-center justify-center transition-colors disabled:opacity-50"
            >
              <History className={`w-4 h-4 mr-1 ${loading ? 'animate-spin' : ''}`} />
              รีเฟรช
            </button>
          </div>
        </div>
      </div>

      {/* Records table */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        {filteredRecords.length === 0 ? (
          <div className="p-12 text-center">
            <History className="w-16 h-16 text-gray-300 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-gray-900 mb-2">
              {searchTerm || statusFilter !== 'ALL' || dateFilter !== 'ALL' 
                ? 'ไม่พบประวัติที่ตรงกับเงื่อนไข' 
                : 'ยังไม่มีประวัติการ Import'
              }
            </h3>
            <p className="text-gray-600">
              {searchTerm || statusFilter !== 'ALL' || dateFilter !== 'ALL'
                ? 'ลองปรับเปลี่ยนเงื่อนไขการค้นหา'
                : 'เริ่มต้น import ข้อมูลเพื่อดูประวัติที่นี่'
              }
            </p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    ไฟล์
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    ปลายทาง
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    ผลลัพธ์
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    สถานะ
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    วันที่
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    การกระทำ
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {filteredRecords.map((record) => {
                  const successRate = record.totalRows > 0 ? (record.successRows / record.totalRows) * 100 : 0;
                  
                  return (
                    <tr key={record.id} className="hover:bg-gray-50">
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="flex items-center">
                          <FileText className="w-5 h-5 text-gray-400 mr-3" />
                          <div>
                            <div className="text-sm font-medium text-gray-900">
                              {record.fileName}
                            </div>
                            <div className="text-sm text-gray-500">
                              {record.fileType} • {formatFileSize(record.fileSize)}
                            </div>
                          </div>
                        </div>
                      </td>
                      
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="flex items-center">
                          <Database className="w-4 h-4 text-blue-500 mr-2" />
                          <div>
                            <div className="text-sm font-medium text-gray-900">
                              {record.schema}.{record.tableName}
                            </div>
                            <div className="text-sm text-gray-500">
                              โดย {record.createdBy}
                            </div>
                          </div>
                        </div>
                      </td>
                      
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="text-sm text-gray-900">
                          {record.successRows.toLocaleString()} / {record.totalRows.toLocaleString()}
                        </div>
                        <div className="w-16 bg-gray-200 rounded-full h-1.5 mt-1">
                          <div 
                            className="bg-green-600 h-1.5 rounded-full"
                            style={{ width: `${successRate}%` }}
                          />
                        </div>
                        <div className="text-xs text-gray-500 mt-1">
                          {successRate.toFixed(1)}%
                        </div>
                      </td>
                      
                      <td className="px-6 py-4 whitespace-nowrap">
                        <StatusBadge status={record.status} />
                      </td>
                      
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="flex items-center text-sm text-gray-500">
                          <Calendar className="w-4 h-4 mr-1" />
                          {new Date(record.createdAt).toLocaleDateString('th-TH')}
                        </div>
                        <div className="text-xs text-gray-400">
                          {new Date(record.createdAt).toLocaleTimeString('th-TH')}
                        </div>
                      </td>
                      
                      <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                        <button
                          onClick={() => showDetails(record)}
                          className="text-indigo-600 hover:text-indigo-900 flex items-center"
                        >
                          <Eye className="w-4 h-4 mr-1" />
                          ดูรายละเอียด
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Detail modal */}
      <ImportDetailModal
        record={selectedRecord}
        isOpen={showDetailModal}
        onClose={() => {
          setShowDetailModal(false);
          setSelectedRecord(null);
        }}
      />
    </div>
  );
}