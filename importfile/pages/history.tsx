import React, { useState, useEffect } from 'react';
import { useSession } from 'next-auth/react';
import AuthGuard from '../components/AuthGuard';
import Layout from '../components/Layout';
import { History, FileText, CheckCircle, XCircle, Clock, AlertTriangle } from 'lucide-react';
import type { ImportLogItem, PaginationInfo } from '../types/import';

export default function HistoryPage() {
  const { data: session } = useSession();
  const [importHistory, setImportHistory] = useState<ImportLogItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [pagination, setPagination] = useState<PaginationInfo>({
    page: 1,
    limit: 10,
    total: 0,
    pages: 0
  });

  useEffect(() => {
    fetchImportHistory();
  }, [pagination.page]);

  const fetchImportHistory = async () => {
    try {
      setLoading(true);
      const response = await fetch(`/api/import/history?page=${pagination.page}&limit=${pagination.limit}`);
      const data = await response.json();
      
      if (data.success) {
        setImportHistory(data.data || []);
        setPagination(data.pagination || pagination);
      } else {
        console.error('Failed to fetch history:', data.error);
        // Fallback mock data for development
        const mockData: ImportLogItem[] = [
          {
            id: '1',
            fileName: 'employees_data.csv',
            fileType: 'csv',
            tableName: 'employees',
            status: 'COMPLETED',
            totalRows: 150,
            successRows: 150,
            errorRows: 0,
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
            user: { 
              id: '1',
              name: 'Admin User' 
            }
          },
          {
            id: '2',
            fileName: 'products_data.xlsx',
            fileType: 'excel',
            tableName: 'products',
            status: 'PARTIAL',
            totalRows: 200,
            successRows: 180,
            errorRows: 20,
            createdAt: new Date(Date.now() - 86400000).toISOString(),
            updatedAt: new Date(Date.now() - 86400000).toISOString(),
            user: { 
              id: '2',
              name: 'Manager User' 
            }
          },
          {
            id: '3',
            fileName: 'customers_data.json',
            fileType: 'json',
            tableName: 'customers',
            status: 'FAILED',
            totalRows: 50,
            successRows: 0,
            errorRows: 50,
            createdAt: new Date(Date.now() - 172800000).toISOString(),
            updatedAt: new Date(Date.now() - 172800000).toISOString(),
            user: { 
              id: '3',
              name: 'Staff User' 
            }
          }
        ];
        
        setImportHistory(mockData);
        setPagination({
          page: 1,
          limit: 10,
          total: 3,
          pages: 1
        });
      }
    } catch (error) {
      console.error('Error fetching import history:', error);
      setImportHistory([]);
    } finally {
      setLoading(false);
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'COMPLETED':
        return <CheckCircle className="h-5 w-5 text-green-500" />;
      case 'PARTIAL':
        return <AlertTriangle className="h-5 w-5 text-yellow-500" />;
      case 'FAILED':
        return <XCircle className="h-5 w-5 text-red-500" />;
      case 'PROCESSING':
        return <Clock className="h-5 w-5 text-blue-500" />;
      default:
        return <FileText className="h-5 w-5 text-gray-500" />;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'COMPLETED':
        return 'bg-green-100 text-green-800';
      case 'PARTIAL':
        return 'bg-yellow-100 text-yellow-800';
      case 'FAILED':
        return 'bg-red-100 text-red-800';
      case 'PROCESSING':
        return 'bg-blue-100 text-blue-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  const getStatusText = (status: string) => {
    switch (status) {
      case 'COMPLETED':
        return 'สำเร็จ';
      case 'PARTIAL':
        return 'สำเร็จบางส่วน';
      case 'FAILED':
        return 'ล้มเหลว';
      case 'PROCESSING':
        return 'กำลังประมวลผล';
      default:
        return status;
    }
  };

  const formatDate = (dateString: string) => {
    const date = new Date(dateString);
    return date.toLocaleString('th-TH', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const getFileTypeIcon = (fileType: string) => {
    switch (fileType.toLowerCase()) {
      case 'csv':
        return '📊';
      case 'excel':
      case 'xlsx':
        return '📈';
      case 'json':
        return '📋';
      default:
        return '📄';
    }
  };

  return (
    <AuthGuard>
      <Layout>
        <div className="py-6">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 md:px-8">
            {/* Header */}
            <div className="mb-6">
              <h1 className="text-2xl font-bold text-gray-900 flex items-center">
                <History className="h-7 w-7 text-blue-600 mr-3" />
                ประวัติการนำเข้าข้อมูล
              </h1>
              <p className="text-gray-600 mt-1">
                ดูประวัติการนำเข้าข้อมูลทั้งหมดของ {session?.user?.companyName}
              </p>
            </div>

            {/* Content */}
            <div className="bg-white shadow overflow-hidden sm:rounded-md">
              {loading ? (
                <div className="flex justify-center items-center py-12">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
                  <span className="ml-2 text-gray-600">กำลังโหลดข้อมูล...</span>
                </div>
              ) : importHistory.length === 0 ? (
                <div className="text-center py-12">
                  <History className="mx-auto h-12 w-12 text-gray-400" />
                  <h3 className="mt-2 text-sm font-medium text-gray-900">ไม่มีประวัติการนำเข้า</h3>
                  <p className="mt-1 text-sm text-gray-500">เริ่มต้นโดยการนำเข้าข้อมูลครั้งแรก</p>
                </div>
              ) : (
                <>
                  <ul className="divide-y divide-gray-200">
                    {importHistory.map((item: ImportLogItem) => (
                      <li key={item.id} className="px-6 py-4 hover:bg-gray-50">
                        <div className="flex items-center justify-between">
                          <div className="flex items-center">
                            <div className="flex-shrink-0">
                              {getStatusIcon(item.status)}
                            </div>
                            <div className="ml-4">
                              <div className="flex items-center">
                                <span className="text-lg mr-2">
                                  {getFileTypeIcon(item.fileType)}
                                </span>
                                <p className="text-sm font-medium text-gray-900">
                                  {item.fileName}
                                </p>
                              </div>
                              <div className="mt-1 flex items-center text-sm text-gray-500">
                                <span>ตาราง: {item.tableName}</span>
                                <span className="mx-2">•</span>
                                <span>โดย: {item.user?.name || 'Unknown'}</span>
                                <span className="mx-2">•</span>
                                <span>{formatDate(item.createdAt)}</span>
                              </div>
                            </div>
                          </div>
                          <div className="flex items-center space-x-4">
                            <div className="text-right">
                              <div className="text-sm text-gray-900">
                                {item.successRows || 0} / {item.totalRows || 0} แถว
                              </div>
                              {item.errorRows > 0 && (
                                <div className="text-sm text-red-600">
                                  ข้อผิดพลาด: {item.errorRows} แถว
                                </div>
                              )}
                            </div>
                            <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getStatusColor(item.status)}`}>
                              {getStatusText(item.status)}
                            </span>
                          </div>
                        </div>
                      </li>
                    ))}
                  </ul>

                  {/* Pagination */}
                  {pagination.pages > 1 && (
                    <div className="bg-white px-4 py-3 flex items-center justify-between border-t border-gray-200 sm:px-6">
                      <div className="flex-1 flex justify-between sm:hidden">
                        <button
                          onClick={() => setPagination(prev => ({ ...prev, page: Math.max(1, prev.page - 1) }))}
                          disabled={pagination.page === 1}
                          className="relative inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                          ก่อนหน้า
                        </button>
                        <button
                          onClick={() => setPagination(prev => ({ ...prev, page: Math.min(prev.pages, prev.page + 1) }))}
                          disabled={pagination.page === pagination.pages}
                          className="ml-3 relative inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                          ถัดไป
                        </button>
                      </div>
                      <div className="hidden sm:flex-1 sm:flex sm:items-center sm:justify-between">
                        <div>
                          <p className="text-sm text-gray-700">
                            แสดง <span className="font-medium">{((pagination.page - 1) * pagination.limit) + 1}</span> ถึง{' '}
                            <span className="font-medium">{Math.min(pagination.page * pagination.limit, pagination.total)}</span> จาก{' '}
                            <span className="font-medium">{pagination.total}</span> รายการ
                          </p>
                        </div>
                        <div>
                          <nav className="relative z-0 inline-flex rounded-md shadow-sm -space-x-px">
                            <button
                              onClick={() => setPagination(prev => ({ ...prev, page: Math.max(1, prev.page - 1) }))}
                              disabled={pagination.page === 1}
                              className="relative inline-flex items-center px-2 py-2 rounded-l-md border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                            >
                              ก่อนหน้า
                            </button>
                            {Array.from({ length: Math.min(pagination.pages, 5) }, (_, i) => i + 1).map((page) => (
                              <button
                                key={page}
                                onClick={() => setPagination(prev => ({ ...prev, page }))}
                                className={`relative inline-flex items-center px-4 py-2 border text-sm font-medium ${
                                  page === pagination.page
                                    ? 'z-10 bg-blue-50 border-blue-500 text-blue-600'
                                    : 'bg-white border-gray-300 text-gray-500 hover:bg-gray-50'
                                }`}
                              >
                                {page}
                              </button>
                            ))}
                            <button
                              onClick={() => setPagination(prev => ({ ...prev, page: Math.min(prev.pages, prev.page + 1) }))}
                              disabled={pagination.page === pagination.pages}
                              className="relative inline-flex items-center px-2 py-2 rounded-r-md border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                            >
                              ถัดไป
                            </button>
                          </nav>
                        </div>
                      </div>
                    </div>
                  )}
                </>
              )}
            </div>
          </div>
        </div>
      </Layout>
    </AuthGuard>
  );
}