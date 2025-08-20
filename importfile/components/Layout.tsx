// importfile/components/Layout.tsx - Updated for Unified System
import React, { useState } from 'react';
import { useSession, signOut } from 'next-auth/react';
import Link from 'next/link';
import { useRouter } from 'next/router';
import { 
  Building2, 
  Database, 
  History, 
  LogOut, 
  Menu, 
  X,
  User,
  Settings
} from 'lucide-react';

interface LayoutProps {
  children: React.ReactNode;
}

export default function Layout({ children }: LayoutProps) {
  const { data: session } = useSession();
  const router = useRouter();
  const [sidebarOpen, setSidebarOpen] = useState(false);

  const navigation = [
    {
      name: 'จัดการข้อมูล',
      href: '/dashboard',
      icon: Database,
      current: router.pathname === '/dashboard',
      description: 'นำเข้าข้อมูล จัดการ Schema และ Tables'
    },
    {
      name: 'ประวัติการนำเข้า',
      href: '/history',
      icon: History,
      current: router.pathname === '/history',
      description: 'ดูประวัติการนำเข้าข้อมูลทั้งหมด'
    },
    {
      name: 'ตั้งค่า',
      href: '/settings',
      icon: Settings,
      current: router.pathname === '/settings',
      description: 'ตั้งค่าระบบและผู้ใช้งาน'
    }
  ];

  const handleSignOut = async () => {
    try {
      // Clear session และ redirect manual
      await signOut({ redirect: false });
      
      // Force redirect ด้วย router
      router.push('/auth/signin');
      
      // หรือถ้าต้องการ force reload page
      window.location.href = '/auth/signin';
    } catch (error) {
      console.error('Logout error:', error);
      // Fallback - force redirect
      window.location.href = '/auth/signin';
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Mobile sidebar */}
      <div className={`fixed inset-0 flex z-40 md:hidden ${sidebarOpen ? '' : 'hidden'}`}>
        <div className="fixed inset-0 bg-gray-600 bg-opacity-75" onClick={() => setSidebarOpen(false)} />
        
        <div className="relative flex-1 flex flex-col max-w-xs w-full bg-white">
          <div className="absolute top-0 right-0 -mr-12 pt-2">
            <button
              type="button"
              className="ml-1 flex items-center justify-center h-10 w-10 rounded-full focus:outline-none focus:ring-2 focus:ring-inset focus:ring-white"
              onClick={() => setSidebarOpen(false)}
            >
              <X className="h-6 w-6 text-white" />
            </button>
          </div>
          
          <div className="flex-1 h-0 pt-5 pb-4 overflow-y-auto">
            <div className="flex-shrink-0 flex items-center px-4">
              <Building2 className="h-8 w-8 text-blue-600" />
              <span className="ml-2 text-lg font-semibold text-gray-900">
                Siamtemp
              </span>
            </div>
            <nav className="mt-5 px-2 space-y-1">
              {navigation.map((item) => (
                <Link
                  key={item.name}
                  href={item.href}
                  className={`group flex items-center px-2 py-3 text-base font-medium rounded-md transition-colors ${
                    item.current
                      ? 'bg-blue-100 text-blue-900'
                      : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900'
                  }`}
                >
                  <item.icon
                    className={`mr-4 flex-shrink-0 h-6 w-6 ${
                      item.current ? 'text-blue-500' : 'text-gray-400 group-hover:text-gray-500'
                    }`}
                  />
                  <div>
                    <div>{item.name}</div>
                    <div className="text-xs text-gray-500 mt-1">{item.description}</div>
                  </div>
                </Link>
              ))}
            </nav>
          </div>
          
          <div className="flex-shrink-0 flex border-t border-gray-200 p-4">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <div className="h-10 w-10 rounded-full bg-blue-500 flex items-center justify-center">
                  <User className="h-6 w-6 text-white" />
                </div>
              </div>
              <div className="ml-3">
                <p className="text-base font-medium text-gray-700">{session?.user?.name}</p>
                <p className="text-sm font-medium text-gray-500">{session?.user?.companyName}</p>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Desktop sidebar */}
      <div className="hidden md:flex md:w-80 md:flex-col md:fixed md:inset-y-0">
        <div className="flex-1 flex flex-col min-h-0 border-r border-gray-200 bg-white">
          <div className="flex-1 flex flex-col pt-5 pb-4 overflow-y-auto">
            <div className="flex items-center flex-shrink-0 px-4 pb-4">
              <Building2 className="h-8 w-8 text-blue-600" />
              <span className="ml-2 text-lg font-semibold text-gray-900">
                Siamtemp
              </span>
            </div>
            
            <nav className="mt-5 flex-1 px-2 bg-white space-y-2">
              {navigation.map((item) => (
                <Link
                  key={item.name}
                  href={item.href}
                  className={`group flex items-start px-4 py-4 text-sm font-medium rounded-lg transition-all ${
                    item.current
                      ? 'bg-blue-100 text-blue-900 border-l-4 border-blue-500'
                      : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900'
                  }`}
                >
                  <item.icon
                    className={`mr-4 flex-shrink-0 h-6 w-6 mt-0.5 ${
                      item.current ? 'text-blue-500' : 'text-gray-400 group-hover:text-gray-500'
                    }`}
                  />
                  <div>
                    <div className="font-medium">{item.name}</div>
                    <div className="text-xs text-gray-500 mt-1 leading-relaxed">
                      {item.description}
                    </div>
                  </div>
                </Link>
              ))}
            </nav>

            {/* Quick Stats */}
            <div className="px-4 py-4">
              <div className="bg-gradient-to-r from-blue-50 to-indigo-50 rounded-lg p-4">
                <h3 className="text-sm font-medium text-gray-900 mb-2">สถิติการใช้งาน</h3>
                <div className="space-y-2">
                  <div className="flex justify-between text-xs">
                    <span className="text-gray-600">Tables</span>
                    <span className="font-medium text-gray-900">12</span>
                  </div>
                  <div className="flex justify-between text-xs">
                    <span className="text-gray-600">Imports (เดือนนี้)</span>
                    <span className="font-medium text-gray-900">48</span>
                  </div>
                  <div className="flex justify-between text-xs">
                    <span className="text-gray-600">Storage</span>
                    <span className="font-medium text-gray-900">2.4 GB</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
          
          <div className="flex-shrink-0 flex border-t border-gray-200 p-4">
            <div className="flex items-center w-full">
              <div className="flex-shrink-0">
                <div className="h-10 w-10 rounded-full bg-blue-500 flex items-center justify-center">
                  <User className="h-6 w-6 text-white" />
                </div>
              </div>
              <div className="ml-3 flex-1">
                <p className="text-sm font-medium text-gray-700">{session?.user?.name}</p>
                <p className="text-xs font-medium text-gray-500">{session?.user?.companyName}</p>
              </div>
              <button
                onClick={handleSignOut}
                className="ml-2 p-2 text-gray-400 hover:text-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500 rounded-md transition-colors"
                title="ออกจากระบบ"
              >
                <LogOut className="h-5 w-5" />
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Main content */}
      <div className="md:pl-80 flex flex-col flex-1">
        <div className="sticky top-0 z-10 md:hidden pl-1 pt-1 sm:pl-3 sm:pt-3 bg-gray-50">
          <button
            type="button"
            className="-ml-0.5 -mt-0.5 h-12 w-12 inline-flex items-center justify-center rounded-md text-gray-500 hover:text-gray-900 focus:outline-none focus:ring-2 focus:ring-inset focus:ring-blue-500"
            onClick={() => setSidebarOpen(true)}
          >
            <span className="sr-only">เปิดเมนู</span>
            <Menu className="h-6 w-6" />
          </button>
        </div>
        
        <main className="flex-1">
          {children}
        </main>
      </div>
    </div>
  );
}

