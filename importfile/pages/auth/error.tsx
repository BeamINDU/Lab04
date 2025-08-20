import Link from 'next/link';
import { useRouter } from 'next/router';
import { AlertCircle, ArrowLeft } from 'lucide-react';

export default function AuthError() {
  const router = useRouter();
  const { error } = router.query;

  const getErrorMessage = (error: string | string[] | undefined) => {
    if (typeof error !== 'string') return 'เกิดข้อผิดพลาดในการเข้าสู่ระบบ';
    
    switch (error) {
      case 'CredentialsSignin':
        return 'อีเมลหรือรหัสผ่านไม่ถูกต้อง';
      case 'OAuthAccountNotLinked':
        return 'บัญชีนี้ถูกลิงค์กับบริการอื่นแล้ว';
      case 'EmailSignin':
        return 'ไม่สามารถส่งอีเมลยืนยันได้';
      case 'Callback':
        return 'เกิดข้อผิดพลาดในระหว่างการตรวจสอบ';
      case 'OAuthCallback':
        return 'เกิดข้อผิดพลาดจากผู้ให้บริการ OAuth';
      case 'OAuthCreateAccount':
        return 'ไม่สามารถสร้างบัญชีได้';
      case 'EmailCreateAccount':
        return 'ไม่สามารถสร้างบัญชีด้วยอีเมลได้';
      case 'Signin':
        return 'เกิดข้อผิดพลาดในการเข้าสู่ระบบ';
      case 'OAuthSignin':
        return 'เกิดข้อผิดพลาดใน OAuth signin';
      case 'SessionRequired':
        return 'จำเป็นต้องเข้าสู่ระบบ';
      default:
        return 'เกิดข้อผิดพลาดที่ไม่ทราบสาเหตุ';
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-red-50 to-red-100 py-12 px-4 sm:px-6 lg:px-8">
      <div className="max-w-md w-full space-y-8">
        <div className="text-center">
          <div className="mx-auto h-16 w-16 bg-red-500 rounded-full flex items-center justify-center">
            <AlertCircle className="h-8 w-8 text-white" />
          </div>
          <h2 className="mt-6 text-3xl font-extrabold text-gray-900">
            เกิดข้อผิดพลาด
          </h2>
          <p className="mt-2 text-sm text-gray-600">
            {getErrorMessage(error)}
          </p>
        </div>

        <div className="bg-white p-8 rounded-lg shadow-md space-y-6">
          <div className="text-center">
            <p className="text-gray-600 mb-6">
              กรุณาลองใหม่อีกครั้ง หรือติดต่อผู้ดูแลระบบ
            </p>
            
            <div className="space-y-3">
              <Link
                href="/auth/signin"
                className="w-full flex justify-center py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200"
              >
                กลับไปหน้าเข้าสู่ระบบ
              </Link>
              
              <button
                onClick={() => router.back()}
                className="w-full flex items-center justify-center py-2 px-4 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200"
              >
                <ArrowLeft className="h-4 w-4 mr-2" />
                กลับไปหน้าก่อนหน้า
              </button>
            </div>
          </div>

          {/* Debug Info (only in development) */}
          {process.env.NODE_ENV === 'development' && error && (
            <div className="border-t border-gray-200 pt-6">
              <p className="text-xs text-gray-500 mb-2">Debug Information:</p>
              <code className="text-xs bg-gray-100 p-2 rounded block">
                {error}
              </code>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
