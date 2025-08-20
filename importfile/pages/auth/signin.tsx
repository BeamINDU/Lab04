import { useState, useEffect } from 'react';
import { signIn, getSession } from 'next-auth/react';
import { useRouter } from 'next/router';
import { useForm } from 'react-hook-form';
import { toast } from 'react-hot-toast';
import { Eye, EyeOff, Building2, Lock, Mail, AlertCircle, CheckCircle } from 'lucide-react';
import { GetServerSideProps } from 'next';
import { getServerSession } from 'next-auth';
import { authOptions } from '../../lib/auth';

interface SignInForm {
  email: string;
  password: string;
}

interface DemoAccount {
  company: string;
  email: string;
  password: string;
  description: string;
  color: string;
}

export default function SignIn() {
  const [showPassword, setShowPassword] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [selectedDemo, setSelectedDemo] = useState<string | null>(null);
  const [loginAttempts, setLoginAttempts] = useState(0);
  const router = useRouter();
  
  const {
    register,
    handleSubmit,
    setValue,
    formState: { errors },
    watch
  } = useForm<SignInForm>();

  const demoAccounts: DemoAccount[] = [
    {
      company: 'Company A',
      email: 'admin.a@siamtemp.com',
      password: 'password123',
      description: 'Bangkok HQ - ระบบจัดการหลัก',
      color: 'bg-blue-500'
    },
    {
      company: 'Company B',
      email: 'admin.b@siamtemp.com',
      password: 'password123',
      description: 'Chiang Mai Branch - สาขาภาคเหนือ',
      color: 'bg-green-500'
    },
    {
      company: 'Company C',
      email: 'admin.c@siamtemp.com',
      password: 'password123',
      description: 'International Office - สำนักงานต่างประเทศ',
      color: 'bg-purple-500'
    }
  ];

  // Reset login attempts after 5 minutes
  useEffect(() => {
    if (loginAttempts >= 3) {
      const timer = setTimeout(() => {
        setLoginAttempts(0);
        toast.success('คุณสามารถลองเข้าสู่ระบบใหม่ได้แล้ว');
      }, 300000); // 5 minutes

      return () => clearTimeout(timer);
    }
  }, [loginAttempts]);

  const fillDemoAccount = (demo: DemoAccount) => {
    setValue('email', demo.email);
    setValue('password', demo.password);
    setSelectedDemo(demo.company);
    
    // แก้ไข: ใช้ toast() แทน toast.info() พร้อมกับ custom icon
    toast(`เติมข้อมูลสำหรับ ${demo.company} แล้ว`, {
      icon: '💡',
      duration: 3000,
      style: {
        background: '#3B82F6',
        color: '#ffffff',
      },
    });
  };

  const onSubmit = async (data: SignInForm) => {
    if (loginAttempts >= 3) {
      toast.error('คุณได้ลองเข้าสู่ระบบผิดมากเกินไป กรุณารอ 5 นาที');
      return;
    }

    setIsLoading(true);
    
    try {
      const result = await signIn('credentials', {
        email: data.email,
        password: data.password,
        redirect: false,
      });

      if (result?.error) {
        setLoginAttempts(prev => prev + 1);
        const remainingAttempts = 3 - (loginAttempts + 1);
        
        if (remainingAttempts > 0) {
          toast.error(`อีเมลหรือรหัสผ่านไม่ถูกต้อง (เหลืออีก ${remainingAttempts} ครั้ง)`);
        } else {
          toast.error('คุณได้ลองเข้าสู่ระบบผิด 3 ครั้งแล้ว กรุณารอ 5 นาที');
        }
      } else {
        // Reset attempts on successful login
        setLoginAttempts(0);
        toast.success('เข้าสู่ระบบสำเร็จ!');
        
        // Get fresh session to ensure proper redirect
        const session = await getSession();
        if (session?.user?.companyCode) {
          router.push('/dashboard');
        } else {
          router.push('/auth/setup');
        }
      }
    } catch (error) {
      console.error('Login error:', error);
      toast.error('เกิดข้อผิดพลาดในระบบ กรุณาลองใหม่อีกครั้ง');
    } finally {
      setIsLoading(false);
    }
  };

  const isBlocked = loginAttempts >= 3;
  const currentEmail = watch('email');
  const currentPassword = watch('password');
  const isFormValid = currentEmail && currentPassword && !errors.email && !errors.password;

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50 py-12 px-4 sm:px-6 lg:px-8">
      <div className="max-w-lg w-full space-y-8">
        {/* Header */}
        <div className="text-center">
          <div className="mx-auto h-20 w-20 bg-gradient-to-br from-blue-600 to-indigo-700 rounded-full flex items-center justify-center shadow-lg">
            <Building2 className="h-10 w-10 text-white" />
          </div>
          <h2 className="mt-6 text-3xl font-extrabold text-gray-900">
            เข้าสู่ระบบ
          </h2>
          <p className="mt-2 text-sm text-gray-600">
            ระบบจัดการข้อมูลบริษัท Siamtemp
          </p>
          {isBlocked && (
            <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-md">
              <div className="flex items-center">
                <AlertCircle className="h-5 w-5 text-red-500 mr-2" />
                <p className="text-sm text-red-700">
                  บัญชีถูกล็อคชั่วคราว กรุณารอ 5 นาที
                </p>
              </div>
            </div>
          )}
        </div>

        {/* Form */}
        <form className="mt-8 space-y-6 bg-white p-8 rounded-xl shadow-lg border border-gray-100" onSubmit={handleSubmit(onSubmit)}>
          <div className="space-y-5">
            {/* Email Field */}
            <div>
              <label htmlFor="email" className="block text-sm font-medium text-gray-700 mb-2">
                อีเมล
              </label>
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <Mail className="h-5 w-5 text-gray-400" />
                </div>
                <input
                  {...register('email', {
                    required: 'กรุณากรอกอีเมล',
                    pattern: {
                      value: /^[^\s@]+@[^\s@]+\.[^\s@]+$/,
                      message: 'รูปแบบอีเมลไม่ถูกต้อง'
                    }
                  })}
                  type="email"
                  autoComplete="email"
                  className={`block w-full pl-10 pr-3 py-3 border rounded-lg leading-5 bg-white placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors ${
                    errors.email ? 'border-red-300' : 'border-gray-300'
                  }`}
                  placeholder="กรอกอีเมลของคุณ"
                />
                {currentEmail && !errors.email && (
                  <div className="absolute inset-y-0 right-0 pr-3 flex items-center">
                    <CheckCircle className="h-5 w-5 text-green-500" />
                  </div>
                )}
              </div>
              {errors.email && (
                <p className="mt-2 text-sm text-red-600 flex items-center">
                  <AlertCircle className="h-4 w-4 mr-1" />
                  {errors.email.message}
                </p>
              )}
            </div>

            {/* Password Field */}
            <div>
              <label htmlFor="password" className="block text-sm font-medium text-gray-700 mb-2">
                รหัสผ่าน
              </label>
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <Lock className="h-5 w-5 text-gray-400" />
                </div>
                <input
                  {...register('password', {
                    required: 'กรุณากรอกรหัสผ่าน',
                    minLength: {
                      value: 6,
                      message: 'รหัสผ่านต้องมีอย่างน้อย 6 ตัวอักษร'
                    }
                  })}
                  type={showPassword ? 'text' : 'password'}
                  autoComplete="current-password"
                  className={`block w-full pl-10 pr-12 py-3 border rounded-lg leading-5 bg-white placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors ${
                    errors.password ? 'border-red-300' : 'border-gray-300'
                  }`}
                  placeholder="กรอกรหัสผ่านของคุณ"
                />
                <div className="absolute inset-y-0 right-0 flex items-center">
                  {currentPassword && !errors.password && (
                    <CheckCircle className="h-5 w-5 text-green-500 mr-2" />
                  )}
                  <button
                    type="button"
                    className="text-gray-400 hover:text-gray-600 focus:outline-none focus:text-gray-600 pr-3"
                    onClick={() => setShowPassword(!showPassword)}
                    tabIndex={-1}
                  >
                    {showPassword ? <EyeOff className="h-5 w-5" /> : <Eye className="h-5 w-5" />}
                  </button>
                </div>
              </div>
              {errors.password && (
                <p className="mt-2 text-sm text-red-600 flex items-center">
                  <AlertCircle className="h-4 w-4 mr-1" />
                  {errors.password.message}
                </p>
              )}
            </div>
          </div>

          {/* Submit Button */}
          <div>
            <button
              type="submit"
              disabled={isLoading || isBlocked || !isFormValid}
              className={`group relative w-full flex justify-center py-3 px-4 border border-transparent text-sm font-medium rounded-lg text-white transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 ${
                isFormValid && !isBlocked && !isLoading
                  ? 'bg-blue-600 hover:bg-blue-700 shadow-md hover:shadow-lg'
                  : 'bg-gray-400 cursor-not-allowed'
              }`}
            >
              {isLoading ? (
                <div className="flex items-center">
                  <div className="animate-spin rounded-full h-5 w-5 border-2 border-white border-t-transparent mr-3"></div>
                  กำลังเข้าสู่ระบบ...
                </div>
              ) : isBlocked ? (
                'บัญชีถูกล็อคชั่วคราว'
              ) : (
                'เข้าสู่ระบบ'
              )}
            </button>
          </div>

          {/* Demo Accounts Section */}
          <div className="mt-8 border-t border-gray-200 pt-6">
            <p className="text-sm text-gray-600 text-center mb-4">
              🚀 บัญชีทดลองใช้งาน (Demo Accounts):
            </p>
            <div className="grid gap-3">
              {demoAccounts.map((demo, index) => (
                <button
                  key={index}
                  type="button"
                  onClick={() => fillDemoAccount(demo)}
                  className={`p-3 rounded-lg border border-gray-200 hover:border-gray-300 transition-all text-left group ${
                    selectedDemo === demo.company ? 'ring-2 ring-blue-500 border-blue-300 bg-blue-50' : 'hover:bg-gray-50'
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center space-x-3">
                      <div className={`w-3 h-3 rounded-full ${demo.color}`}></div>
                      <div>
                        <p className="text-sm font-medium text-gray-900">{demo.company}</p>
                        <p className="text-xs text-gray-500">{demo.description}</p>
                      </div>
                    </div>
                    <div className="text-xs text-gray-400 group-hover:text-gray-600">
                      คลิกเพื่อเติมข้อมูล
                    </div>
                  </div>
                </button>
              ))}
            </div>
            
            {selectedDemo && (
              <div className="mt-4 p-3 bg-blue-50 border border-blue-200 rounded-lg">
                <p className="text-xs text-blue-700 text-center">
                  ✨ ข้อมูล {selectedDemo} ถูกเติมในฟอร์มแล้ว กดปุ่ม "เข้าสู่ระบบ" เพื่อดำเนินการต่อ
                </p>
              </div>
            )}
          </div>

          {/* Help Text */}
          <div className="mt-6 text-center">
            <p className="text-xs text-gray-500">
              มีปัญหาในการเข้าสู่ระบบ? ติดต่อ IT Support: 
              <span className="font-medium text-blue-600"> support@siamtemp.com</span>
            </p>
          </div>
        </form>
      </div>
    </div>
  );
}

// Server-side protection - redirect if already logged in
export const getServerSideProps: GetServerSideProps = async (context) => {
  try {
    const session = await getServerSession(context.req, context.res, authOptions);
    
    // If user is already logged in, redirect to dashboard
    if (session?.user) {
      return {
        redirect: {
          destination: '/dashboard',
          permanent: false,
        },
      };
    }

    return {
      props: {},
    };
  } catch (error) {
    console.error('Error in signin getServerSideProps:', error);
    return {
      props: {},
    };
  }
};