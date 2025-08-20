// pages/dashboard.tsx - Updated for Complete System
import { GetServerSideProps } from 'next';
import { getServerSession } from 'next-auth';
import { authOptions } from '../lib/auth';
import AuthGuard from '../components/AuthGuard';
import Layout from '../components/Layout';
import CompleteSchemaManagementSystem from '../components/CompleteSchemaManagementSystem';

export default function Dashboard() {
  return (
    <AuthGuard>
      <Layout>
        <CompleteSchemaManagementSystem />
      </Layout>
    </AuthGuard>
  );
}

export const getServerSideProps: GetServerSideProps = async (context) => {
  try {
    const session = await getServerSession(context.req, context.res, authOptions);
    
    if (!session) {
      return {
        redirect: {
          destination: '/auth/signin',
          permanent: false,
        },
      };
    }

    // ตรวจสอบว่า user มี companyCode หรือไม่
    if (!session.user?.companyCode) {
      return {
        redirect: {
          destination: '/auth/setup', // หน้าสำหรับตั้งค่า company (ถ้ามี)
          permanent: false,
        },
      };
    }

    return {
      props: {
        session,
      },
    };
  } catch (error) {
    console.error('Error in dashboard getServerSideProps:', error);
    return {
      redirect: {
        destination: '/auth/signin',
        permanent: false,
      },
    };
  }
};