// importfile/pages/schema-management.tsx
import { GetServerSideProps } from 'next';
import { getServerSession } from 'next-auth';
import { authOptions } from '../lib/auth';
import AuthGuard from '../components/AuthGuard';
import Layout from '../components/Layout';
import SchemaManagementSystem from '../components/CompleteSchemaManagementSystem';

export default function SchemaManagementPage() {
  return (
    <AuthGuard>
      <Layout>
        <div className="py-6">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 md:px-8">
            <SchemaManagementSystem />
          </div>
        </div>
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

    if (session.user?.role && !['ADMIN', 'MANAGER'].includes(session.user.role)) {
      return {
        redirect: {
          destination: '/dashboard',
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
    console.error('Error in getServerSideProps:', error);
    return {
      redirect: {
        destination: '/auth/signin',
        permanent: false,
      },
    };
  }
};