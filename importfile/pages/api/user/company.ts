// pages/api/user/company.ts
import { NextApiRequest, NextApiResponse } from 'next';
import { getServerSession } from 'next-auth';
import { authOptions } from '../../../lib/auth';
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const session = await getServerSession(req, res, authOptions);
    if (!session?.user) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // ตรวจสอบว่า email ไม่เป็น null หรือ undefined
    if (!session.user.email) {
      return res.status(400).json({ error: 'User email not found in session' });
    }

    // ดึงข้อมูล user พร้อม company information
    const user = await prisma.user.findUnique({
      where: {
        email: session.user.email
      },
      include: {
        company: {
          select: {
            id: true,
            name: true,
            code: true,
            dbName: true,
            description: true,
            location: true,
            createdAt: true,
            updatedAt: true
          }
        }
      }
    });

    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }

    if (!user.company) {
      return res.status(404).json({ error: 'Company not found' });
    }

    return res.status(200).json({
      success: true,
      data: {
        user: {
          id: user.id,
          name: user.name,
          email: user.email,
          role: user.role,
          companyId: user.companyId
        },
        company: user.company
      }
    });

  } catch (error) {
    console.error('Error fetching company info:', error);
    return res.status(500).json({
      error: 'Failed to fetch company information',
      details: error instanceof Error ? error.message : 'Unknown error'
    });
  } finally {
    await prisma.$disconnect();
  }
}