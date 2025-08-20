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

    const { page = '1', limit = '10', status, tableName } = req.query;
    const pageNum = parseInt(page as string) || 1;
    const limitNum = parseInt(limit as string) || 10;
    const offset = (pageNum - 1) * limitNum;

    // Build where clause
    const where: any = {
      user: {
        companyId: session.user.companyId
      }
    };

    if (status && status !== 'all') {
      where.status = status;
    }

    if (tableName) {
      where.tableName = {
        contains: tableName as string,
        mode: 'insensitive'
      };
    }

    // Get total count
    const total = await prisma.importLog.count({ where });

    // Get paginated data
    const importLogs = await prisma.importLog.findMany({
      where,
      include: {
        user: {
          select: {
            id: true,
            name: true,
            email: true
          }
        },
        company: {
          select: {
            id: true,
            name: true,
            code: true
          }
        }
      },
      orderBy: {
        createdAt: 'desc'
      },
      take: limitNum,
      skip: offset
    });

    const pages = Math.ceil(total / limitNum);

    return res.status(200).json({
      success: true,
      data: importLogs,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        pages
      }
    });

  } catch (error) {
    console.error('Error fetching import history:', error);
    return res.status(500).json({
      error: 'Failed to fetch import history',
      details: error instanceof Error ? error.message : 'Unknown error'
    });
  } finally {
    await prisma.$disconnect();
  }
}