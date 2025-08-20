import { NextApiRequest, NextApiResponse } from 'next';
import { getServerSession } from 'next-auth';
import { authOptions } from '../../../../lib/auth';
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

    const { id } = req.query;

    const importLog = await prisma.importLog.findFirst({
      where: {
        id: id as string,
        user: {
          companyId: session.user.companyId
        }
      },
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
      }
    });

    if (!importLog) {
      return res.status(404).json({ error: 'Import log not found' });
    }

    return res.status(200).json({
      success: true,
      data: importLog
    });

  } catch (error) {
    console.error('Error fetching import details:', error);
    return res.status(500).json({
      error: 'Failed to fetch import details',
      details: error instanceof Error ? error.message : 'Unknown error'
    });
  } finally {
    await prisma.$disconnect();
  }
}
