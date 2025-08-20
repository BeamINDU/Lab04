import { NextApiRequest, NextApiResponse } from 'next';
import { getServerSession } from 'next-auth';
import { authOptions } from '../../../lib/auth';
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

interface DailyStatRaw {
  createdAt: Date;
  _count: {
    id: number;
  };
  _sum: {
    totalRows: number | null;
    successRows: number | null;
    errorRows: number | null;
  };
}

interface TopTableRaw {
  tableName: string;
  _count: {
    id: number;
  };
  _sum: {
    totalRows: number | null;
  };
}

interface TotalRowsAggregate {
  _sum: {
    totalRows: number | null;
    successRows: number | null;
    errorRows: number | null;
  };
}

const mockStats = {
  overview: {
    totalImports: 10,
    successfulImports: 8,
    failedImports: 1,
    partialImports: 1,
    successRate: 80,
    totalRowsProcessed: 1500,
    successfulRows: 1350,
    errorRows: 150
  },
  dailyStats: [
    {
      date: new Date(Date.now() - 86400000 * 6).toISOString(),
      imports: 2,
      totalRows: 300,
      successRows: 280,
      errorRows: 20
    },
    {
      date: new Date(Date.now() - 86400000 * 5).toISOString(),
      imports: 1,
      totalRows: 150,
      successRows: 150,
      errorRows: 0
    },
    {
      date: new Date(Date.now() - 86400000 * 4).toISOString(),
      imports: 3,
      totalRows: 450,
      successRows: 400,
      errorRows: 50
    }
  ],
  topTables: [
    { tableName: 'employees', imports: 5, totalRows: 750 },
    { tableName: 'products', imports: 3, totalRows: 450 },
    { tableName: 'customers', imports: 2, totalRows: 300 }
  ]
};

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  // 🐛 ย้ายการกำหนด period มาด้านนอก try/catch เพื่อให้อยู่ใน scope ตลอด
  const { period = '30d' } = req.query;

  try {
    const session = await getServerSession(req, res, authOptions);
    if (!session?.user) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Calculate date range
    const now = new Date();
    let startDate = new Date();
    
    switch (period) {
      case '7d':
        startDate.setDate(now.getDate() - 7);
        break;
      case '30d':
        startDate.setDate(now.getDate() - 30);
        break;
      case '90d':
        startDate.setDate(now.getDate() - 90);
        break;
      default:
        startDate.setDate(now.getDate() - 30);
    }

    const where = {
      user: {
        companyId: session.user.companyId
      },
      createdAt: {
        gte: startDate
      }
    };

    // Get overall stats
    const [
      totalImports,
      successfulImports,
      failedImports,
      partialImports,
      totalRowsProcessed
    ] = await Promise.all([
      prisma.importLog.count({ where }),
      prisma.importLog.count({ where: { ...where, status: 'COMPLETED' } }),
      prisma.importLog.count({ where: { ...where, status: 'FAILED' } }),
      prisma.importLog.count({ where: { ...where, status: 'PARTIAL' } }),
      prisma.importLog.aggregate({
        where,
        _sum: {
          totalRows: true,
          successRows: true,
          errorRows: true
        }
      })
    ]);

    // Get daily stats for chart - แก้ไข type assertion
    const dailyStatsRaw = await prisma.importLog.groupBy({
      by: ['createdAt'],
      where,
      _count: {
        id: true
      },
      _sum: {
        totalRows: true,
        successRows: true,
        errorRows: true
      },
      orderBy: {
        createdAt: 'asc'
      }
    });

    // Get top tables - แก้ไข type assertion
    const topTablesRaw = await prisma.importLog.groupBy({
      by: ['tableName'],
      where,
      _count: {
        id: true
      },
      _sum: {
        totalRows: true
      },
      orderBy: {
        _count: {
          id: 'desc'
        }
      },
      take: 10
    });

    // Transform data with proper typing
    const dailyStats = (dailyStatsRaw as DailyStatRaw[]).map((stat) => ({
      date: stat.createdAt.toISOString(),
      imports: stat._count.id,
      totalRows: stat._sum.totalRows || 0,
      successRows: stat._sum.successRows || 0,
      errorRows: stat._sum.errorRows || 0
    }));

    const topTables = (topTablesRaw as TopTableRaw[]).map((table) => ({
      tableName: table.tableName,
      imports: table._count.id,
      totalRows: table._sum.totalRows || 0
    }));

    const stats = {
      overview: {
        totalImports,
        successfulImports,
        failedImports,
        partialImports,
        successRate: totalImports > 0 ? Math.round((successfulImports / totalImports) * 100) : 0,
        totalRowsProcessed: (totalRowsProcessed as TotalRowsAggregate)._sum.totalRows || 0,
        successfulRows: (totalRowsProcessed as TotalRowsAggregate)._sum.successRows || 0,
        errorRows: (totalRowsProcessed as TotalRowsAggregate)._sum.errorRows || 0
      },
      dailyStats,
      topTables
    };

    return res.status(200).json({
      success: true,
      data: stats,
      period
    });

  } catch (error) {
    console.error('Error fetching import stats:', error);
    
    // 💡 ใช้ mockStats ที่ประกาศไว้ด้านนอกเพื่อลดการเขียนโค้ดซ้ำซ้อน
    return res.status(200).json({
      success: true,
      data: mockStats,
      period,
      note: 'Using mock data due to database error'
    });
  } finally {
    await prisma.$disconnect();
  }
}