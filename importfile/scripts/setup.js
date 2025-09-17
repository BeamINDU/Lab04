const { PrismaClient } = require('@prisma/client');
const bcrypt = require('bcryptjs');

const prisma = new PrismaClient();

async function main() {
  console.log('🚀 Starting database setup...');

  // Create companies
  const companies = [
    {
      id: 'company-a-id',
      name: 'Siamtemp Main Office',
      code: 'company_a',
      dbName: 'siamtemp_company_a',
      description: 'สำนักงานใหญ่ สยามเทค จำกัด',
      location: 'กรุงเทพมหานคร'
    },
    {
      id: 'company-b-id',
      name: 'Siamtemp Regional Office',
      code: 'company_b',
      dbName: 'siamtemp_company_b',
      description: 'สาขาภาคเหนือ สยามเทค จำกัด',
      location: 'เชียงใหม่'
    },
    {
      id: 'company-c-id',
      name: 'Siamtemp International',
      code: 'company_c',
      dbName: 'siamtemp_company_c',
      description: 'สำนักงานต่างประเทศ สยามเทค จำกัด',
      location: 'International'
    }
  ];

  console.log('📊 Creating companies...');
  for (const company of companies) {
    await prisma.company.upsert({
      where: { code: company.code },
      update: company,
      create: company,
    });
    console.log(`✅ Company created: ${company.name}`);
  }

  // Create hashed passwords
  const hashedPassword123 = await bcrypt.hash('password123', 12);
  const hashedSiamPOC = await bcrypt.hash('SiamPOC001_', 12);  // Hash รหัสผ่านของ Admin

  const users = [
    {
      email: 'admin.a@siamtemp.com',
      name: 'Admin Company A',
      password: hashedPassword123,
      companyId: 'company-a-id',
      role: 'ADMIN'
    },
    {
      email: 'Admin@Siamtemp.com',  // Admin พิเศษ
      name: 'Admin Siamtemp',
      password: hashedSiamPOC,  // ใช้ hashed password ที่ถูกต้อง
      companyId: 'company-a-id',
      role: 'ADMIN'
    },
    {
      email: 'admin.b@siamtemp.com',
      name: 'Admin Company B',
      password: hashedPassword123,
      companyId: 'company-b-id',
      role: 'ADMIN'
    },
    {
      email: 'admin.c@siamtemp.com',
      name: 'Admin Company C',
      password: hashedPassword123,
      companyId: 'company-c-id',
      role: 'ADMIN'
    },
    // Regular users
    {
      email: 'manager.a@siamtemp.com',
      name: 'Manager Company A',
      password: hashedPassword123,
      companyId: 'company-a-id',
      role: 'MANAGER'
    },
    {
      email: 'user.a@siamtemp.com',
      name: 'User Company A',
      password: hashedPassword123,
      companyId: 'company-a-id',
      role: 'USER'
    }
  ];

  console.log('👥 Creating users...');
  for (const user of users) {
    try {
      await prisma.user.upsert({
        where: { email: user.email },
        update: {
          name: user.name,
          password: user.password,
          companyId: user.companyId,
          role: user.role
        },
        create: user,
      });
      console.log(`✅ User created/updated: ${user.email}`);
    } catch (error) {
      console.error(`❌ Failed to create user ${user.email}:`, error.message);
    }
  }

  console.log('✨ Database setup completed!');
  console.log('\n🔑 Demo Login Credentials:');
  console.log('Super Admin: Admin@Siamtemp.com / SiamPOC001_');
  console.log('Company A Admin: admin.a@siamtemp.com / password123');
  console.log('Company B Admin: admin.b@siamtemp.com / password123');
  console.log('Company C Admin: admin.c@siamtemp.com / password123');
  console.log('Company A Manager: manager.a@siamtemp.com / password123');
  console.log('Company A User: user.a@siamtemp.com / password123');

  // Verify users were created
  console.log('\n📋 Verifying created users:');
  const allUsers = await prisma.user.findMany({
    select: {
      email: true,
      name: true,
      role: true,
      company: {
        select: {
          name: true
        }
      }
    }
  });

  allUsers.forEach(user => {
    console.log(`  - ${user.email} (${user.role}) - ${user.company?.name || 'No company'}`);
  });
}

main()
  .catch((e) => {
    console.error('❌ Setup failed:', e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });