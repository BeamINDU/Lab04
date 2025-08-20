const { PrismaClient } = require('@prisma/client');
const bcrypt = require('bcryptjs');

const prisma = new PrismaClient();

async function createCompanyAUsers() {
  console.log('🚀 Creating 5 users for Company A...');

  const hashedPassword = await bcrypt.hash('password123', 12);
  
  const companyAUsers = [
    {
      email: 'admin.a@siamtemp.com',
      name: 'Admin Company A',
      password: hashedPassword,
      companyId: 'company-a-id',
      role: 'ADMIN'
    },
    {
      email: 'manager.a@siamtemp.com',
      name: 'Manager Company A',
      password: hashedPassword,
      companyId: 'company-a-id',
      role: 'MANAGER'
    },
    {
      email: 'dev1.a@siamtemp.com',
      name: 'Developer 1 Company A',
      password: hashedPassword,
      companyId: 'company-a-id',
      role: 'USER'
    },
    {
      email: 'dev2.a@siamtemp.com',
      name: 'Developer 2 Company A',
      password: hashedPassword,
      companyId: 'company-a-id',
      role: 'USER'
    },
    {
      email: 'analyst.a@siamtemp.com',
      name: 'Data Analyst Company A',
      password: hashedPassword,
      companyId: 'company-a-id',
      role: 'USER'
    }
  ];

  console.log('👥 Creating Company A users...');
  for (const user of companyAUsers) {
    try {
      await prisma.user.upsert({
        where: { email: user.email },
        update: user,
        create: user,
      });
      console.log(`✅ User created: ${user.email} (${user.role})`);
    } catch (error) {
      console.error(`❌ Failed to create user ${user.email}:`, error.message);
    }
  }

  console.log('\n🔑 Company A Login Credentials:');
  console.log('Admin:     admin.a@siamtemp.com / password123');
  console.log('Manager:   manager.a@siamtemp.com / password123');
  console.log('Dev 1:     dev1.a@siamtemp.com / password123');
  console.log('Dev 2:     dev2.a@siamtemp.com / password123');
  console.log('Analyst:   analyst.a@siamtemp.com / password123');
}

createCompanyAUsers()
  .catch((e) => {
    console.error('❌ Setup failed:', e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });