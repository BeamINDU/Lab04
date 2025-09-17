const { PrismaClient } = require('@prisma/client');
const bcrypt = require('bcryptjs');

const prisma = new PrismaClient();

async function addAdmin() {
  try {
    // Hash password SiamPOC001_
    const hashedPassword = await bcrypt.hash('SiamPOC001_', 12);
    console.log('Generated hash for SiamPOC001_:', hashedPassword);
    
    // Create user
    const user = await prisma.user.create({
      data: {
        email: 'Admin@Siamtemp.com',
        name: 'Admin Siamtemp',
        password: hashedPassword,
        companyId: 'company-a-id',
        role: 'ADMIN'
      }
    });
    
    console.log('✅ Admin user created successfully:', user.email);
    console.log('Login with: Admin@Siamtemp.com / SiamPOC001_');
    
  } catch (error) {
    if (error.code === 'P2002') {
      console.log('User already exists, updating password...');
      
      const hashedPassword = await bcrypt.hash('SiamPOC001_', 12);
      
      const user = await prisma.user.update({
        where: { email: 'Admin@Siamtemp.com' },
        data: {
          password: hashedPassword,
          name: 'Admin Siamtemp'
        }
      });
      
      console.log('✅ Admin user password updated');
    } else {
      console.error('Error:', error);
    }
  }
}

addAdmin()
  .catch(console.error)
  .finally(() => prisma.$disconnect());