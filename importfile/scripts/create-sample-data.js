const fs = require('fs');
const path = require('path');

// Sample Employee Data
const sampleEmployees = [
  {
    name: 'สมชาย ใจดี',
    department: 'Information tempnology',
    position: 'Senior Developer',
    salary: 65000,
    hire_date: '2023-01-15',
    email: 'somchai@siamtemp.com'
  },
  {
    name: 'สมหญิง รักงาน',
    department: 'Sales & Marketing', 
    position: 'Marketing Manager',
    salary: 55000,
    hire_date: '2023-02-01',
    email: 'somying@siamtemp.com'
  },
  {
    name: 'วิชัย เก่งงาน',
    department: 'Human Resources',
    position: 'HR Specialist',
    salary: 45000,
    hire_date: '2023-03-10',
    email: 'wichai@siamtemp.com'
  }
];

// Sample Project Data
const sampleProjects = [
  {
    name: 'E-Commerce Platform',
    client: 'ABC Company',
    budget: 1500000,
    status: 'active',
    start_date: '2024-01-01',
    end_date: '2024-06-30',
    temp_stack: 'React, Node.js, PostgreSQL'
  },
  {
    name: 'Mobile App Development',
    client: 'XYZ Corporation',
    budget: 800000,
    status: 'completed',
    start_date: '2023-10-01', 
    end_date: '2024-01-15',
    temp_stack: 'React Native, Firebase'
  }
];

// Sample Department Data
const sampleDepartments = [
  {
    name: 'Information tempnology',
    description: 'แผนกเทคโนโลยีสารสนเทศ',
    budget: 5000000,
    location: 'Floor 3',
    established_date: '2022-01-01'
  },
  {
    name: 'Sales & Marketing',
    description: 'แผนกขายและการตลาด',
    budget: 3000000,
    location: 'Floor 2', 
    established_date: '2022-01-01'
  }
];

function createCSV(data, filename) {
  if (data.length === 0) return;
  
  const headers = Object.keys(data[0]);
  const csvContent = [
    headers.join(','),
    ...data.map(row => 
      headers.map(header => {
        const value = row[header];
        // Handle values that contain commas or quotes
        if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {
          return `"${value.replace(/"/g, '""')}"`;
        }
        return value;
      }).join(',')
    )
  ].join('\n');
  
  fs.writeFileSync(path.join(__dirname, '..', 'sample-data', filename), csvContent);
  console.log(`✅ Created ${filename}`);
}

function createJSON(data, filename) {
  fs.writeFileSync(
    path.join(__dirname, '..', 'sample-data', filename),
    JSON.stringify(data, null, 2)
  );
  console.log(`✅ Created ${filename}`);
}

// Create sample-data directory if it doesn't exist
const sampleDataDir = path.join(__dirname, '..', 'sample-data');
if (!fs.existsSync(sampleDataDir)) {
  fs.mkdirSync(sampleDataDir);
}

console.log('📁 Creating sample data files...');

// Create CSV files
createCSV(sampleEmployees, 'employees.csv');
createCSV(sampleProjects, 'projects.csv');
createCSV(sampleDepartments, 'departments.csv');

// Create JSON files
createJSON(sampleEmployees, 'employees.json');
createJSON(sampleProjects, 'projects.json');
createJSON(sampleDepartments, 'departments.json');

console.log('✨ Sample data files created in /sample-data directory');
