-- Company B (SiamTech Regional Office) Database Schema
-- เชียงใหม่ - สาขาภาคเหนือ

-- Create database
CREATE DATABASE siamtech_company_b;
\c siamtech_company_b;

-- Create employees table (same structure)
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    department VARCHAR(50) NOT NULL,
    position VARCHAR(100) NOT NULL,
    salary DECIMAL(10,2) NOT NULL,
    hire_date DATE NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL
);

-- Create projects table (same structure)
CREATE TABLE projects (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    client VARCHAR(100) NOT NULL,
    budget DECIMAL(12,2) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('active', 'completed', 'cancelled')),
    start_date DATE NOT NULL,
    end_date DATE,
    tech_stack VARCHAR(500)
);

-- Create employee_projects junction table
CREATE TABLE employee_projects (
    employee_id INTEGER REFERENCES employees(id),
    project_id INTEGER REFERENCES projects(id),
    role VARCHAR(100) NOT NULL,
    allocation DECIMAL(3,2) NOT NULL CHECK (allocation BETWEEN 0 AND 1),
    PRIMARY KEY (employee_id, project_id)
);

-- Insert Company B employees (Regional Office - 10 people)
INSERT INTO employees (name, department, position, salary, hire_date, email) VALUES
-- IT Department (7 people)
('สมปอง เขียนโค้ด', 'IT', 'Senior Developer', 55000.00, '2022-02-15', 'sompong@siamtech-north.co.th'),
('มณี พัฒนา', 'IT', 'Frontend Developer', 42000.00, '2022-04-20', 'manee@siamtech-north.co.th'),
('วีระ ระบบ', 'IT', 'Backend Developer', 45000.00, '2022-03-10', 'weera@siamtech-north.co.th'),
('ปิยะ ดีไซน์', 'IT', 'UI/UX Designer', 38000.00, '2022-05-15', 'piya@siamtech-north.co.th'),
('ชาญ ทดสอบ', 'IT', 'QA Engineer', 35000.00, '2022-06-01', 'chan@siamtech-north.co.th'),
('กิตติ โมบาย', 'IT', 'Mobile Developer', 43000.00, '2022-07-10', 'kitti@siamtech-north.co.th'),
('อนุรักษ์ ดาต้า', 'IT', 'Data Analyst', 40000.00, '2022-08-05', 'anurak@siamtech-north.co.th'),

-- Sales Department (2 people)
('สุดา ขายดี', 'Sales', 'Sales Manager', 45000.00, '2022-03-01', 'suda@siamtech-north.co.th'),
('ประยุทธ ลูกค้า', 'Sales', 'Account Executive', 32000.00, '2022-07-15', 'prayut@siamtech-north.co.th'),

-- Management (1 person)
('วิไล จัดการ', 'Management', 'Branch Manager', 70000.00, '2022-02-01', 'wilai@siamtech-north.co.th');

-- Insert Company B projects (Regional projects)
INSERT INTO projects (name, client, budget, status, start_date, end_date, tech_stack) VALUES
('ระบบจัดการโรงแรม', 'โรงแรมดุสิต เชียงใหม่', 800000.00, 'active', '2024-02-01', '2024-06-30', 'Vue.js, Node.js, MySQL'),
('เว็บไซต์ท่องเที่ยว', 'การท่องเที่ยวแห่งประเทศไทย', 600000.00, 'active', '2024-03-15', '2024-07-15', 'React, Firebase, Maps API'),
('Mobile App สวนสวยงาม', 'สวนพฤกษศาสตร์เชียงใหม่', 450000.00, 'completed', '2023-09-01', '2024-01-15', 'Flutter, Firebase'),
('ระบบ POS ร้านอาหาร', 'กลุ่มร้านอาหารล้านนา', 350000.00, 'active', '2024-01-20', '2024-05-20', 'React Native, Node.js');

-- Insert employee-project relationships (Company B)
INSERT INTO employee_projects (employee_id, project_id, role, allocation) VALUES
-- Hotel Management System
(1, 1, 'Lead Developer', 0.8),
(2, 1, 'Frontend Developer', 1.0),
(3, 1, 'Backend Developer', 0.9),
(4, 1, 'UI/UX Designer', 0.7),

-- Tourism Website
(2, 2, 'Frontend Lead', 0.0), -- will start after hotel project
(4, 2, 'UI/UX Designer', 0.3),
(7, 2, 'Data Integration', 0.6),

-- Mobile App (completed)
(6, 3, 'Mobile Developer', 0.0),
(4, 3, 'UI/UX Designer', 0.0),

-- POS System
(3, 4, 'Backend Lead', 0.1),
(6, 4, 'Mobile Developer', 0.8),
(5, 4, 'QA Engineer', 1.0);

-- Create indexes
CREATE INDEX idx_employees_department_b ON employees(department);
CREATE INDEX idx_employees_position_b ON employees(position);
CREATE INDEX idx_projects_status_b ON projects(status);

-- Create view for department summary
CREATE VIEW department_summary AS
SELECT 
    department,
    COUNT(*) as employee_count,
    AVG(salary) as avg_salary,
    MIN(hire_date) as earliest_hire,
    MAX(hire_date) as latest_hire
FROM employees 
GROUP BY department;

-- Company B specific information
COMMENT ON DATABASE siamtech_company_b IS 'SiamTech Regional Office Database - Chiang Mai';
COMMENT ON TABLE employees IS 'พนักงานสาขาภาคเหนือ เชียงใหม่';
COMMENT ON TABLE projects IS 'โปรเจคสาขาภาคเหนือ';

-- Insert company metadata
CREATE TABLE company_info (
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(200) NOT NULL,
    branch_type VARCHAR(100) NOT NULL,
    location VARCHAR(200) NOT NULL,
    total_employees INTEGER NOT NULL,
    established_date DATE NOT NULL,
    description TEXT
);

INSERT INTO company_info (company_name, branch_type, location, total_employees, established_date, description) VALUES
('SiamTech Regional Office', 'Regional Branch', 'Chiang Mai, Thailand', 10, '2022-02-01', 'สาขาภาคเหนือของบริษัท สยามเทค จำกัด ตั้งอยู่ในจังหวัดเชียงใหม่ เน้นงานบริการลูกค้าภาคเหนือและโปรเจคท้องถิ่น');

-- Regional-specific view
CREATE VIEW regional_projects AS
SELECT 
    p.name as project_name,
    p.client,
    p.budget,
    p.status,
    'Northern Thailand' as region,
    COUNT(ep.employee_id) as team_size
FROM projects p
LEFT JOIN employee_projects ep ON p.id = ep.project_id
GROUP BY p.id, p.name, p.client, p.budget, p.status;

-- Final check
SELECT 'Company B Database Setup Complete' as status,
       (SELECT COUNT(*) FROM employees) as total_employees,
       (SELECT COUNT(*) FROM projects) as total_projects;