-- Company A (SiamTech Main Office) Database Schema
-- กรุงเทพมหานคร - สำนักงานใหญ่

-- Create database
CREATE DATABASE siamtech_company_a;
\c siamtech_company_a;

-- Create employees table
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    department VARCHAR(50) NOT NULL,
    position VARCHAR(100) NOT NULL,
    salary DECIMAL(10,2) NOT NULL,
    hire_date DATE NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL
);

-- Create projects table
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

-- Insert Company A employees (Main Office - 15 people)
INSERT INTO employees (name, department, position, salary, hire_date, email) VALUES
-- IT Department (10 people)
('สมชาย ใจดี', 'IT', 'Senior Developer', 65000.00, '2022-01-15', 'somchai@siamtech-main.co.th'),
('สมหญิง รักงาน', 'IT', 'Frontend Developer', 50000.00, '2022-03-20', 'somying@siamtech-main.co.th'),
('ประสิทธิ์ เก่งมาก', 'IT', 'DevOps Engineer', 70000.00, '2021-11-10', 'prasit@siamtech-main.co.th'),
('วิชัย โค้ดดี', 'IT', 'Full Stack Developer', 55000.00, '2022-04-01', 'wichai@siamtech-main.co.th'),
('มาลี เขียนโค้ด', 'IT', 'Backend Developer', 52000.00, '2022-05-15', 'malee@siamtech-main.co.th'),
('สุรชัย ดีไซน์', 'IT', 'UI/UX Designer', 45000.00, '2022-06-01', 'surachai@siamtech-main.co.th'),
('นิรันดร์ ทดสอบ', 'IT', 'QA Engineer', 42000.00, '2022-07-10', 'niran@siamtech-main.co.th'),
('อำนาจ เน็ตเวิร์ก', 'IT', 'Network Engineer', 58000.00, '2022-02-20', 'amnaj@siamtech-main.co.th'),
('ชญานิษฐ์ เอไอ', 'IT', 'AI Developer', 72000.00, '2022-01-05', 'chayanit@siamtech-main.co.th'),
('ภาณุวัฒน์ อาร์คิเทค', 'IT', 'Solution Architect', 80000.00, '2021-10-15', 'phanuwat@siamtech-main.co.th'),

-- Sales Department (3 people)
('อาทิตย์ ขายเก่ง', 'Sales', 'Sales Manager', 60000.00, '2022-02-01', 'athit@siamtech-main.co.th'),
('จันทร์ ดูแลดี', 'Sales', 'Sales Executive', 40000.00, '2022-06-15', 'jan@siamtech-main.co.th'),
('อังคาร ลูกค้า', 'Sales', 'Account Manager', 48000.00, '2022-04-20', 'angkarn@siamtech-main.co.th'),

-- Management (2 people)
('พฤหัส บังคับได้', 'Management', 'CEO', 150000.00, '2022-01-01', 'ceo@siamtech-main.co.th'),
('ศุกร์ วางแผนดี', 'Management', 'CTO', 120000.00, '2022-01-01', 'cto@siamtech-main.co.th');

-- Insert Company A projects (Active projects)
INSERT INTO projects (name, client, budget, status, start_date, end_date, tech_stack) VALUES
('ระบบ CRM สำหรับธนาคาร', 'ธนาคารกรุงเทพ', 3000000.00, 'active', '2024-01-01', '2024-08-01', 'React, Node.js, AWS, PostgreSQL'),
('AI Chatbot E-commerce', 'Central Group', 1200000.00, 'active', '2024-03-01', '2024-07-01', 'Python, AWS Bedrock, Claude AI'),
('Mobile Banking App', 'ธนาคารไทยพาณิชย์', 2500000.00, 'active', '2024-02-15', '2024-09-15', 'React Native, AWS, Microservices'),
('เว็บไซต์ E-learning', 'จุฬาลงกรณ์มหาวิทยาลัย', 800000.00, 'completed', '2023-10-01', '2024-01-31', 'Vue.js, Laravel, MySQL');

-- Insert employee-project relationships (Company A)
INSERT INTO employee_projects (employee_id, project_id, role, allocation) VALUES
-- CRM Project (Large team)
(1, 1, 'Lead Developer', 0.8),
(2, 1, 'Frontend Developer', 1.0),
(3, 1, 'DevOps Engineer', 0.6),
(4, 1, 'Full Stack Developer', 0.7),
(10, 1, 'Solution Architect', 0.4),

-- AI Chatbot (AI focused)
(9, 2, 'AI Developer', 1.0),
(1, 2, 'Backend Integration', 0.2),
(3, 2, 'Cloud Infrastructure', 0.4),

-- Mobile Banking (Mobile focused)
(4, 3, 'Mobile Lead', 0.3),
(5, 3, 'Backend Developer', 0.8),
(6, 3, 'UI/UX Designer', 1.0),
(8, 3, 'Network Security', 0.5);

-- Create indexes for better performance
CREATE INDEX idx_employees_department_a ON employees(department);
CREATE INDEX idx_employees_position_a ON employees(position);
CREATE INDEX idx_projects_status_a ON projects(status);
CREATE INDEX idx_projects_client_a ON projects(client);

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

-- Grant permissions (if needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;

-- Company A specific information
COMMENT ON DATABASE siamtech_company_a IS 'SiamTech Main Office Database - Bangkok';
COMMENT ON TABLE employees IS 'พนักงานสำนักงานใหญ่ กรุงเทพฯ';
COMMENT ON TABLE projects IS 'โปรเจคหลักของบริษัท';

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
('SiamTech Main Office', 'Head Office', 'Bangkok, Thailand', 15, '2022-01-01', 'สำนักงานใหญ่ของบริษัท สยามเทค จำกัด ตั้งอยู่ในกรุงเทพมหานคร เป็นศูนย์กลางการดำเนินงานหลักของบริษัท');

-- Final check
SELECT 'Company A Database Setup Complete' as status,
       (SELECT COUNT(*) FROM employees) as total_employees,
       (SELECT COUNT(*) FROM projects) as total_projects;