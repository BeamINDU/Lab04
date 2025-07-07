-- Create database
CREATE DATABASE siamtech;

-- Use database
\c siamtech;

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

-- Insert sample employees
INSERT INTO employees (name, department, position, salary, hire_date, email) VALUES
('สมชาย ใจดี', 'IT', 'Senior Developer', 55000.00, '2022-01-15', 'somchai@siamtech.co.th'),
('สมหญิง รักงาน', 'IT', 'Frontend Developer', 45000.00, '2022-03-20', 'somying@siamtech.co.th'),
('ประสิทธิ์ เก่งมาก', 'IT', 'DevOps Engineer', 60000.00, '2021-11-10', 'prasit@siamtech.co.th'),
('อาทิตย์ ขายเก่ง', 'Sales', 'Sales Manager', 50000.00, '2022-02-01', 'athit@siamtech.co.th'),
('จันทร์ ดูแลดี', 'Sales', 'Sales Executive', 35000.00, '2022-06-15', 'jan@siamtech.co.th'),
('อังคาร บริการดี', 'HR', 'HR Manager', 48000.00, '2022-01-10', 'angkarn@siamtech.co.th'),
('พุธ จัดการเก่ง', 'HR', 'HR Officer', 32000.00, '2022-04-01', 'put@siamtech.co.th'),
('พฤหัส บังคับได้', 'Management', 'CEO', 120000.00, '2022-01-01', 'preuhat@siamtech.co.th'),
('ศุกร์ วางแผนดี', 'Management', 'CTO', 100000.00, '2022-01-01', 'suk@siamtech.co.th');

-- Insert sample projects
INSERT INTO projects (name, client, budget, status, start_date, end_date, tech_stack) VALUES
('ระบบ CRM สำหรับธนาคาร', 'ธนาคารกรุงเทพ', 2500000.00, 'active', '2024-01-01', '2024-07-01', 'React, Node.js, PostgreSQL, AWS'),
('Chatbot AI สำหรับ E-commerce', 'บริษัท ช้อปดี', 800000.00, 'active', '2024-04-01', '2024-07-01', 'Python, AWS Bedrock, Claude, FastAPI'),
('Mobile App สำหรับ Food Delivery', 'เดลิเวอรี่ เฟส', 1200000.00, 'active', '2024-03-01', '2024-08-01', 'React Native, Firebase, Node.js'),
('เว็บไซต์บริษัท SiamTech', 'SiamTech (Internal)', 300000.00, 'completed', '2023-11-01', '2023-12-15', 'Next.js, Tailwind CSS, Vercel'),
('ระบบ HR Management', 'บริษัท ทำงานดี', 1500000.00, 'completed', '2023-08-01', '2023-12-31', 'Vue.js, Laravel, MySQL');

-- Insert employee-project relationships
INSERT INTO employee_projects (employee_id, project_id, role, allocation) VALUES
-- CRM Project (id: 1)
(1, 1, 'Lead Developer', 0.8),
(2, 1, 'Frontend Developer', 1.0),
(3, 1, 'DevOps Engineer', 0.5),

-- Chatbot Project (id: 2)
(1, 2, 'Backend Developer', 0.2),
(3, 2, 'Cloud Engineer', 0.3),

-- Mobile App Project (id: 3)
(2, 3, 'Mobile Developer', 0.0), -- finished allocation
(3, 3, 'DevOps Engineer', 0.2),

-- Website Project (id: 4) - completed
(2, 4, 'Full Stack Developer', 0.0),

-- HR System (id: 5) - completed
(1, 5, 'Backend Developer', 0.0);

-- Create indexes for better performance
CREATE INDEX idx_employees_department ON employees(department);
CREATE INDEX idx_projects_status ON projects(status);
CREATE INDEX idx_employee_projects_employee_id ON employee_projects(employee_id);
CREATE INDEX idx_employee_projects_project_id ON employee_projects(project_id);

-- Sample queries for testing
/*
-- Test queries:
SELECT COUNT(*) FROM employees;
SELECT COUNT(*) FROM employees WHERE department = 'IT';
SELECT AVG(salary) FROM employees;
SELECT AVG(salary) FROM employees WHERE department = 'IT';
SELECT * FROM projects WHERE status = 'active';
SELECT e.name, p.name as project_name, ep.role 
FROM employees e 
JOIN employee_projects ep ON e.id = ep.employee_id 
JOIN projects p ON ep.project_id = p.id 
WHERE p.name LIKE '%CRM%';
*/