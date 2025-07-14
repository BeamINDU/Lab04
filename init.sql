-- Create database \i /docker-entrypoint-initdb.d/init-company-a.sql docker exec -it chat-postgres-1 psql -U postgres -d siamtech
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

-- Create projects table (same as before)
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

-- Insert 25 employees to match Knowledge Base info
-- IT Department (15 people)
INSERT INTO employees (name, department, position, salary, hire_date, email) VALUES
('สมชาย ใจดี', 'IT', 'Senior Developer', 55000.00, '2022-01-15', 'somchai@siamtech.co.th'),
('สมหญิง รักงาน', 'IT', 'Frontend Developer', 45000.00, '2022-03-20', 'somying@siamtech.co.th'),
('ประสิทธิ์ เก่งมาก', 'IT', 'DevOps Engineer', 60000.00, '2021-11-10', 'prasit@siamtech.co.th'),
('วิชัย โค้ดดี', 'IT', 'Full Stack Developer', 48000.00, '2022-04-01', 'wichai@siamtech.co.th'),
('มาลี เขียนโค้ด', 'IT', 'Backend Developer', 46000.00, '2022-05-15', 'malee@siamtech.co.th'),
('สุรชัย ดีไซน์', 'IT', 'UI/UX Designer', 42000.00, '2022-06-01', 'surachai@siamtech.co.th'),
('นิรันดร์ ทดสอบ', 'IT', 'QA Engineer', 40000.00, '2022-07-10', 'niran@siamtech.co.th'),
('อำนาจ เน็ตเวิร์ก', 'IT', 'Network Engineer', 50000.00, '2022-02-20', 'amnaj@siamtech.co.th'),
('สุพิชญา ดาต้า', 'IT', 'Data Analyst', 47000.00, '2022-08-05', 'supichya@siamtech.co.th'),
('ธนากร ระบบ', 'IT', 'System Admin', 52000.00, '2022-01-25', 'thanakorn@siamtech.co.th'),
('อภิชาติ ปลอดภัย', 'IT', 'Security Engineer', 58000.00, '2021-12-01', 'aphichat@siamtech.co.th'),
('ศุภชัย โมบาย', 'IT', 'Mobile Developer', 49000.00, '2022-03-10', 'supchai@siamtech.co.th'),
('ณัฐพล คลาวด์', 'IT', 'Cloud Engineer', 56000.00, '2022-02-14', 'natthaphon@siamtech.co.th'),
('ชญานิษฐ์ เอไอ', 'IT', 'AI Developer', 62000.00, '2022-01-05', 'chayanit@siamtech.co.th'),
('ภาณุวัฒน์ อาร์คิเทค', 'IT', 'Solution Architect', 70000.00, '2021-10-15', 'phanuwat@siamtech.co.th'),

-- Sales Department (5 people)
('อาทิตย์ ขายเก่ง', 'Sales', 'Sales Manager', 50000.00, '2022-02-01', 'athit@siamtech.co.th'),
('จันทร์ ดูแลดี', 'Sales', 'Sales Executive', 35000.00, '2022-06-15', 'jan@siamtech.co.th'),
('อังคาร ลูกค้า', 'Sales', 'Account Manager', 42000.00, '2022-04-20', 'angkarn2@siamtech.co.th'),
('พุธ ขายดี', 'Sales', 'Sales Representative', 32000.00, '2022-07-01', 'put2@siamtech.co.th'),
('พฤหัส การตลาด', 'Sales', 'Marketing Specialist', 38000.00, '2022-05-10', 'preuhat2@siamtech.co.th'),

-- HR Department (3 people)
('อังคาร บริการดี', 'HR', 'HR Manager', 48000.00, '2022-01-10', 'angkarn@siamtech.co.th'),
('พุธ จัดการเก่ง', 'HR', 'HR Officer', 32000.00, '2022-04-01', 'put@siamtech.co.th'),
('ศุกร์ สรรหา', 'HR', 'Recruitment Specialist', 35000.00, '2022-08-15', 'suk2@siamtech.co.th'),

-- Management (2 people)
('พฤหัส บังคับได้', 'Management', 'CEO', 120000.00, '2022-01-01', 'preuhat@siamtech.co.th'),
('ศุกร์ วางแผนดี', 'Management', 'CTO', 100000.00, '2022-01-01', 'suk@siamtech.co.th');

-- Insert projects to match Knowledge Base info
INSERT INTO projects (name, client, budget, status, start_date, end_date, tech_stack) VALUES
-- Active projects from Knowledge Base
('ระบบ CRM สำหรับธนาคาร', 'ธนาคารกรุงเทพ', 2500000.00, 'active', '2024-01-01', '2024-07-01', 'React, Node.js, AWS'),
('Chatbot AI สำหรับ E-commerce', 'บริษัท ช้อปดี', 800000.00, 'active', '2024-04-01', '2024-07-01', 'Python, AWS Bedrock, Claude'),
('Mobile App สำหรับ Food Delivery', 'เดลิเวอรี่ เฟส', 1200000.00, 'active', '2024-03-01', '2024-08-01', 'React Native, Firebase'),

-- Completed projects
('เว็บไซต์บริษัท SiamTech', 'SiamTech (Internal)', 300000.00, 'completed', '2023-11-01', '2023-12-15', 'Next.js, Tailwind CSS, Vercel'),
('ระบบ HR Management', 'บริษัท ทำงานดี', 1500000.00, 'completed', '2023-08-01', '2023-12-31', 'Vue.js, Laravel, MySQL'),

-- Additional projects to match company scale
('ระบบ ERP สำหรับ SME', 'บริษัท ธุรกิจดี', 1800000.00, 'active', '2024-02-01', '2024-09-01', 'React, Node.js, PostgreSQL'),
('เว็บแอป E-learning', 'มหาวิทยาลัยเทค', 950000.00, 'active', '2024-03-15', '2024-08-15', 'Vue.js, Express, MongoDB');

-- Insert employee-project relationships (matching 25 employees)
INSERT INTO employee_projects (employee_id, project_id, role, allocation) VALUES
-- CRM Project (id: 1) - Large project, many people
(1, 1, 'Lead Developer', 0.8),
(2, 1, 'Frontend Developer', 1.0),
(3, 1, 'DevOps Engineer', 0.5),
(4, 1, 'Full Stack Developer', 0.7),
(8, 1, 'Network Engineer', 0.3),

-- Chatbot Project (id: 2) - AI focused
(14, 2, 'AI Developer', 1.0),
(1, 2, 'Backend Developer', 0.2),
(3, 2, 'Cloud Engineer', 0.3),
(13, 2, 'Cloud Engineer', 0.4),

-- Mobile App Project (id: 3) - Mobile focused
(12, 3, 'Mobile Developer', 1.0),
(2, 3, 'Frontend Developer', 0.0), -- finished
(3, 3, 'DevOps Engineer', 0.2),
(6, 3, 'UI/UX Designer', 0.8),

-- ERP Project (id: 6) - New large project
(4, 6, 'Lead Developer', 0.3),
(5, 6, 'Backend Developer', 1.0),
(15, 6, 'Solution Architect', 0.6),
(9, 6, 'Data Analyst', 0.8),
(10, 6, 'System Admin', 0.4),

-- E-learning Project (id: 7) - New project
(2, 7, 'Frontend Developer', 0.0), -- will start later
(6, 7, 'UI/UX Designer', 0.2),
(7, 7, 'QA Engineer', 1.0),
(11, 7, 'Security Engineer', 0.3),

-- Website Project (id: 4) - completed
(2, 4, 'Full Stack Developer', 0.0),
(6, 4, 'UI/UX Designer', 0.0),

-- HR System (id: 5) - completed  
(1, 5, 'Backend Developer', 0.0),
(5, 5, 'Backend Developer', 0.0);

-- Create indexes for better performance
CREATE INDEX idx_employees_department ON employees(department);
CREATE INDEX idx_employees_position ON employees(position);
CREATE INDEX idx_projects_status ON projects(status);
CREATE INDEX idx_projects_client ON projects(client);
CREATE INDEX idx_employee_projects_employee_id ON employee_projects(employee_id);
CREATE INDEX idx_employee_projects_project_id ON employee_projects(project_id);

-- Create view for easy department summary (matches Knowledge Base)
CREATE VIEW department_summary AS
SELECT 
    department,
    COUNT(*) as employee_count,
    AVG(salary) as avg_salary,
    MIN(hire_date) as earliest_hire,
    MAX(hire_date) as latest_hire
FROM employees 
GROUP BY department;

-- Create view for project summary
CREATE VIEW project_summary AS
SELECT 
    p.name as project_name,
    p.client,
    p.status,
    p.budget,
    COUNT(ep.employee_id) as team_size,
    STRING_AGG(e.name, ', ') as team_members
FROM projects p
LEFT JOIN employee_projects ep ON p.id = ep.project_id
LEFT JOIN employees e ON ep.employee_id = e.id
GROUP BY p.id, p.name, p.client, p.status, p.budget;