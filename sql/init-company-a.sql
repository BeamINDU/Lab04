-- Company A (Siamtemp Main Office) Database Schema
-- กรุงเทพมหานคร - สำนักงานใหญ่

-- Create database
CREATE DATABASE siamtemp_company_a;
\c siamtemp_company_a;

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
    temp_stack VARCHAR(500)
);

-- Create employee_projects junction table
CREATE TABLE employee_projects (
    employee_id INTEGER REFERENCES employees(id),
    project_id INTEGER REFERENCES projects(id),
    role VARCHAR(100) NOT NULL,
    allocation DECIMAL(3,2) NOT NULL CHECK (allocation BETWEEN 0 AND 1),
    PRIMARY KEY (employee_id, project_id)
);

CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    manager_id INTEGER,
    budget DECIMAL(12,2) DEFAULT 0,
    location VARCHAR(100),
    established_date DATE DEFAULT CURRENT_DATE,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Clients Table (ข้อมูลลูกค้า)
CREATE TABLE clients (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    industry VARCHAR(100),
    contact_person VARCHAR(100),
    email VARCHAR(150),
    phone VARCHAR(20),
    address TEXT,
    website VARCHAR(200),
    contract_value DECIMAL(15,2) DEFAULT 0,
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'prospect', 'churned')),
    first_project_date DATE,
    last_contact_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Skills Table (ทักษะของพนักงาน)
CREATE TABLE skills (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    category VARCHAR(50), -- 'tempnical', 'soft', 'language', 'certification'
    description TEXT,
    is_active BOOLEAN DEFAULT true
);

-- 4. Employee Skills (ความสัมพันธ์ระหว่างพนักงานและทักษะ)
CREATE TABLE employee_skills (
    employee_id INTEGER REFERENCES employees(id) ON DELETE CASCADE,
    skill_id INTEGER REFERENCES skills(id) ON DELETE CASCADE,
    proficiency_level INTEGER CHECK (proficiency_level BETWEEN 1 AND 5), -- 1=Beginner, 5=Expert
    acquired_date DATE DEFAULT CURRENT_DATE,
    certified BOOLEAN DEFAULT false,
    certification_date DATE,
    PRIMARY KEY (employee_id, skill_id)
);

-- 5. Timesheets (การบันทึกเวลาทำงาน)
CREATE TABLE timesheets (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES employees(id) ON DELETE CASCADE,
    project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
    work_date DATE NOT NULL,
    hours_worked DECIMAL(4,2) NOT NULL CHECK (hours_worked >= 0 AND hours_worked <= 24),
    task_description TEXT,
    billable BOOLEAN DEFAULT true,
    hourly_rate DECIMAL(8,2),
    status VARCHAR(20) DEFAULT 'draft' CHECK (status IN ('draft', 'submitted', 'approved', 'rejected')),
    submitted_at TIMESTAMP,
    approved_by INTEGER REFERENCES employees(id),
    approved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. Expenses (ค่าใช้จ่ายโปรเจค)
CREATE TABLE expenses (
    id SERIAL PRIMARY KEY,
    project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
    employee_id INTEGER REFERENCES employees(id) ON DELETE CASCADE,
    category VARCHAR(50), -- 'travel', 'equipment', 'software', 'training', 'other'
    description TEXT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    expense_date DATE NOT NULL,
    receipt_url VARCHAR(500),
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected', 'reimbursed')),
    approved_by INTEGER REFERENCES employees(id),
    approved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 7. Meetings (การประชุม)
CREATE TABLE meetings (
    id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    meeting_date TIMESTAMP NOT NULL,
    duration_minutes INTEGER DEFAULT 60,
    location VARCHAR(200),
    meeting_type VARCHAR(50) DEFAULT 'internal', -- 'internal', 'client', 'vendor', 'training'
    status VARCHAR(20) DEFAULT 'scheduled' CHECK (status IN ('scheduled', 'completed', 'cancelled', 'rescheduled')),
    organizer_id INTEGER REFERENCES employees(id),
    project_id INTEGER REFERENCES projects(id),
    client_id INTEGER REFERENCES clients(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 8. Meeting Attendees (ผู้เข้าร่วมประชุม)
CREATE TABLE meeting_attendees (
    meeting_id INTEGER REFERENCES meetings(id) ON DELETE CASCADE,
    employee_id INTEGER REFERENCES employees(id) ON DELETE CASCADE,
    attendance_status VARCHAR(20) DEFAULT 'invited' CHECK (attendance_status IN ('invited', 'confirmed', 'attended', 'absent')),
    role VARCHAR(50) DEFAULT 'participant', -- 'organizer', 'presenter', 'participant'
    notes TEXT,
    PRIMARY KEY (meeting_id, employee_id)
);

-- 9. Equipment (อุปกรณ์และทรัพย์สิน)
CREATE TABLE equipment (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(100), -- 'laptop', 'desktop', 'monitor', 'phone', 'software_license'
    brand VARCHAR(100),
    model VARCHAR(100),
    serial_number VARCHAR(100) UNIQUE,
    purchase_date DATE,
    purchase_price DECIMAL(10,2),
    warranty_expiry DATE,
    status VARCHAR(20) DEFAULT 'available' CHECK (status IN ('available', 'assigned', 'maintenance', 'retired')),
    assigned_to INTEGER REFERENCES employees(id),
    assigned_date DATE,
    location VARCHAR(200),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 10. Training (การฝึกอบรม)
CREATE TABLE training (
    id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    provider VARCHAR(200),
    category VARCHAR(100), -- 'tempnical', 'soft_skills', 'certification', 'compliance'
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    duration_hours INTEGER,
    cost DECIMAL(10,2) DEFAULT 0,
    max_participants INTEGER,
    location VARCHAR(200),
    training_type VARCHAR(50) DEFAULT 'classroom', -- 'classroom', 'online', 'workshop', 'conference'
    status VARCHAR(20) DEFAULT 'planned' CHECK (status IN ('planned', 'ongoing', 'completed', 'cancelled')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 11. Training Attendees (ผู้เข้าอบรม)
CREATE TABLE training_attendees (
    training_id INTEGER REFERENCES training(id) ON DELETE CASCADE,
    employee_id INTEGER REFERENCES employees(id) ON DELETE CASCADE,
    registration_date DATE DEFAULT CURRENT_DATE,
    attendance_status VARCHAR(20) DEFAULT 'registered' CHECK (attendance_status IN ('registered', 'attended', 'completed', 'cancelled')),
    completion_date DATE,
    score DECIMAL(5,2), -- คะแนนจากการอบรม
    certificate_issued BOOLEAN DEFAULT false,
    certificate_url VARCHAR(500),
    feedback TEXT,
    PRIMARY KEY (training_id, employee_id)
);
-- Insert Company A employees (Main Office - 15 people)
INSERT INTO employees (name, department, position, salary, hire_date, email) VALUES
-- IT Department (10 people)
('สมชาย ใจดี', 'IT', 'Senior Developer', 65000.00, '2022-01-15', 'somchai@siamtemp-main.co.th'),
('สมหญิง รักงาน', 'IT', 'Frontend Developer', 50000.00, '2022-03-20', 'somying@siamtemp-main.co.th'),
('ประสิทธิ์ เก่งมาก', 'IT', 'DevOps Engineer', 70000.00, '2021-11-10', 'prasit@siamtemp-main.co.th'),
('วิชัย โค้ดดี', 'IT', 'Full Stack Developer', 55000.00, '2022-04-01', 'wichai@siamtemp-main.co.th'),
('มาลี เขียนโค้ด', 'IT', 'Backend Developer', 52000.00, '2022-05-15', 'malee@siamtemp-main.co.th'),
('สุรชัย ดีไซน์', 'IT', 'UI/UX Designer', 45000.00, '2022-06-01', 'surachai@siamtemp-main.co.th'),
('นิรันดร์ ทดสอบ', 'IT', 'QA Engineer', 42000.00, '2022-07-10', 'niran@siamtemp-main.co.th'),
('อำนาจ เน็ตเวิร์ก', 'IT', 'Network Engineer', 58000.00, '2022-02-20', 'amnaj@siamtemp-main.co.th'),
('ชญานิษฐ์ เอไอ', 'IT', 'AI Developer', 72000.00, '2022-01-05', 'chayanit@siamtemp-main.co.th'),
('ภาณุวัฒน์ อาร์คิเทค', 'IT', 'Solution Architect', 80000.00, '2021-10-15', 'phanuwat@siamtemp-main.co.th'),

-- Sales Department (3 people)
('อาทิตย์ ขายเก่ง', 'Sales', 'Sales Manager', 60000.00, '2022-02-01', 'athit@siamtemp-main.co.th'),
('จันทร์ ดูแลดี', 'Sales', 'Sales Executive', 40000.00, '2022-06-15', 'jan@siamtemp-main.co.th'),
('อังคาร ลูกค้า', 'Sales', 'Account Manager', 48000.00, '2022-04-20', 'angkarn@siamtemp-main.co.th'),

-- Management (2 people)
('พฤหัส บังคับได้', 'Management', 'CEO', 150000.00, '2022-01-01', 'ceo@siamtemp-main.co.th'),
('ศุกร์ วางแผนดี', 'Management', 'CTO', 120000.00, '2022-01-01', 'cto@siamtemp-main.co.th');

-- Insert Company A projects (Active projects)
INSERT INTO projects (name, client, budget, status, start_date, end_date, temp_stack) VALUES
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
INSERT INTO departments (name, description, manager_id, budget, location) VALUES
('Information tempnology', 'พัฒนาและดูแลระบบเทคโนโลยี', 1, 5000000.00, 'Floor 3'),
('Sales & Marketing', 'ขายและการตลาด', 11, 2000000.00, 'Floor 2'),
('Human Resources', 'ทรัพยากรบุคคล', NULL, 800000.00, 'Floor 1'),
('Management', 'ผู้บริหาร', 14, 1000000.00, 'Floor 4'),
('Research & Development', 'วิจัยและพัฒนา', 9, 3000000.00, 'Floor 3');

-- Insert Clients
INSERT INTO clients (name, industry, contact_person, email, phone, address, website, contract_value, first_project_date) VALUES
('ธนาคารกรุงเทพ', 'Banking', 'คุณสมชาย วงศ์ทอง', 'somchai@bangkokbank.com', '02-234-5678', 'กรุงเทพมหานคร', 'https://bangkokbank.com', 3000000.00, '2024-01-01'),
('Central Group', 'Retail', 'คุณสุภา จิรกิตติ์', 'supha@central.co.th', '02-345-6789', 'กรุงเทพมหานคร', 'https://central.co.th', 1200000.00, '2024-03-01'),
('ธนาคารไทยพาณิชย์', 'Banking', 'คุณวิชัย ธนกิจ', 'wichai@scb.co.th', '02-456-7890', 'กรุงเทพมหานคร', 'https://scb.co.th', 2500000.00, '2024-02-15'),
('จุฬาลงกรณ์มหาวิทยาลัย', 'Education', 'ผศ.ดร.นพดล สิทธิ์ดี', 'napadol@chula.ac.th', '02-567-8901', 'กรุงเทพมหานคร', 'https://chula.ac.th', 800000.00, '2023-10-01'),
('CP Group', 'Conglomerate', 'คุณธนา เจริญโชค', 'thana@cp.co.th', '02-678-9012', 'กรุงเทพมหานคร', 'https://cp.co.th', 1800000.00, '2024-01-20');

-- Insert Skills
INSERT INTO skills (name, category, description) VALUES
-- tempnical Skills
('React', 'tempnical', 'Frontend JavaScript library'),
('Node.js', 'tempnical', 'Backend JavaScript runtime'),
('Python', 'tempnical', 'Programming language'),
('PostgreSQL', 'tempnical', 'Database management'),
('AWS', 'tempnical', 'Cloud computing platform'),
('Docker', 'tempnical', 'Containerization platform'),
('Git', 'tempnical', 'Version control system'),
('JavaScript', 'tempnical', 'Programming language'),
('TypeScript', 'tempnical', 'Typed JavaScript'),
('Vue.js', 'tempnical', 'Frontend JavaScript framework'),

-- Soft Skills
('Project Management', 'soft', 'Managing projects and teams'),
('Communication', 'soft', 'Effective communication skills'),
('Leadership', 'soft', 'Leading teams and initiatives'),
('Problem Solving', 'soft', 'Analytical and creative problem solving'),
('Time Management', 'soft', 'Efficient time and priority management'),

-- Languages
('English', 'language', 'English language proficiency'),
('Thai', 'language', 'Thai language proficiency'),
('Japanese', 'language', 'Japanese language proficiency'),

-- Certifications
('AWS Certified Solutions Architect', 'certification', 'AWS cloud certification'),
('PMP', 'certification', 'Project Management Professional'),
('Scrum Master', 'certification', 'Agile Scrum certification');

-- Insert Employee Skills (เฉพาะบางคน)
INSERT INTO employee_skills (employee_id, skill_id, proficiency_level, certified) VALUES
-- สมชาย ใจดี (Senior Developer)
(1, 1, 5, false), -- React
(1, 2, 5, false), -- Node.js
(1, 8, 5, false), -- JavaScript
(1, 11, 4, false), -- Project Management
(1, 16, 4, false), -- English

-- สมหญิง รักงาน (Frontend Developer)
(2, 1, 4, false), -- React
(2, 8, 4, false), -- JavaScript
(2, 9, 3, false), -- TypeScript
(2, 10, 3, false), -- Vue.js

-- ประสิทธิ์ เก่งมาก (DevOps Engineer)
(3, 5, 5, true), -- AWS
(3, 6, 5, false), -- Docker
(3, 7, 4, false), -- Git
(3, 19, 5, true); -- AWS Certified Solutions Architect

-- Insert Sample Timesheets (เฉพาะสัปดาห์ที่แล้ว)
INSERT INTO timesheets (employee_id, project_id, work_date, hours_worked, task_description, billable, hourly_rate, status) VALUES
-- สมชาย - CRM Project
(1, 1, CURRENT_DATE - INTERVAL '7 days', 8.0, 'Backend API development', true, 800.00, 'approved'),
(1, 1, CURRENT_DATE - INTERVAL '6 days', 8.0, 'Database optimization', true, 800.00, 'approved'),
(1, 1, CURRENT_DATE - INTERVAL '5 days', 7.5, 'Code review and testing', true, 800.00, 'approved'),

-- สมหญิง - CRM Project
(2, 1, CURRENT_DATE - INTERVAL '7 days', 8.0, 'Frontend component development', true, 650.00, 'approved'),
(2, 1, CURRENT_DATE - INTERVAL '6 days', 8.0, 'UI/UX implementation', true, 650.00, 'approved'),

-- ชญานิษฐ์ - AI Chatbot
(9, 2, CURRENT_DATE - INTERVAL '7 days', 8.0, 'AI model training', true, 900.00, 'submitted'),
(9, 2, CURRENT_DATE - INTERVAL '6 days', 7.0, 'Claude API integration', true, 900.00, 'submitted');

-- Insert Sample Expenses
INSERT INTO expenses (project_id, employee_id, category, description, amount, expense_date, status) VALUES
(1, 1, 'software', 'AWS cloud services - monthly', 15000.00, CURRENT_DATE - INTERVAL '5 days', 'approved'),
(1, 3, 'equipment', 'External monitor for development', 12000.00, CURRENT_DATE - INTERVAL '10 days', 'approved'),
(2, 9, 'training', 'Claude AI certification course', 8000.00, CURRENT_DATE - INTERVAL '3 days', 'pending');

-- Insert Sample Meetings
INSERT INTO meetings (title, description, meeting_date, duration_minutes, location, meeting_type, organizer_id, project_id, client_id) VALUES
('CRM Project Kickoff', 'เริ่มต้นโปรเจค CRM ของธนาคาร', CURRENT_TIMESTAMP + INTERVAL '2 days', 120, 'Conference Room A', 'client', 1, 1, 1),
('Weekly Stand-up', 'ประชุมรายสัปดาห์ทีม IT', CURRENT_TIMESTAMP + INTERVAL '1 day', 30, 'IT Office', 'internal', 1, NULL, NULL),
('AI Training Session', 'อบรมการใช้ Claude AI', CURRENT_TIMESTAMP + INTERVAL '5 days', 180, 'Training Room', 'training', 9, NULL, NULL);

-- Insert Meeting Attendees
INSERT INTO meeting_attendees (meeting_id, employee_id, attendance_status, role) VALUES
-- CRM Kickoff Meeting
(1, 1, 'confirmed', 'presenter'),
(1, 2, 'confirmed', 'participant'),
(1, 3, 'confirmed', 'participant'),
(1, 11, 'confirmed', 'participant'),

-- Weekly Stand-up
(2, 1, 'confirmed', 'organizer'),
(2, 2, 'confirmed', 'participant'),
(2, 3, 'confirmed', 'participant'),
(2, 4, 'confirmed', 'participant'),
(2, 5, 'confirmed', 'participant');

-- Insert Sample Equipment
INSERT INTO equipment (name, category, brand, model, serial_number, purchase_date, purchase_price, status, assigned_to, location) VALUES
('MacBook Pro 16"', 'laptop', 'Apple', 'MBP16-2024', 'MBP001BKK', '2024-01-15', 89000.00, 'assigned', 1, 'IT Department'),
('Dell Monitor 27"', 'monitor', 'Dell', 'U2723QE', 'DLL001BKK', '2024-01-20', 18000.00, 'assigned', 1, 'IT Department'),
('ThinkPad X1 Carbon', 'laptop', 'Lenovo', 'X1C-2024', 'LNV001BKK', '2024-02-01', 65000.00, 'assigned', 2, 'IT Department'),
('AWS License', 'software_license', 'Amazon', 'Professional', 'AWS-BKK-001', '2024-01-01', 120000.00, 'assigned', 3, 'Cloud Infrastructure'),
('JetBrains Suite', 'software_license', 'JetBrains', 'All Products', 'JB-BKK-001', '2024-01-01', 25000.00, 'available', NULL, 'Software Licenses');

-- Insert Sample Training
INSERT INTO training (title, description, provider, category, start_date, end_date, duration_hours, cost, max_participants, location, training_type) VALUES
('AWS Solutions Architect Certification', 'การอบรม AWS สำหรับการออกแบบ Cloud Architecture', 'AWS Training Center', 'certification', '2024-08-01', '2024-08-03', 24, 45000.00, 10, 'AWS Training Center Bangkok', 'classroom'),
('Agile Project Management', 'การจัดการโปรเจคแบบ Agile และ Scrum', 'Agile Institute Thailand', 'soft_skills', '2024-07-15', '2024-07-17', 21, 30000.00, 15, 'Training Room B', 'workshop'),
('Advanced React Development', 'React เชิงลึกสำหรับ Enterprise Applications', 'temp Academy Thailand', 'tempnical', '2024-08-10', '2024-08-12', 18, 35000.00, 12, 'Online', 'online');

-- Insert Training Attendees
INSERT INTO training_attendees (training_id, employee_id, registration_date, attendance_status) VALUES
-- AWS Training
(1, 3, '2024-07-01', 'registered'), -- ประสิทธิ์
(1, 10, '2024-07-01', 'registered'), -- ณัฐพล (Cloud Engineer)

-- Agile Training
(2, 1, '2024-06-15', 'registered'), -- สมชาย
(2, 11, '2024-06-15', 'registered'), -- อาทิตย์ (Sales Manager)
(2, 15, '2024-06-15', 'registered'), -- ภาณุวัฒน์ (Solution Architect)

-- React Training
(3, 2, '2024-07-10', 'registered'), -- สมหญิง
(3, 4, '2024-07-10', 'registered'); -- วิชัย

-- =============================================================================
-- เพิ่ม Indexes สำหรับประสิทธิภาพ
-- =============================================================================

-- Departments
CREATE INDEX idx_departments_manager_id ON departments(manager_id);
CREATE INDEX idx_departments_is_active ON departments(is_active);

-- Clients
CREATE INDEX idx_clients_status ON clients(status);
CREATE INDEX idx_clients_industry ON clients(industry);
CREATE INDEX idx_clients_name ON clients(name);

-- Skills
CREATE INDEX idx_skills_category ON skills(category);
CREATE INDEX idx_skills_is_active ON skills(is_active);

-- Employee Skills
CREATE INDEX idx_employee_skills_proficiency ON employee_skills(proficiency_level);
CREATE INDEX idx_employee_skills_certified ON employee_skills(certified);

-- Timesheets
CREATE INDEX idx_timesheets_work_date ON timesheets(work_date);
CREATE INDEX idx_timesheets_status ON timesheets(status);
CREATE INDEX idx_timesheets_billable ON timesheets(billable);
CREATE INDEX idx_timesheets_employee_project ON timesheets(employee_id, project_id);

-- Expenses
CREATE INDEX idx_expenses_category ON expenses(category);
CREATE INDEX idx_expenses_status ON expenses(status);
CREATE INDEX idx_expenses_expense_date ON expenses(expense_date);

-- Meetings
CREATE INDEX idx_meetings_meeting_date ON meetings(meeting_date);
CREATE INDEX idx_meetings_status ON meetings(status);
CREATE INDEX idx_meetings_meeting_type ON meetings(meeting_type);

-- Equipment
CREATE INDEX idx_equipment_status ON equipment(status);
CREATE INDEX idx_equipment_category ON equipment(category);
CREATE INDEX idx_equipment_assigned_to ON equipment(assigned_to);

-- Training
CREATE INDEX idx_training_start_date ON training(start_date);
CREATE INDEX idx_training_status ON training(status);
CREATE INDEX idx_training_category ON training(category);

-- =============================================================================
-- เพิ่ม Views ใหม่
-- =============================================================================

-- Employee Performance View
CREATE VIEW employee_performance AS
SELECT 
    e.id,
    e.name,
    e.department,
    e.position,
    COUNT(DISTINCT ep.project_id) as projects_count,
    COALESCE(SUM(t.hours_worked), 0) as total_hours_this_month,
    COALESCE(AVG(ta.score), 0) as avg_training_score,
    COUNT(DISTINCT es.skill_id) as skills_count,
    COUNT(DISTINCT eq.id) as equipment_assigned
FROM employees e
LEFT JOIN employee_projects ep ON e.id = ep.employee_id
LEFT JOIN timesheets t ON e.id = t.employee_id 
    AND t.work_date >= date_trunc('month', CURRENT_DATE)
LEFT JOIN training_attendees ta ON e.id = ta.employee_id
    AND ta.score IS NOT NULL
LEFT JOIN employee_skills es ON e.id = es.employee_id
LEFT JOIN equipment eq ON e.id = eq.assigned_to
GROUP BY e.id, e.name, e.department, e.position;

-- Project Financial Summary
CREATE VIEW project_financial_summary AS
SELECT 
    p.id,
    p.name,
    p.client,
    p.budget,
    p.status,
    COALESCE(SUM(t.hours_worked * t.hourly_rate), 0) as labor_cost,
    COALESCE(SUM(ex.amount), 0) as expenses_total,
    COALESCE(SUM(t.hours_worked * t.hourly_rate), 0) + COALESCE(SUM(ex.amount), 0) as total_cost,
    p.budget - (COALESCE(SUM(t.hours_worked * t.hourly_rate), 0) + COALESCE(SUM(ex.amount), 0)) as remaining_budget,
    COUNT(DISTINCT ep.employee_id) as team_size
FROM projects p
LEFT JOIN timesheets t ON p.id = t.project_id AND t.status = 'approved'
LEFT JOIN expenses ex ON p.id = ex.project_id AND ex.status = 'approved'
LEFT JOIN employee_projects ep ON p.id = ep.project_id
GROUP BY p.id, p.name, p.client, p.budget, p.status;

-- Department Statistics
CREATE VIEW department_statistics AS
SELECT 
    d.id,
    d.name,
    d.budget,
    COUNT(e.id) as employee_count,
    AVG(e.salary) as avg_salary,
    SUM(e.salary) as total_salary,
    COUNT(DISTINCT eq.id) as equipment_count,
    COUNT(DISTINCT t.id) as training_count
FROM departments d
LEFT JOIN employees e ON d.name = e.department
LEFT JOIN equipment eq ON e.id = eq.assigned_to
LEFT JOIN training_attendees ta ON e.id = ta.employee_id
LEFT JOIN training t ON ta.training_id = t.id
WHERE d.is_active = true
GROUP BY d.id, d.name, d.budget;

-- Client Engagement Summary
CREATE VIEW client_engagement_summary AS
SELECT 
    c.id,
    c.name,
    c.industry,
    c.status,
    c.contract_value,
    COUNT(p.id) as projects_count,
    SUM(p.budget) as total_project_budget,
    COUNT(m.id) as meetings_count,
    MAX(m.meeting_date) as last_meeting_date,
    CASE 
        WHEN MAX(m.meeting_date) > CURRENT_DATE - INTERVAL '30 days' THEN 'Active'
        WHEN MAX(m.meeting_date) > CURRENT_DATE - INTERVAL '90 days' THEN 'Recent'
        ELSE 'Inactive'
    END as engagement_level
FROM clients c
LEFT JOIN projects p ON c.id = p.client
LEFT JOIN meetings m ON c.id = m.client_id
GROUP BY c.id, c.name, c.industry, c.status, c.contract_value;

-- =============================================================================
-- เพิ่ม Foreign Key Constraints ที่ขาดหายไป
-- =============================================================================

-- เพิ่ม foreign key สำหรับ departments.manager_id
ALTER TABLE departments 
ADD CONSTRAINT fk_departments_manager 
FOREIGN KEY (manager_id) REFERENCES employees(id);

-- Update ข้อมูลโครงสร้างองกรณ์
UPDATE employees SET department = 'Information tempnology' WHERE department = 'IT';
UPDATE employees SET department = 'Sales & Marketing' WHERE department = 'Sales';
UPDATE employees SET department = 'Human Resources' WHERE department = 'HR';
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
COMMENT ON DATABASE siamtemp_company_a IS 'Siamtemp Main Office Database - Bangkok';
COMMENT ON TABLE employees IS 'พนักงานสำนักงานใหญ่ กรุงเทพฯ';
COMMENT ON TABLE projects IS 'โปรเจคหลักของบริษัท';
COMMENT ON TABLE departments IS 'แผนกต่างๆ ในบริษัท';
COMMENT ON TABLE clients IS 'ข้อมูลลูกค้าของบริษัท';
COMMENT ON TABLE skills IS 'ทักษะและความสามารถต่างๆ';
COMMENT ON TABLE employee_skills IS 'ทักษะของพนักงานแต่ละคน';
COMMENT ON TABLE timesheets IS 'การบันทึกเวลาทำงานของพนักงาน';
COMMENT ON TABLE expenses IS 'ค่าใช้จ่ายในโปรเจค';
COMMENT ON TABLE meetings IS 'การประชุมต่างๆ';
COMMENT ON TABLE meeting_attendees IS 'ผู้เข้าร่วมประชุม';
COMMENT ON TABLE equipment IS 'อุปกรณ์และทรัพย์สินของบริษัท';
COMMENT ON TABLE training IS 'หลักสูตรอบรมต่างๆ';
COMMENT ON TABLE training_attendees IS 'ผู้เข้าร่วมอบรม';
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
('Siamtemp Main Office', 'Head Office', 'Bangkok, Thailand', 15, '2022-01-01', 'สำนักงานใหญ่ของบริษัท สยามเทค จำกัด ตั้งอยู่ในกรุงเทพมหานคร เป็นศูนย์กลางการดำเนินงานหลักของบริษัท');

-- Final check
SELECT 'Enhanced Company A Database Setup Complete' as status,
       (SELECT COUNT(*) FROM employees) as total_employees,
       (SELECT COUNT(*) FROM projects) as total_projects,
       (SELECT COUNT(*) FROM clients) as total_clients,
       (SELECT COUNT(*) FROM departments) as total_departments,
       (SELECT COUNT(*) FROM skills) as total_skills;