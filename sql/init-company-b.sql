-- Company B (Siamtemp Regional Office) Database Schema
-- เชียงใหม่ - สาขาภาคเหนือ

-- Create database
CREATE DATABASE siamtemp_company_b;
\c siamtemp_company_b;

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

-- 2. Clients Table (ลูกค้าภาคเหนือ)
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
    region VARCHAR(50) DEFAULT 'Northern Thailand',
    first_project_date DATE,
    last_contact_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Skills Table
CREATE TABLE skills (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    category VARCHAR(50), -- 'tempnical', 'soft', 'language', 'certification'
    description TEXT,
    is_active BOOLEAN DEFAULT true
);

-- 4. Employee Skills
CREATE TABLE employee_skills (
    employee_id INTEGER REFERENCES employees(id) ON DELETE CASCADE,
    skill_id INTEGER REFERENCES skills(id) ON DELETE CASCADE,
    proficiency_level INTEGER CHECK (proficiency_level BETWEEN 1 AND 5),
    acquired_date DATE DEFAULT CURRENT_DATE,
    certified BOOLEAN DEFAULT false,
    certification_date DATE,
    PRIMARY KEY (employee_id, skill_id)
);

-- 5. Timesheets
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

-- 6. Expenses
CREATE TABLE expenses (
    id SERIAL PRIMARY KEY,
    project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
    employee_id INTEGER REFERENCES employees(id) ON DELETE CASCADE,
    category VARCHAR(50),
    description TEXT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    expense_date DATE NOT NULL,
    receipt_url VARCHAR(500),
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected', 'reimbursed')),
    approved_by INTEGER REFERENCES employees(id),
    approved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 7. Meetings
CREATE TABLE meetings (
    id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    meeting_date TIMESTAMP NOT NULL,
    duration_minutes INTEGER DEFAULT 60,
    location VARCHAR(200),
    meeting_type VARCHAR(50) DEFAULT 'internal',
    status VARCHAR(20) DEFAULT 'scheduled' CHECK (status IN ('scheduled', 'completed', 'cancelled', 'rescheduled')),
    organizer_id INTEGER REFERENCES employees(id),
    project_id INTEGER REFERENCES projects(id),
    client_id INTEGER REFERENCES clients(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 8. Meeting Attendees
CREATE TABLE meeting_attendees (
    meeting_id INTEGER REFERENCES meetings(id) ON DELETE CASCADE,
    employee_id INTEGER REFERENCES employees(id) ON DELETE CASCADE,
    attendance_status VARCHAR(20) DEFAULT 'invited' CHECK (attendance_status IN ('invited', 'confirmed', 'attended', 'absent')),
    role VARCHAR(50) DEFAULT 'participant',
    notes TEXT,
    PRIMARY KEY (meeting_id, employee_id)
);

-- 9. Equipment
CREATE TABLE equipment (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
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

-- 10. Training
CREATE TABLE training (
    id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    provider VARCHAR(200),
    category VARCHAR(100),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    duration_hours INTEGER,
    cost DECIMAL(10,2) DEFAULT 0,
    max_participants INTEGER,
    location VARCHAR(200),
    training_type VARCHAR(50) DEFAULT 'classroom',
    status VARCHAR(20) DEFAULT 'planned' CHECK (status IN ('planned', 'ongoing', 'completed', 'cancelled')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 11. Training Attendees
CREATE TABLE training_attendees (
    training_id INTEGER REFERENCES training(id) ON DELETE CASCADE,
    employee_id INTEGER REFERENCES employees(id) ON DELETE CASCADE,
    registration_date DATE DEFAULT CURRENT_DATE,
    attendance_status VARCHAR(20) DEFAULT 'registered' CHECK (attendance_status IN ('registered', 'attended', 'completed', 'cancelled')),
    completion_date DATE,
    score DECIMAL(5,2),
    certificate_issued BOOLEAN DEFAULT false,
    certificate_url VARCHAR(500),
    feedback TEXT,
    PRIMARY KEY (training_id, employee_id)
);

-- =============================================================================
-- เพิ่มข้อมูลตัวอย่างสำหรับ Company B (Regional Office)
-- =============================================================================

-- Insert Departments สำหรับสาขาเชียงใหม่
INSERT INTO departments (name, description, manager_id, budget, location) VALUES
('Information tempnology', 'พัฒนาและดูแลระบบเทคโนโลยี สาขาเหนือ', 1, 2000000.00, 'Floor 2'),
('Sales & Marketing', 'ขายและการตลาด ภาคเหนือ', 8, 1500000.00, 'Floor 1'),
('Operations', 'ดำเนินงานและบริการลูกค้า', 10, 800000.00, 'Floor 1');

-- Insert Regional Clients (ลูกค้าภาคเหนือ)
INSERT INTO clients (name, industry, contact_person, email, phone, address, website, contract_value, region, first_project_date) VALUES
('โรงแรมดุสิต เชียงใหม่', 'Hospitality', 'คุณสุนีย์ รักเมือง', 'sunee@dusit.com', '053-234-567', 'เชียงใหม่', 'https://dusit.com', 800000.00, 'Northern Thailand', '2024-02-01'),
('การท่องเที่ยวแห่งประเทศไทย', 'Tourism', 'คุณวิมล ท่องเที่ยว', 'wimon@tat.or.th', '053-345-678', 'เชียงใหม่', 'https://tat.or.th', 600000.00, 'Northern Thailand', '2024-03-15'),
('สวนพฤกษศาสตร์เชียงใหม่', 'Education/Tourism', 'ดร.สมพร ธรรมชาติ', 'somporn@botanic.org', '053-456-789', 'เชียงใหม่', 'https://botanic.org', 450000.00, 'Northern Thailand', '2023-09-01'),
('กลุ่มร้านอาหารล้านนา', 'Restaurant', 'คุณชลิดา อร่อยดี', 'chalida@lanna-food.com', '053-567-890', 'เชียงใหม่', NULL, 350000.00, 'Northern Thailand', '2024-01-20'),
('มหาวิทยาลัยเชียงใหม่', 'Education', 'ผศ.ดร.ประสิทธิ์ เรียนดี', 'prasit@cmu.ac.th', '053-678-901', 'เชียงใหม่', 'https://cmu.ac.th', 550000.00, 'Northern Thailand', '2024-04-01');

-- Insert Skills สำหรับ Regional Office
INSERT INTO skills (name, category, description) VALUES
-- tempnical Skills
('Vue.js', 'tempnical', 'Frontend JavaScript framework'),
('Node.js', 'tempnical', 'Backend JavaScript runtime'),
('MySQL', 'tempnical', 'Database management'),
('Firebase', 'tempnical', 'Google cloud platform'),
('Flutter', 'tempnical', 'Mobile development framework'),
('PHP', 'tempnical', 'Server-side programming language'),
('JavaScript', 'tempnical', 'Programming language'),
('CSS/SCSS', 'tempnical', 'Styling languages'),
('Git', 'tempnical', 'Version control system'),
('MongoDB', 'tempnical', 'NoSQL database'),

-- Regional Specific Skills
('Tourism Industry Knowledge', 'domain', 'ความรู้ด้านอุตสาหกรรมท่องเที่ยว'),
('Local Language (Northern Thai)', 'language', 'ภาษาถิ่นล้านนา'),
('Hospitality Systems', 'domain', 'ระบบโรงแรมและการต้อนรับ'),

-- Soft Skills
('Customer Service', 'soft', 'การบริการลูกค้า'),
('Regional Market Knowledge', 'soft', 'ความรู้ตลาดภาคเหนือ'),
('Communication', 'soft', 'การสื่อสารที่มีประสิทธิภาพ'),

-- Languages
('English', 'language', 'ภาษาอังกฤษ'),
('Thai', 'language', 'ภาษาไทย'),
('Chinese', 'language', 'ภาษาจีน (สำหรับนักท่องเที่ยว)');

-- Insert Employee Skills สำหรับพนักงานสาขา
INSERT INTO employee_skills (employee_id, skill_id, proficiency_level, certified) VALUES
-- สมปอง เขียนโค้ด (Senior Developer)
(1, 1, 5, false), -- Vue.js
(1, 2, 4, false), -- Node.js
(1, 7, 5, false), -- JavaScript
(1, 16, 4, false), -- Communication

-- มณี พัฒนา (Frontend Developer)
(2, 1, 4, false), -- Vue.js
(2, 7, 4, false), -- JavaScript
(2, 8, 4, false), -- CSS/SCSS
(2, 11, 3, false), -- Tourism Industry Knowledge

-- วีระ ระบบ (Backend Developer)
(3, 2, 4, false), -- Node.js
(3, 3, 4, false), -- MySQL
(3, 6, 3, false); -- PHP

-- Insert Sample Timesheets สำหรับ Regional Projects
INSERT INTO timesheets (employee_id, project_id, work_date, hours_worked, task_description, billable, hourly_rate, status) VALUES
-- สมปอง - Hotel Management System
(1, 1, CURRENT_DATE - INTERVAL '7 days', 8.0, 'Hotel booking system development', true, 700.00, 'approved'),
(1, 1, CURRENT_DATE - INTERVAL '6 days', 8.0, 'Payment gateway integration', true, 700.00, 'approved'),

-- มณี - Tourism Website
(2, 2, CURRENT_DATE - INTERVAL '7 days', 8.0, 'Tourism website frontend', true, 550.00, 'approved'),
(2, 2, CURRENT_DATE - INTERVAL '6 days', 7.5, 'Responsive design implementation', true, 550.00, 'approved'),

-- กิตติ - Mobile App
(6, 3, CURRENT_DATE - INTERVAL '5 days', 8.0, 'Botanical garden mobile app', true, 600.00, 'submitted');

-- Insert Sample Expenses สำหรับ Regional Office
INSERT INTO expenses (project_id, employee_id, category, description, amount, expense_date, status) VALUES
(1, 1, 'software', 'Hotel management software license', 8000.00, CURRENT_DATE - INTERVAL '5 days', 'approved'),
(2, 2, 'travel', 'Client visit to TAT office', 1500.00, CURRENT_DATE - INTERVAL '10 days', 'approved'),
(4, 6, 'equipment', 'Mobile testing devices', 5000.00, CURRENT_DATE - INTERVAL '3 days', 'pending');

-- Insert Sample Meetings สำหรับ Regional Office
INSERT INTO meetings (title, description, meeting_date, duration_minutes, location, meeting_type, organizer_id, project_id, client_id) VALUES
('Hotel System Demo', 'นำเสนอระบบจัดการโรงแรม', CURRENT_TIMESTAMP + INTERVAL '3 days', 90, 'Dusit Hotel Conference Room', 'client', 1, 1, 1),
('Weekly Team Meeting', 'ประชุมทีมประจำสัปดาห์', CURRENT_TIMESTAMP + INTERVAL '1 day', 45, 'Chiang Mai Office', 'internal', 1, NULL, NULL),
('Tourism Project Planning', 'วางแผนโปรเจคท่องเที่ยว', CURRENT_TIMESTAMP + INTERVAL '5 days', 120, 'TAT Chiang Mai Office', 'client', 8, 2, 2);

-- Insert Meeting Attendees
INSERT INTO meeting_attendees (meeting_id, employee_id, attendance_status, role) VALUES
-- Hotel Demo
(1, 1, 'confirmed', 'presenter'),
(1, 2, 'confirmed', 'participant'),
(1, 3, 'confirmed', 'participant'),

-- Team Meeting
(2, 1, 'confirmed', 'organizer'),
(2, 2, 'confirmed', 'participant'),
(2, 3, 'confirmed', 'participant'),
(2, 4, 'confirmed', 'participant'),
(2, 6, 'confirmed', 'participant');

-- Insert Sample Equipment สำหรับ Regional Office
INSERT INTO equipment (name, category, brand, model, serial_number, purchase_date, purchase_price, status, assigned_to, location) VALUES
('MacBook Air M2', 'laptop', 'Apple', 'MBA-M2-2024', 'MBA001CNX', '2024-02-01', 45000.00, 'assigned', 1, 'Chiang Mai IT'),
('LG Monitor 24"', 'monitor', 'LG', '24UP550', 'LG001CNX', '2024-02-05', 8500.00, 'assigned', 1, 'Chiang Mai IT'),
('ThinkPad E14', 'laptop', 'Lenovo', 'E14-2024', 'LNV001CNX', '2024-02-10', 35000.00, 'assigned', 2, 'Chiang Mai IT'),
('iPad Pro', 'tablet', 'Apple', 'iPad Pro 11"', 'IPD001CNX', '2024-03-01', 35000.00, 'assigned', 6, 'Mobile Development'),
('Firebase License', 'software_license', 'Google', 'Blaze Plan', 'FB-CNX-001', '2024-02-01', 15000.00, 'assigned', 3, 'Cloud Services');

-- Insert Sample Training สำหรับ Regional Office
INSERT INTO training (title, description, provider, category, start_date, end_date, duration_hours, cost, max_participants, location, training_type) VALUES
('Tourism tempnology Trends', 'เทคโนโลยีสำหรับอุตสาหกรรมท่องเที่ยว', 'Thailand Tourism Association', 'domain', '2024-08-05', '2024-08-06', 12, 15000.00, 8, 'Chiang Mai Convention Center', 'workshop'),
('Vue.js Advanced tempniques', 'Vue.js เชิงลึกสำหรับ Regional Applications', 'CodeCamp Chiang Mai', 'tempnical', '2024-07-20', '2024-07-22', 18, 25000.00, 10, 'Chiang Mai temp Hub', 'classroom'),
('Customer Service Excellence', 'การบริการลูกค้าระดับเยียม', 'Service Excellence Institute', 'soft_skills', '2024-08-15', '2024-08-16', 14, 20000.00, 12, 'Training Room CNX', 'workshop');

-- Insert Training Attendees
INSERT INTO training_attendees (training_id, employee_id, registration_date, attendance_status) VALUES
-- Tourism tempnology
(1, 8, '2024-07-01', 'registered'), -- สุดา (Sales Manager)
(1, 9, '2024-07-01', 'registered'), -- ประยุทธ

-- Vue.js Training
(2, 1, '2024-06-20', 'registered'), -- สมปอง
(2, 2, '2024-06-20', 'registered'), -- มณี

-- Customer Service
(3, 8, '2024-07-15', 'registered'), -- สุดา
(3, 9, '2024-07-15', 'registered'), -- ประยุทธ
(3, 10, '2024-07-15', 'registered'); -- วิไล

-- =============================================================================
-- เพิ่ม Indexes
-- =============================================================================

CREATE INDEX idx_departments_manager_id_b ON departments(manager_id);
CREATE INDEX idx_clients_region_b ON clients(region);
CREATE INDEX idx_clients_status_b ON clients(status);
CREATE INDEX idx_skills_category_b ON skills(category);
CREATE INDEX idx_timesheets_work_date_b ON timesheets(work_date);
CREATE INDEX idx_timesheets_status_b ON timesheets(status);
CREATE INDEX idx_expenses_category_b ON expenses(category);
CREATE INDEX idx_meetings_meeting_date_b ON meetings(meeting_date);
CREATE INDEX idx_equipment_status_b ON equipment(status);
CREATE INDEX idx_training_start_date_b ON training(start_date);

-- =============================================================================
-- เพิ่ม Views สำหรับ Regional Office
-- =============================================================================

-- Regional Performance View
CREATE VIEW regional_performance AS
SELECT 
    e.id,
    e.name,
    e.department,
    e.position,
    COUNT(DISTINCT ep.project_id) as projects_count,
    COALESCE(SUM(t.hours_worked), 0) as total_hours_this_month,
    COUNT(DISTINCT es.skill_id) as skills_count,
    'Northern Thailand' as region
FROM employees e
LEFT JOIN employee_projects ep ON e.id = ep.employee_id
LEFT JOIN timesheets t ON e.id = t.employee_id 
    AND t.work_date >= date_trunc('month', CURRENT_DATE)
LEFT JOIN employee_skills es ON e.id = es.employee_id
GROUP BY e.id, e.name, e.department, e.position;

-- Regional Project Summary
CREATE VIEW regional_project_summary AS
SELECT 
    p.id,
    p.name,
    p.client,
    p.budget,
    p.status,
    'Northern Thailand' as region,
    COUNT(ep.employee_id) as team_size,
    COALESCE(SUM(t.hours_worked * t.hourly_rate), 0) as labor_cost
FROM projects p
LEFT JOIN employee_projects ep ON p.id = ep.project_id
LEFT JOIN timesheets t ON p.id = t.project_id AND t.status = 'approved'
GROUP BY p.id, p.name, p.client, p.budget, p.status;

-- Regional Client Analysis
CREATE VIEW regional_client_analysis AS
SELECT 
    c.id,
    c.name,
    c.industry,
    c.region,
    c.contract_value,
    COUNT(p.id) as projects_count,
    SUM(p.budget) as total_project_budget,
    CASE 
        WHEN c.industry = 'Hospitality' THEN 'Tourism Sector'
        WHEN c.industry = 'Tourism' THEN 'Tourism Sector'
        WHEN c.industry = 'Education' THEN 'Education Sector'
        ELSE 'Other'
    END as sector_classification
FROM clients c
LEFT JOIN projects p ON c.name = p.client
GROUP BY c.id, c.name, c.industry, c.region, c.contract_value;

-- =============================================================================
-- Foreign Key Constraints
-- =============================================================================

ALTER TABLE departments 
ADD CONSTRAINT fk_departments_manager_b 
FOREIGN KEY (manager_id) REFERENCES employees(id);

-- Update ข้อมูลแผนกให้ตรงกับ departments table
UPDATE employees SET department = 'Information tempnology' WHERE department = 'IT';
UPDATE employees SET department = 'Sales & Marketing' WHERE department = 'Sales';
-- Insert Company B employees (Regional Office - 10 people)
INSERT INTO employees (name, department, position, salary, hire_date, email) VALUES
-- IT Department (7 people)
('สมปอง เขียนโค้ด', 'IT', 'Senior Developer', 55000.00, '2022-02-15', 'sompong@siamtemp-north.co.th'),
('มณี พัฒนา', 'IT', 'Frontend Developer', 42000.00, '2022-04-20', 'manee@siamtemp-north.co.th'),
('วีระ ระบบ', 'IT', 'Backend Developer', 45000.00, '2022-03-10', 'weera@siamtemp-north.co.th'),
('ปิยะ ดีไซน์', 'IT', 'UI/UX Designer', 38000.00, '2022-05-15', 'piya@siamtemp-north.co.th'),
('ชาญ ทดสอบ', 'IT', 'QA Engineer', 35000.00, '2022-06-01', 'chan@siamtemp-north.co.th'),
('กิตติ โมบาย', 'IT', 'Mobile Developer', 43000.00, '2022-07-10', 'kitti@siamtemp-north.co.th'),
('อนุรักษ์ ดาต้า', 'IT', 'Data Analyst', 40000.00, '2022-08-05', 'anurak@siamtemp-north.co.th'),

-- Sales Department (2 people)
('สุดา ขายดี', 'Sales', 'Sales Manager', 45000.00, '2022-03-01', 'suda@siamtemp-north.co.th'),
('ประยุทธ ลูกค้า', 'Sales', 'Account Executive', 32000.00, '2022-07-15', 'prayut@siamtemp-north.co.th'),

-- Management (1 person)
('วิไล จัดการ', 'Management', 'Branch Manager', 70000.00, '2022-02-01', 'wilai@siamtemp-north.co.th');

-- Insert Company B projects (Regional projects)
INSERT INTO projects (name, client, budget, status, start_date, end_date, temp_stack) VALUES
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
COMMENT ON DATABASE siamtemp_company_b IS 'Siamtemp Regional Office Database - Chiang Mai';
COMMENT ON TABLE employees IS 'พนักงานสาขาภาคเหนือ เชียงใหม่';
COMMENT ON TABLE projects IS 'โปรเจคสาขาภาคเหนือ';
COMMENT ON DATABASE siamtemp_company_b IS 'Siamtemp Regional Office Database - Enhanced with comprehensive business data';
COMMENT ON TABLE departments IS 'แผนกต่างๆ ในสาขาเชียงใหม่';
COMMENT ON TABLE clients IS 'ลูกค้าภาคเหนือและลูกค้าท้องถิ่น';
COMMENT ON TABLE skills IS 'ทักษะเฉพาะสำหรับตลาดภาคเหนือ';
COMMENT ON TABLE timesheets IS 'การบันทึกเวลาทำงานพนักงานสาขา';
COMMENT ON TABLE training IS 'หลักสูตรอบรมสำหรับพนักงานสาขา';

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
('Siamtemp Regional Office', 'Regional Branch', 'Chiang Mai, Thailand', 10, '2022-02-01', 'สาขาภาคเหนือของบริษัท สยามเทค จำกัด ตั้งอยู่ในจังหวัดเชียงใหม่ เน้นงานบริการลูกค้าภาคเหนือและโปรเจคท้องถิ่น');

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

SELECT 'Enhanced Company B Database Setup Complete' as status,
       (SELECT COUNT(*) FROM employees) as total_employees,
       (SELECT COUNT(*) FROM projects) as total_projects,
       (SELECT COUNT(*) FROM clients) as total_clients,
       (SELECT COUNT(*) FROM departments) as total_departments,
       (SELECT COUNT(*) FROM skills) as total_skills,
       'Northern Thailand Regional Office' as office_type;