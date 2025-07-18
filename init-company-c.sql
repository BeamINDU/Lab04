-- Company C (SiamTech International) Database Schema
-- International Office - Multi-language support
-- Note: Database is already created by Docker ENV variables

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

-- 2. Clients Table (International Clients)
CREATE TABLE clients (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    industry VARCHAR(100),
    contact_person VARCHAR(100),
    email VARCHAR(150),
    phone VARCHAR(20),
    address TEXT,
    website VARCHAR(200),
    country VARCHAR(100),
    timezone VARCHAR(50),
    currency VARCHAR(10) DEFAULT 'USD',
    contract_value DECIMAL(15,2) DEFAULT 0,
    contract_value_usd DECIMAL(15,2) DEFAULT 0,
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'prospect', 'churned')),
    market_type VARCHAR(30) DEFAULT 'international', -- 'domestic', 'regional', 'international', 'global'
    first_project_date DATE,
    last_contact_date DATE,
    preferred_language VARCHAR(20) DEFAULT 'en',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Skills Table (International Skills)
CREATE TABLE skills (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    category VARCHAR(50), -- 'technical', 'soft', 'language', 'certification', 'international'
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    global_demand_level VARCHAR(20) DEFAULT 'medium' -- 'low', 'medium', 'high', 'critical'
);

-- 4. Employee Skills
CREATE TABLE employee_skills (
    employee_id INTEGER REFERENCES employees(id) ON DELETE CASCADE,
    skill_id INTEGER REFERENCES skills(id) ON DELETE CASCADE,
    proficiency_level INTEGER CHECK (proficiency_level BETWEEN 1 AND 5),
    acquired_date DATE DEFAULT CURRENT_DATE,
    certified BOOLEAN DEFAULT false,
    certification_date DATE,
    certification_authority VARCHAR(200),
    expiry_date DATE,
    PRIMARY KEY (employee_id, skill_id)
);

-- 5. Timesheets (with timezone support)
CREATE TABLE timesheets (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES employees(id) ON DELETE CASCADE,
    project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
    work_date DATE NOT NULL,
    hours_worked DECIMAL(4,2) NOT NULL CHECK (hours_worked >= 0 AND hours_worked <= 24),
    task_description TEXT,
    billable BOOLEAN DEFAULT true,
    hourly_rate DECIMAL(8,2),
    hourly_rate_usd DECIMAL(8,2),
    currency VARCHAR(10) DEFAULT 'USD',
    client_timezone VARCHAR(50),
    status VARCHAR(20) DEFAULT 'draft' CHECK (status IN ('draft', 'submitted', 'approved', 'rejected')),
    submitted_at TIMESTAMP,
    approved_by INTEGER REFERENCES employees(id),
    approved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. Expenses (International)
CREATE TABLE expenses (
    id SERIAL PRIMARY KEY,
    project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
    employee_id INTEGER REFERENCES employees(id) ON DELETE CASCADE,
    category VARCHAR(50), -- 'travel', 'equipment', 'software', 'training', 'international_transfer', 'visa', 'other'
    description TEXT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'USD',
    amount_usd DECIMAL(10,2),
    exchange_rate DECIMAL(10,4) DEFAULT 1.0000,
    expense_date DATE NOT NULL,
    country VARCHAR(100),
    receipt_url VARCHAR(500),
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected', 'reimbursed')),
    approved_by INTEGER REFERENCES employees(id),
    approved_at TIMESTAMP,
    reimbursement_method VARCHAR(50), -- 'bank_transfer', 'paypal', 'wise', 'check'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 7. Meetings (Global)
CREATE TABLE meetings (
    id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    meeting_date TIMESTAMP NOT NULL,
    duration_minutes INTEGER DEFAULT 60,
    location VARCHAR(200),
    timezone VARCHAR(50) DEFAULT 'UTC',
    meeting_platform VARCHAR(100), -- 'zoom', 'teams', 'google_meet', 'in_person'
    meeting_link VARCHAR(500),
    meeting_type VARCHAR(50) DEFAULT 'internal', -- 'internal', 'client', 'vendor', 'training', 'international'
    status VARCHAR(20) DEFAULT 'scheduled' CHECK (status IN ('scheduled', 'completed', 'cancelled', 'rescheduled')),
    organizer_id INTEGER REFERENCES employees(id),
    project_id INTEGER REFERENCES projects(id),
    client_id INTEGER REFERENCES clients(id),
    language VARCHAR(20) DEFAULT 'en',
    recording_url VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 8. Meeting Attendees
CREATE TABLE meeting_attendees (
    meeting_id INTEGER REFERENCES meetings(id) ON DELETE CASCADE,
    employee_id INTEGER REFERENCES employees(id) ON DELETE CASCADE,
    attendance_status VARCHAR(20) DEFAULT 'invited' CHECK (attendance_status IN ('invited', 'confirmed', 'attended', 'absent')),
    role VARCHAR(50) DEFAULT 'participant', -- 'organizer', 'presenter', 'participant', 'translator'
    timezone VARCHAR(50),
    local_meeting_time TIMESTAMP,
    notes TEXT,
    PRIMARY KEY (meeting_id, employee_id)
);

-- 9. Equipment (International)
CREATE TABLE equipment (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(100), -- 'laptop', 'desktop', 'monitor', 'phone', 'software_license', 'subscription'
    brand VARCHAR(100),
    model VARCHAR(100),
    serial_number VARCHAR(100) UNIQUE,
    purchase_date DATE,
    purchase_price DECIMAL(10,2),
    currency VARCHAR(10) DEFAULT 'USD',
    purchase_price_usd DECIMAL(10,2),
    warranty_expiry DATE,
    status VARCHAR(20) DEFAULT 'available' CHECK (status IN ('available', 'assigned', 'maintenance', 'retired', 'shipped')),
    assigned_to INTEGER REFERENCES employees(id),
    assigned_date DATE,
    location VARCHAR(200),
    country VARCHAR(100),
    shipping_status VARCHAR(50), -- 'not_shipped', 'shipped', 'delivered', 'customs', 'lost'
    tracking_number VARCHAR(200),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 10. Training (International)
CREATE TABLE training (
    id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    provider VARCHAR(200),
    category VARCHAR(100), -- 'technical', 'soft_skills', 'certification', 'compliance', 'cross_cultural', 'language'
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    duration_hours INTEGER,
    cost DECIMAL(10,2) DEFAULT 0,
    currency VARCHAR(10) DEFAULT 'USD',
    cost_usd DECIMAL(10,2) DEFAULT 0,
    max_participants INTEGER,
    location VARCHAR(200),
    timezone VARCHAR(50) DEFAULT 'UTC',
    training_type VARCHAR(50) DEFAULT 'online', -- 'classroom', 'online', 'workshop', 'conference', 'self_paced'
    language VARCHAR(20) DEFAULT 'en',
    certification_provided BOOLEAN DEFAULT false,
    international_certification BOOLEAN DEFAULT false,
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
    certificate_number VARCHAR(100),
    feedback TEXT,
    timezone VARCHAR(50),
    PRIMARY KEY (training_id, employee_id)
);

-- 12. International Contracts (เพิ่มเฉพาะสำหรับ International Office)
CREATE TABLE international_contracts (
    id SERIAL PRIMARY KEY,
    contract_number VARCHAR(100) UNIQUE NOT NULL,
    client_id INTEGER REFERENCES clients(id),
    project_id INTEGER REFERENCES projects(id),
    contract_type VARCHAR(50), -- 'fixed_price', 'time_and_materials', 'retainer', 'subscription'
    total_value DECIMAL(15,2),
    currency VARCHAR(10) DEFAULT 'USD',
    total_value_usd DECIMAL(15,2),
    payment_terms VARCHAR(100), -- 'net_30', 'net_60', 'milestone_based', 'monthly'
    start_date DATE NOT NULL,
    end_date DATE,
    auto_renewal BOOLEAN DEFAULT false,
    governing_law VARCHAR(100), -- 'Thailand', 'Singapore', 'USA', 'UK'
    dispute_resolution VARCHAR(100), -- 'Thailand Courts', 'Singapore Arbitration', 'SIAC'
    status VARCHAR(30) DEFAULT 'draft' CHECK (status IN ('draft', 'sent', 'signed', 'active', 'completed', 'terminated')),
    signed_date DATE,
    contract_file_url VARCHAR(500),
    notes TEXT,
    created_by INTEGER REFERENCES employees(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 13. International Payments (การชำระเงินระหว่างประเทศ)
CREATE TABLE international_payments (
    id SERIAL PRIMARY KEY,
    contract_id INTEGER REFERENCES international_contracts(id),
    invoice_number VARCHAR(100),
    amount DECIMAL(15,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'USD',
    amount_usd DECIMAL(15,2),
    exchange_rate DECIMAL(10,4) DEFAULT 1.0000,
    payment_method VARCHAR(50), -- 'wire_transfer', 'swift', 'wise', 'paypal', 'cryptocurrency'
    payment_date DATE,
    due_date DATE NOT NULL,
    received_date DATE,
    bank_fees DECIMAL(8,2) DEFAULT 0,
    status VARCHAR(30) DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'received', 'overdue', 'cancelled')),
    reference_number VARCHAR(200),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- เพิ่มข้อมูลตัวอย่างสำหรับ International Office
-- =============================================================================

-- Insert International Departments
INSERT INTO departments (name, description, manager_id, budget, location) VALUES
('International Development', 'Global software development team', 1, 8000000.00, 'Bangkok Office - Floor 5'),
('Global Sales', 'International sales and business development', 6, 3000000.00, 'Bangkok Office - Floor 3'),
('Operations', 'International operations and project management', 8, 2000000.00, 'Bangkok Office - Floor 4');

-- Insert International Clients
INSERT INTO clients (name, industry, contact_person, email, phone, address, country, timezone, currency, contract_value, contract_value_usd, market_type, preferred_language) VALUES
('MegaCorp International', 'Technology', 'John Smith', 'john.smith@megacorp.com', '+1-555-0123', 'New York, NY', 'USA', 'America/New_York', 'USD', 2800000.00, 2800000.00, 'global', 'en'),
('Education Global Network', 'Education', 'Dr. Sarah Johnson', 'sarah@eduglobal.org', '+44-20-1234-5678', 'London, UK', 'United Kingdom', 'Europe/London', 'GBP', 1800000.00, 2250000.00, 'international', 'en'),
('Global Finance Corp', 'Financial Services', 'Michael Chen', 'michael.chen@globalfinance.sg', '+65-6123-4567', 'Singapore', 'Singapore', 'Asia/Singapore', 'SGD', 4000000.00, 3200000.00, 'global', 'en'),
('PayGlobal Ltd', 'Fintech', 'Emma Williams', 'emma@payglobal.com', '+61-2-1234-5678', 'Sydney, Australia', 'Australia', 'Australia/Sydney', 'AUD', 2400000.00, 1800000.00, 'international', 'en'),
('Tech Solutions Europe', 'Technology Consulting', 'Andreas Mueller', 'andreas@techsolutions.de', '+49-30-12345678', 'Berlin, Germany', 'Germany', 'Europe/Berlin', 'EUR', 3500000.00, 3850000.00, 'international', 'en');

-- Insert International Skills
INSERT INTO skills (name, category, description, global_demand_level) VALUES
-- International Technical Skills
('React', 'technical', 'Modern frontend library', 'high'),
('Node.js', 'technical', 'JavaScript runtime for backend', 'high'),
('TypeScript', 'technical', 'Typed JavaScript for large applications', 'high'),
('Microservices Architecture', 'technical', 'Distributed system design', 'critical'),
('AWS Cloud Architecture', 'technical', 'Enterprise cloud solutions', 'critical'),
('Docker & Kubernetes', 'technical', 'Container orchestration', 'high'),
('GraphQL', 'technical', 'Modern API development', 'medium'),
('Blockchain Development', 'technical', 'Distributed ledger technology', 'high'),
('AI/ML Integration', 'technical', 'Artificial Intelligence implementation', 'critical'),
('DevOps & CI/CD', 'technical', 'Continuous integration and deployment', 'high'),

-- International Business Skills
('Cross-Cultural Communication', 'international', 'Working across cultures effectively', 'critical'),
('International Project Management', 'soft', 'Managing global distributed teams', 'high'),
('Global Market Analysis', 'soft', 'Understanding international markets', 'high'),
('Agile/Scrum Methodology', 'soft', 'Agile project management', 'high'),
('Client Relationship Management', 'soft', 'Managing international clients', 'high'),
('Business Development', 'soft', 'Growing international business', 'high'),

-- Languages (Critical for International)
('English (Native)', 'language', 'Native English proficiency', 'critical'),
('English (Business)', 'language', 'Business-level English', 'critical'),
('Mandarin Chinese', 'language', 'Chinese language skills', 'high'),
('Japanese', 'language', 'Japanese language skills', 'medium'),
('German', 'language', 'German language skills', 'medium'),
('Spanish', 'language', 'Spanish language skills', 'medium'),

-- International Certifications
('AWS Solutions Architect Professional', 'certification', 'Advanced AWS certification', 'high'),
('PMP International', 'certification', 'Project Management Professional', 'high'),
('Certified Scrum Master', 'certification', 'Agile certification', 'medium'),
('CISSP', 'certification', 'Information security certification', 'high');

-- Insert Employee Skills for International Team
INSERT INTO employee_skills (employee_id, skill_id, proficiency_level, certified, certification_authority) VALUES
-- Alex Johnson (Senior Full Stack Developer)
(1, 1, 5, false, NULL), -- React
(1, 2, 5, false, NULL), -- Node.js
(1, 3, 5, false, NULL), -- TypeScript
(1, 4, 4, false, NULL), -- Microservices
(1, 17, 5, true, 'Native Speaker'), -- English (Native)
(1, 22, 4, false, NULL), -- AWS Solutions Architect Professional

-- Sarah Chen (Frontend Developer)
(2, 1, 4, false, NULL), -- React
(2, 3, 4, false, NULL), -- TypeScript
(2, 18, 4, false, NULL), -- English (Business)
(2, 19, 3, false, NULL), -- Mandarin Chinese

-- Michael Kim (DevOps Engineer)
(3, 5, 5, true, 'AWS'), -- AWS Cloud Architecture
(3, 6, 5, false, NULL), -- Docker & Kubernetes
(3, 10, 4, false, NULL), -- DevOps & CI/CD
(3, 22, 5, true, 'AWS'); -- AWS Solutions Architect Professional

-- Insert Sample International Timesheets
INSERT INTO timesheets (employee_id, project_id, work_date, hours_worked, task_description, billable, hourly_rate, hourly_rate_usd, currency, client_timezone, status) VALUES
-- Alex - Global E-commerce Platform
(1, 1, CURRENT_DATE - INTERVAL '7 days', 8.0, 'Microservices architecture design', true, 120.00, 120.00, 'USD', 'America/New_York', 'approved'),
(1, 1, CURRENT_DATE - INTERVAL '6 days', 8.0, 'API development and testing', true, 120.00, 120.00, 'USD', 'America/New_York', 'approved'),

-- Sarah - Multi-language CMS
(2, 2, CURRENT_DATE - INTERVAL '7 days', 8.0, 'Internationalization implementation', true, 85.00, 85.00, 'USD', 'Europe/London', 'approved'),
(2, 2, CURRENT_DATE - INTERVAL '6 days', 7.5, 'Multi-language UI components', true, 85.00, 85.00, 'USD', 'Europe/London', 'approved'),

-- Michael - Banking API Infrastructure
(3, 3, CURRENT_DATE - INTERVAL '5 days', 8.0, 'Kubernetes cluster setup', true, 110.00, 110.00, 'USD', 'Asia/Singapore', 'submitted');

-- Insert Sample International Expenses
INSERT INTO expenses (project_id, employee_id, category, description, amount, currency, amount_usd, exchange_rate, expense_date, country, status) VALUES
(1, 1, 'software', 'Enterprise GitHub subscription', 500.00, 'USD', 500.00, 1.0000, CURRENT_DATE - INTERVAL '5 days', 'USA', 'approved'),
(2, 2, 'software', 'Figma Professional license', 144.00, 'USD', 144.00, 1.0000, CURRENT_DATE - INTERVAL '10 days', 'UK', 'approved'),
(3, 3, 'training', 'Kubernetes certification course', 400.00, 'USD', 400.00, 1.0000, CURRENT_DATE - INTERVAL '3 days', 'Singapore', 'pending'),
(1, 1, 'international_transfer', 'Wire transfer fees for client payment', 45.00, 'USD', 45.00, 1.0000, CURRENT_DATE - INTERVAL '15 days', 'Thailand', 'reimbursed');

-- Insert International Meetings
INSERT INTO meetings (title, description, meeting_date, duration_minutes, location, timezone, meeting_platform, meeting_type, organizer_id, project_id, client_id, language) VALUES
('Global E-commerce Kickoff', 'Project kickoff with MegaCorp team', CURRENT_TIMESTAMP + INTERVAL '2 days', 90, 'Virtual', 'America/New_York', 'zoom', 'client', 1, 1, 1, 'en'),
('Weekly International Standup', 'Team sync across timezones', CURRENT_TIMESTAMP + INTERVAL '1 day', 30, 'Virtual', 'UTC', 'teams', 'internal', 6, NULL, NULL, 'en'),
('Banking API Security Review', 'Security assessment with Global Finance', CURRENT_TIMESTAMP + INTERVAL '4 days', 120, 'Virtual', 'Asia/Singapore', 'zoom', 'client', 3, 3, 3, 'en');

-- Insert Meeting Attendees with Timezone Info
INSERT INTO meeting_attendees (meeting_id, employee_id, attendance_status, role, timezone, local_meeting_time) VALUES
-- Global E-commerce Kickoff
(1, 1, 'confirmed', 'presenter', 'Asia/Bangkok', CURRENT_TIMESTAMP + INTERVAL '2 days' + INTERVAL '12 hours'),
(1, 2, 'confirmed', 'participant', 'Asia/Bangkok', CURRENT_TIMESTAMP + INTERVAL '2 days' + INTERVAL '12 hours'),
(1, 6, 'confirmed', 'participant', 'Asia/Bangkok', CURRENT_TIMESTAMP + INTERVAL '2 days' + INTERVAL '12 hours'),

-- Weekly Standup
(2, 1, 'confirmed', 'organizer', 'Asia/Bangkok', CURRENT_TIMESTAMP + INTERVAL '1 day' + INTERVAL '7 hours'),
(2, 2, 'confirmed', 'participant', 'Asia/Bangkok', CURRENT_TIMESTAMP + INTERVAL '1 day' + INTERVAL '7 hours'),
(2, 3, 'confirmed', 'participant', 'Asia/Bangkok', CURRENT_TIMESTAMP + INTERVAL '1 day' + INTERVAL '7 hours');

-- Insert International Equipment
INSERT INTO equipment (name, category, brand, model, serial_number, purchase_date, purchase_price, currency, purchase_price_usd, status, assigned_to, location, country, shipping_status) VALUES
('MacBook Pro 16" M3', 'laptop', 'Apple', 'MBP16-M3-2024', 'MBP001INTL', '2024-01-15', 3500.00, 'USD', 3500.00, 'assigned', 1, 'Bangkok Office', 'Thailand', 'delivered'),
('Dell XPS 15', 'laptop', 'Dell', 'XPS15-2024', 'DLL001INTL', '2024-02-01', 2200.00, 'USD', 2200.00, 'assigned', 2, 'Bangkok Office', 'Thailand', 'delivered'),
('AWS Enterprise Support', 'software_license', 'Amazon', 'Enterprise', 'AWS-INTL-001', '2024-01-01', 15000.00, 'USD', 15000.00, 'assigned', 3, 'Cloud Infrastructure', 'Global', 'not_shipped'),
('GitHub Enterprise Server', 'software_license', 'GitHub', 'Enterprise', 'GH-INTL-001', '2024-01-01', 8000.00, 'USD', 8000.00, 'available', NULL, 'Software Licenses', 'Global', 'not_shipped'),
('Zoom Pro License', 'software_license', 'Zoom', 'Professional', 'ZM-INTL-001', '2024-01-01', 1800.00, 'USD', 1800.00, 'assigned', 6, 'Communication Tools', 'Global', 'not_shipped');

-- Insert International Training
INSERT INTO training (title, description, provider, category, start_date, end_date, duration_hours, cost, currency, cost_usd, location, timezone, training_type, language, international_certification) VALUES
('AWS Solutions Architect Professional', 'Advanced AWS certification for enterprise', 'AWS Training', 'certification', '2024-08-01', '2024-08-05', 40, 2000.00, 'USD', 2000.00, 'Online', 'UTC', 'online', 'en', true),
('Cross-Cultural Communication', 'Working effectively with global teams', 'Global Business Institute', 'cross_cultural', '2024-07-15', '2024-07-17', 21, 1500.00, 'USD', 1500.00, 'Bangkok Office', 'Asia/Bangkok', 'workshop', 'en', false),
('Advanced Microservices Architecture', 'Enterprise microservices patterns', 'Cloud Native Foundation', 'technical', '2024-08-10', '2024-08-14', 35, 2500.00, 'USD', 2500.00, 'Online', 'UTC', 'online', 'en', true),
('International Project Management', 'Managing distributed global teams', 'PMI International', 'soft_skills', '2024-09-01', '2024-09-03', 24, 1800.00, 'USD', 1800.00, 'Singapore', 'Asia/Singapore', 'classroom', 'en', true);

-- Insert Training Attendees
INSERT INTO training_attendees (training_id, employee_id, registration_date, attendance_status, timezone) VALUES
-- AWS Training
(1, 1, '2024-07-01', 'registered', 'Asia/Bangkok'),
(1, 3, '2024-07-01', 'registered', 'Asia/Bangkok'),

-- Cross-Cultural Communication
(2, 6, '2024-06-15', 'registered', 'Asia/Bangkok'),
(2, 7, '2024-06-15', 'registered', 'Asia/Bangkok'),
(2, 8, '2024-06-15', 'registered', 'Asia/Bangkok'),

-- Microservices Training
(3, 1, '2024-07-10', 'registered', 'Asia/Bangkok'),
(3, 5, '2024-07-10', 'registered', 'Asia/Bangkok'),

-- International PM
(4, 8, '2024-08-01', 'registered', 'Asia/Bangkok');

-- Insert International Contracts
INSERT INTO international_contracts (contract_number, client_id, project_id, contract_type, total_value, currency, total_value_usd, payment_terms, start_date, end_date, governing_law, dispute_resolution, status, created_by) VALUES
('INTL-2024-001', 1, 1, 'fixed_price', 2800000.00, 'USD', 2800000.00, 'milestone_based', '2024-01-15', '2024-10-15', 'Singapore', 'SIAC', 'active', 8),
('INTL-2024-002', 2, 2, 'time_and_materials', 1800000.00, 'GBP', 2250000.00, 'monthly', '2024-02-01', '2024-08-01', 'UK', 'London Courts', 'active', 8),
('INTL-2024-003', 3, 3, 'fixed_price', 4000000.00, 'SGD', 3200000.00, 'net_30', '2024-03-01', '2024-12-01', 'Singapore', 'SIAC', 'active', 8),
('INTL-2024-004', 4, 4, 'fixed_price', 2400000.00, 'AUD', 1800000.00, 'milestone_based', '2023-08-01', '2024-02-29', 'Australia', 'Australian Courts', 'completed', 8);

-- Insert International Payments
INSERT INTO international_payments (contract_id, invoice_number, amount, currency, amount_usd, exchange_rate, payment_method, due_date, received_date, status, reference_number) VALUES
(1, 'INV-MEGA-001', 700000.00, 'USD', 700000.00, 1.0000, 'wire_transfer', '2024-02-15', '2024-02-12', 'received', 'WT240212001'),
(1, 'INV-MEGA-002', 700000.00, 'USD', 700000.00, 1.0000, 'wire_transfer', '2024-04-15', '2024-04-18', 'received', 'WT240418001'),
(2, 'INV-EDU-001', 300000.00, 'GBP', 375000.00, 1.2500, 'swift', '2024-03-01', '2024-02-28', 'received', 'SW240228001'),
(3, 'INV-GFC-001', 1000000.00, 'SGD', 800000.00, 0.8000, 'wire_transfer', '2024-04-01', NULL, 'pending', NULL);

-- Insert Company C employees (International Office - 8 people)
INSERT INTO employees (name, department, position, salary, hire_date, email) VALUES
-- IT Department (5 people)
('Alex Johnson', 'IT', 'Senior Full Stack Developer', 85000.00, '2022-03-15', 'alex.johnson@siamtech-intl.com'),
('Sarah Chen', 'IT', 'Frontend Developer', 65000.00, '2022-05-20', 'sarah.chen@siamtech-intl.com'),
('Michael Kim', 'IT', 'DevOps Engineer', 75000.00, '2022-04-10', 'michael.kim@siamtech-intl.com'),
('Emma Wilson', 'IT', 'UI/UX Designer', 60000.00, '2022-06-15', 'emma.wilson@siamtech-intl.com'),
('David Rodriguez', 'IT', 'Backend Developer', 70000.00, '2022-07-01', 'david.rodriguez@siamtech-intl.com'),

-- Sales Department (2 people)
('Jessica Brown', 'Sales', 'International Sales Manager', 80000.00, '2022-03-01', 'jessica.brown@siamtech-intl.com'),
('Robert Lee', 'Sales', 'Business Development', 55000.00, '2022-08-15', 'robert.lee@siamtech-intl.com'),

-- Management (1 person)
('Linda Taylor', 'Management', 'International Director', 120000.00, '2022-02-15', 'linda.taylor@siamtech-intl.com');

-- Insert Company C projects (International projects)
INSERT INTO projects (name, client, budget, status, start_date, end_date, tech_stack) VALUES
('Global E-commerce Platform', 'MegaCorp International', 2800000.00, 'active', '2024-01-15', '2024-10-15', 'React, Node.js, AWS, Stripe API'),
('Multi-language CMS', 'Education Global Network', 1500000.00, 'active', '2024-02-01', '2024-08-01', 'Next.js, Prisma, PostgreSQL, i18n'),
('International Banking API', 'Global Finance Corp', 3200000.00, 'active', '2024-03-01', '2024-12-01', 'Node.js, Express, JWT, Microservices'),
('Cross-border Payment System', 'PayGlobal Ltd', 1800000.00, 'completed', '2023-08-01', '2024-02-29', 'React, Node.js, Blockchain, APIs');

-- Insert employee-project relationships (Company C)
INSERT INTO employee_projects (employee_id, project_id, role, allocation) VALUES
-- Global E-commerce Platform
(1, 1, 'Lead Developer', 0.8),
(2, 1, 'Frontend Developer', 1.0),
(3, 1, 'DevOps Engineer', 0.7),
(4, 1, 'UI/UX Designer', 0.9),

-- Multi-language CMS
(1, 2, 'Technical Architect', 0.2),
(5, 2, 'Backend Developer', 1.0),
(2, 2, 'Frontend Integration', 0.0), -- will start later

-- International Banking API
(5, 3, 'API Developer', 0.0), -- will start after CMS
(3, 3, 'Infrastructure', 0.3),
(1, 3, 'System Design', 0.0); -- future involvement

-- Create indexes
CREATE INDEX idx_employees_department_c ON employees(department);
CREATE INDEX idx_employees_position_c ON employees(position);
CREATE INDEX idx_projects_status_c ON projects(status);
CREATE INDEX idx_clients_country ON clients(country);
CREATE INDEX idx_clients_market_type ON clients(market_type);
CREATE INDEX idx_clients_currency ON clients(currency);
CREATE INDEX idx_skills_global_demand ON skills(global_demand_level);
CREATE INDEX idx_timesheets_currency ON timesheets(currency);
CREATE INDEX idx_expenses_currency ON expenses(currency);
CREATE INDEX idx_expenses_country ON expenses(country);
CREATE INDEX idx_meetings_timezone ON meetings(timezone);
CREATE INDEX idx_meetings_language ON meetings(language);
CREATE INDEX idx_equipment_country ON equipment(country);
CREATE INDEX idx_equipment_shipping_status ON equipment(shipping_status);
CREATE INDEX idx_training_language ON training(language);
CREATE INDEX idx_training_international_cert ON training(international_certification);
CREATE INDEX idx_contracts_currency ON international_contracts(currency);
CREATE INDEX idx_contracts_governing_law ON international_contracts(governing_law);
CREATE INDEX idx_payments_currency ON international_payments(currency);
CREATE INDEX idx_payments_status ON international_payments(status);

-- =============================================================================
-- เพิ่ม Views สำหรับ International Office
-- =============================================================================

-- International Team Performance
CREATE VIEW international_team_performance AS
SELECT 
    e.id,
    e.name,
    e.department,
    e.position,
    COUNT(DISTINCT ep.project_id) as international_projects,
    COALESCE(SUM(t.hours_worked * t.hourly_rate_usd), 0) as revenue_generated_usd,
    COUNT(DISTINCT es.skill_id) FILTER (WHERE s.global_demand_level = 'critical') as critical_skills,
    COUNT(DISTINCT es.skill_id) FILTER (WHERE s.category = 'language') as language_skills,
    AVG(es.proficiency_level) FILTER (WHERE s.category = 'technical') as avg_technical_level
FROM employees e
LEFT JOIN employee_projects ep ON e.id = ep.employee_id
LEFT JOIN timesheets t ON e.id = t.employee_id AND t.status = 'approved'
LEFT JOIN employee_skills es ON e.id = es.employee_id
LEFT JOIN skills s ON es.skill_id = s.id
GROUP BY e.id, e.name, e.department, e.position;

-- Global Project Financial Summary
CREATE VIEW global_project_summary AS
SELECT 
    p.id,
    p.name,
    p.client,
    p.status,
    c.country as client_country,
    c.currency as client_currency,
    ic.total_value_usd as contract_value_usd,
    COALESCE(SUM(t.hours_worked * t.hourly_rate_usd), 0) as labor_cost_usd,
    COALESCE(SUM(ex.amount_usd), 0) as expenses_usd,
    ic.total_value_usd - (COALESCE(SUM(t.hours_worked * t.hourly_rate_usd), 0) + COALESCE(SUM(ex.amount_usd), 0)) as profit_margin_usd,
    COUNT(DISTINCT ep.employee_id) as team_size
FROM projects p
LEFT JOIN clients c ON p.client = c.name
LEFT JOIN international_contracts ic ON p.id = ic.project_id
LEFT JOIN timesheets t ON p.id = t.project_id AND t.status = 'approved'
LEFT JOIN expenses ex ON p.id = ex.project_id AND ex.status = 'approved'
LEFT JOIN employee_projects ep ON p.id = ep.project_id
GROUP BY p.id, p.name, p.client, p.status, c.country, c.currency, ic.total_value_usd;

-- International Client Analysis
CREATE VIEW international_client_analysis AS
SELECT 
    c.id,
    c.name,
    c.country,
    c.industry,
    c.market_type,
    c.currency,
    c.contract_value_usd,
    COUNT(p.id) as total_projects,
    SUM(ic.total_value_usd) as total_contracts_usd,
    COUNT(ip.id) as total_payments,
    SUM(ip.amount_usd) FILTER (WHERE ip.status = 'received') as payments_received_usd,
    CASE 
        WHEN c.market_type = 'global' THEN 'Tier 1 - Global'
        WHEN c.contract_value_usd > 2000000 THEN 'Tier 1 - Enterprise'
        WHEN c.contract_value_usd > 500000 THEN 'Tier 2 - Corporate'
        ELSE 'Tier 3 - SME'
    END as client_tier
FROM clients c
LEFT JOIN projects p ON c.name = p.client
LEFT JOIN international_contracts ic ON c.id = ic.client_id
LEFT JOIN international_payments ip ON ic.id = ip.contract_id
GROUP BY c.id, c.name, c.country, c.industry, c.market_type, c.currency, c.contract_value_usd;

-- Revenue by Currency and Country
CREATE VIEW revenue_by_geography AS
SELECT 
    c.country,
    c.currency,
    COUNT(DISTINCT c.id) as clients_count,
    COUNT(DISTINCT p.id) as projects_count,
    SUM(ic.total_value_usd) as total_contracts_usd,
    SUM(ip.amount_usd) FILTER (WHERE ip.status = 'received') as revenue_received_usd,
    AVG(ic.total_value_usd) as avg_contract_size_usd
FROM clients c
LEFT JOIN projects p ON c.name = p.client
LEFT JOIN international_contracts ic ON c.id = ic.client_id
LEFT JOIN international_payments ip ON ic.id = ip.contract_id
GROUP BY c.country, c.currency
ORDER BY total_contracts_usd DESC;

-- =============================================================================
-- Foreign Key Constraints
-- =============================================================================

ALTER TABLE departments 
ADD CONSTRAINT fk_departments_manager_c 
FOREIGN KEY (manager_id) REFERENCES employees(id);

ALTER TABLE international_contracts 
ADD CONSTRAINT fk_contracts_created_by 
FOREIGN KEY (created_by) REFERENCES employees(id);

-- Update employee departments
UPDATE employees SET department = 'International Development' WHERE department = 'IT';
UPDATE employees SET department = 'Global Sales' WHERE department = 'Sales';
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

-- Company C specific information
COMMENT ON DATABASE siamtech_company_c IS 'SiamTech International Office Database - Global Operations';
COMMENT ON TABLE employees IS 'International team members';
COMMENT ON TABLE projects IS 'Global and international projects';
COMMENT ON DATABASE siamtech_company_c IS 'SiamTech International Office Database - Global Operations with Multi-Currency Support';
COMMENT ON TABLE clients IS 'International clients with multi-currency and timezone support';
COMMENT ON TABLE international_contracts IS 'International contracts with various currencies and legal frameworks';
COMMENT ON TABLE international_payments IS 'Cross-border payments with currency conversion tracking';
COMMENT ON TABLE timesheets IS 'International timesheets with timezone and currency support';
COMMENT ON TABLE expenses IS 'International expenses with multi-currency support';

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
('SiamTech International', 'International Office', 'Bangkok, Thailand (Global Operations)', 8, '2022-02-15', 'International division of SiamTech Co., Ltd. focusing on global clients and cross-border technology solutions');

-- International-specific views
CREATE VIEW international_projects AS
SELECT 
    p.name as project_name,
    p.client,
    p.budget,
    p.status,
    'International' as market_type,
    CASE 
        WHEN p.budget > 2000000 THEN 'Enterprise'
        WHEN p.budget > 1000000 THEN 'Large'
        ELSE 'Medium'
    END as project_scale
FROM projects p;

-- Currency conversion view (example)
CREATE VIEW project_budget_usd AS
SELECT 
    p.name,
    p.budget as budget_thb,
    ROUND(p.budget / 35.0, 2) as budget_usd,  -- Approximate THB to USD conversion
    p.client,
    p.status
FROM projects p;

-- Final check
SELECT 'Company C Database Setup Complete' as status,
       (SELECT COUNT(*) FROM employees) as total_employees,
       (SELECT COUNT(*) FROM projects) as total_projects,
       (SELECT SUM(budget) FROM projects WHERE status = 'active') as active_budget_thb;

SELECT 'Enhanced Company C International Database Setup Complete' as status,
       (SELECT COUNT(*) FROM employees) as total_employees,
       (SELECT COUNT(*) FROM projects) as total_projects,
       (SELECT COUNT(*) FROM clients) as total_international_clients,
       (SELECT COUNT(DISTINCT country) FROM clients) as countries_served,
       (SELECT COUNT(DISTINCT currency) FROM clients) as currencies_supported,
       (SELECT COUNT(*) FROM international_contracts) as active_contracts,
       'Global Operations Ready' as office_status;