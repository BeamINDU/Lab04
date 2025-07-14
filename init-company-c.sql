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