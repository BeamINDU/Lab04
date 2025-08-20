// lib/services/DatabaseService.ts - Fixed CREATE TABLE Implementation
import { Pool } from 'pg';
import { getCompanyDatabase } from '../database';

export interface DatabaseColumn {
  name: string;
  type: string;
  length?: number;
  isPrimary?: boolean;
  isRequired?: boolean;
  isUnique?: boolean;
  defaultValue?: string;
  comment?: string;
  references?: {
    table: string;
    column: string;
  };
}

export interface CreateTableOptions {
  companyCode: string;
  schema: string;
  tableName: string;
  description?: string;
  columns: DatabaseColumn[];
  ifNotExists?: boolean;
}

export interface SchemaInfo {
  name: string;
  description?: string;
  tables: TableInfo[];
  createdAt?: Date;
}

export interface TableInfo {
  name: string;
  schema: string;
  comment?: string;
  columnCount: number;
  hasData: boolean;
  createdAt?: Date;
}

export interface ImportResult {
  success: boolean;
  totalRows: number;
  successRows: number;
  errorRows: number;
  errors: Array<{
    row: number;
    column?: string;
    error: string;
    data?: any;
  }>;
  executionTime: number;
}

/**
 * Enhanced Database Service สำหรับจัดการ database operations
 * Fixed: CREATE TABLE syntax error โดยไม่ใช้ parameterized query
 */
export class DatabaseService {
  private _pool: Pool;
  private companyCode: string;

  constructor(companyCode: string) {
    this.companyCode = companyCode;
    this._pool = getCompanyDatabase(companyCode);
  }

  /**
   * Get database pool for direct queries (used by FileImportService)
   */
  get pool(): Pool {
    return this._pool;
  }

  /**
   * ดึงรายชื่อ schema ทั้งหมดที่ user สามารถเข้าถึงได้
   */
  async getSchemas(): Promise<SchemaInfo[]> {
    try {
      const query = `
        SELECT 
          schema_name,
          COALESCE(
            (SELECT description FROM pg_description 
             WHERE objoid = (SELECT oid FROM pg_namespace WHERE nspname = schema_name)
             AND objsubid = 0), 
            null
          ) as description
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name;
      `;

      const result = await this._pool.query(query);
      
      const schemas: SchemaInfo[] = [];
      
      for (const row of result.rows) {
        const tables = await this.getTablesInSchema(row.schema_name);
        schemas.push({
          name: row.schema_name,
          description: row.description,
          tables
        });
      }

      return schemas;
    } catch (error) {
      throw new Error(`Failed to fetch schemas: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  /**
   * สร้างตารางใหม่ - Fixed: ตรวจสอบว่า schema มีอยู่จริงก่อน
   */
  async createTable(options: CreateTableOptions): Promise<void> {
    const { schema, tableName, description, columns, ifNotExists = true } = options;
    
    // Validate input
    this.validateTableCreation(options);
    
    const client = await this._pool.connect();
    
    try {
      await client.query('BEGIN');
      
      // ตรวจสอบว่า schema มีอยู่จริง หากไม่มีให้สร้างก่อน
      const schemaExistsQuery = `
        SELECT EXISTS (
          SELECT 1 FROM information_schema.schemata 
          WHERE schema_name = $1
        );
      `;
      const schemaResult = await client.query(schemaExistsQuery, [schema]);
      
      if (!schemaResult.rows[0].exists) {
        // สร้าง schema ก่อน
        const createSchemaSQL = `CREATE SCHEMA "${this.sanitizeIdentifier(schema)}";`;
        await client.query(createSchemaSQL);
        console.log(`✅ Schema "${schema}" created automatically`);
      }
      
      // สร้าง SQL สำหรับการสร้างตาราง (ไม่ใช้ parameters)
      const createTableSQL = this.generateCreateTableSQL(schema, tableName, columns, ifNotExists);
      
      console.log('=== CREATE TABLE SQL ===');
      console.log(createTableSQL);
      console.log('========================');
      
      // Execute SQL โดยไม่ใช้ parameters เพราะ CREATE TABLE ไม่รองรับ
      await client.query(createTableSQL);
      
      // เพิ่มคำอธิบายของตาราง (ใช้ string concatenation แทน parameters)
      if (description) {
        const sanitizedDescription = description.replace(/'/g, "''"); // Escape single quotes
        const commentSQL = `COMMENT ON TABLE "${this.sanitizeIdentifier(schema)}"."${this.sanitizeIdentifier(tableName)}" IS '${sanitizedDescription}';`;
        await client.query(commentSQL);
      }
      
      // เพิ่มคำอธิบายของ columns (ใช้ string concatenation แทน parameters)
      for (const column of columns) {
        if (column.comment) {
          const sanitizedComment = column.comment.replace(/'/g, "''"); // Escape single quotes
          const columnCommentSQL = `COMMENT ON COLUMN "${this.sanitizeIdentifier(schema)}"."${this.sanitizeIdentifier(tableName)}"."${this.sanitizeIdentifier(column.name)}" IS '${sanitizedComment}';`;
          await client.query(columnCommentSQL);
        }
      }
      
      await client.query('COMMIT');
      console.log(`✅ Table "${schema}"."${tableName}" created successfully for company ${this.companyCode}`);
      
    } catch (error) {
      await client.query('ROLLBACK');
      console.error('❌ Error creating table:', error);
      console.error('❌ Failed SQL:', this.generateCreateTableSQL(schema, tableName, columns, ifNotExists));
      
      // ให้ข้อมูล error ที่ละเอียดขึ้น
      if (error instanceof Error) {
        throw new Error(`Failed to create table "${tableName}": ${error.message}`);
      } else {
        throw new Error(`Failed to create table "${tableName}": Unknown error`);
      }
    } finally {
      client.release();
    }
  }

  /**
   * สร้าง CREATE TABLE SQL - Fixed: ไม่ใช้ parameters
   */
  private generateCreateTableSQL(
    schema: string, 
    tableName: string, 
    columns: DatabaseColumn[], 
    ifNotExists: boolean
  ): string {
    // สร้าง column definitions
    const columnDefs = columns.map(col => {
      let def = `"${this.sanitizeIdentifier(col.name)}" ${this.buildColumnType(col)}`;
      
      // เพิ่ม constraints
      if (col.isRequired) {
        def += ' NOT NULL';
      }
      
      if (col.isUnique && !col.isPrimary) {
        def += ' UNIQUE';
      }
      
      if (col.defaultValue !== undefined && col.defaultValue !== '') {
        // Handle different default value types
        if (col.type.toUpperCase().includes('CHAR') || 
            col.type.toUpperCase().includes('TEXT') ||
            col.type.toUpperCase().includes('VARCHAR')) {
          def += ` DEFAULT '${this.sanitizeStringValue(col.defaultValue)}'`;
        } else if (col.type.toUpperCase().includes('BOOL')) {
          def += ` DEFAULT ${col.defaultValue.toLowerCase()}`;
        } else if (col.type.toUpperCase().includes('TIMESTAMP')) {
          if (col.defaultValue.toUpperCase() === 'NOW()' || 
              col.defaultValue.toUpperCase() === 'CURRENT_TIMESTAMP') {
            def += ` DEFAULT CURRENT_TIMESTAMP`;
          } else {
            def += ` DEFAULT '${this.sanitizeStringValue(col.defaultValue)}'`;
          }
        } else {
          def += ` DEFAULT ${col.defaultValue}`;
        }
      }
      
      return def;
    }).join(',\n    ');
    
    // สร้าง primary key constraint
    const primaryKeys = columns
      .filter(col => col.isPrimary)
      .map(col => `"${this.sanitizeIdentifier(col.name)}"`);
    
    let constraintDefs = '';
    if (primaryKeys.length > 0) {
      constraintDefs = `,\n    PRIMARY KEY (${primaryKeys.join(', ')})`;
    }
    
    // สร้าง foreign key constraints
    const foreignKeys = columns
      .filter(col => col.references)
      .map(col => {
        const ref = col.references!;
        return `    FOREIGN KEY ("${this.sanitizeIdentifier(col.name)}") REFERENCES "${this.sanitizeIdentifier(ref.table)}" ("${this.sanitizeIdentifier(ref.column)}")`;
      });
    
    if (foreignKeys.length > 0) {
      constraintDefs += ',\n' + foreignKeys.join(',\n');
    }
    
    // สร้าง SQL statement สุดท้าย
    const ifNotExistsClause = ifNotExists ? 'IF NOT EXISTS ' : '';
    
    const sql = `CREATE TABLE ${ifNotExistsClause}"${this.sanitizeIdentifier(schema)}"."${this.sanitizeIdentifier(tableName)}" (
    ${columnDefs}${constraintDefs}
);`;
    
    return sql;
  }

  /**
   * สร้าง column type definition
   */
  private buildColumnType(col: DatabaseColumn): string {
    let type = col.type.toUpperCase();
    
    // Handle types with length
    if (col.length && (
      type.includes('VARCHAR') || 
      type.includes('CHAR') || 
      type.includes('DECIMAL') || 
      type.includes('NUMERIC')
    )) {
      return `${type}(${col.length})`;
    }
    
    return type;
  }

  /**
   * ทำความสะอาด identifier (table name, column name, etc.)
   */
  private sanitizeIdentifier(identifier: string): string {
    // Remove any non-alphanumeric characters except underscore
    // Keep only letters, numbers, and underscores
    return identifier.replace(/[^a-zA-Z0-9_]/g, '');
  }

  /**
   * ทำความสะอาด string values
   */
  private sanitizeStringValue(value: string): string {
    // Escape single quotes by doubling them
    return value.replace(/'/g, "''");
  }

  /**
   * ลบตารางพร้อมตรวจสอบ dependencies
   */
  async dropTable(schema: string, tableName: string, cascade: boolean = false): Promise<void> {
    const client = await this._pool.connect();
    
    try {
      await client.query('BEGIN');
      
      // ตรวจสอบว่าตารางมีอยู่จริง
      const existsQuery = `
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_schema = $1 AND table_name = $2
        );
      `;
      const existsResult = await client.query(existsQuery, [schema, tableName]);
      
      if (!existsResult.rows[0].exists) {
        throw new Error(`Table "${schema}"."${tableName}" does not exist`);
      }
      
      const dropSQL = `DROP TABLE "${this.sanitizeIdentifier(schema)}"."${this.sanitizeIdentifier(tableName)}" ${cascade ? 'CASCADE' : 'RESTRICT'};`;
      await client.query(dropSQL);
      
      await client.query('COMMIT');
      console.log(`✅ Table "${schema}"."${tableName}" dropped successfully for company ${this.companyCode}`);
      
    } catch (error) {
      await client.query('ROLLBACK');
      throw new Error(`Failed to drop table: ${error instanceof Error ? error.message : 'Unknown error'}`);
    } finally {
      client.release();
    }
  }

  /**
   * ดึงรายชื่อตารางใน schema
   */
  async getTablesInSchema(schemaName: string): Promise<TableInfo[]> {
    try {
      // Query with enhanced information
      const query = `
        SELECT 
          t.table_name,
          t.table_schema,
          COALESCE(
            (SELECT description FROM pg_description d 
             JOIN pg_class c ON c.oid = d.objoid 
             JOIN pg_namespace n ON n.oid = c.relnamespace 
             WHERE n.nspname = t.table_schema 
             AND c.relname = t.table_name 
             AND d.objsubid = 0),
            null
          ) as comment,
          (
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_schema = t.table_schema 
            AND table_name = t.table_name
          ) as column_count
        FROM information_schema.tables t
        WHERE t.table_schema = $1 
        AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name;
      `;
      
      const result = await this.pool.query(query, [schemaName]);
      const tables: TableInfo[] = [];
      
      for (const row of result.rows) {
        // Check if table has data
        let hasData = false;
        try {
          const dataCheckQuery = `SELECT EXISTS(SELECT 1 FROM "${row.table_schema}"."${row.table_name}" LIMIT 1);`;
          const dataResult = await this.pool.query(dataCheckQuery);
          hasData = dataResult.rows[0].exists;
        } catch (error) {
          // ถ้าเช็คไม่ได้ (เช่น permissions) ให้ใส่ false
          hasData = false;
        }
        
        tables.push({
          name: row.table_name,
          schema: row.table_schema,
          comment: row.comment || undefined,
          columnCount: parseInt(row.column_count) || 0,
          hasData
        });
      }
      
      return tables;
    } catch (error) {
      // ถ้า query ล้มเหลว ให้ใช้วิธีง่ายๆ แทน
      const simpleQuery = `
        SELECT 
          table_name,
          table_schema
        FROM information_schema.tables 
        WHERE table_schema = $1 AND table_type = 'BASE TABLE'
        ORDER BY table_name;
      `;
      
      const result = await this.pool.query(simpleQuery, [schemaName]);
      
      return result.rows.map(row => ({
        name: row.table_name,
        schema: row.table_schema,
        comment: undefined,
        columnCount: 0,
        hasData: false
      }));
    }
  }

  /**
   * สร้าง schema ใหม่พร้อมการตรวจสอบความปลอดภัย
   */
  async createSchema(name: string, description?: string): Promise<void> {
    const client = await this.pool.connect();
    
    try {
      await client.query('BEGIN');
      
      // สร้าง schema (ไม่ใช้ parameters)
      const createQuery = `CREATE SCHEMA IF NOT EXISTS "${this.sanitizeIdentifier(name)}";`;
      await client.query(createQuery);
      
      // เพิ่มคำอธิบาย (ถ้ามี) - ใช้ parameters ได้
      if (description) {
        const commentQuery = `COMMENT ON SCHEMA "${this.sanitizeIdentifier(name)}" IS $1;`;
        await client.query(commentQuery, [description]);
      }
      
      await client.query('COMMIT');
      console.log(`✅ Schema "${name}" created successfully for company ${this.companyCode}`);
      
    } catch (error) {
      await client.query('ROLLBACK');
      throw new Error(`Failed to create schema: ${error instanceof Error ? error.message : 'Unknown error'}`);
    } finally {
      client.release();
    }
  }

  /**
   * ลบ schema พร้อมตรวจสอบความปลอดภัย
   */
  async dropSchema(name: string, cascade: boolean = false): Promise<void> {
    // ป้องกันการลบ schema ระบบ
    const protectedSchemas = ['public', 'information_schema', 'pg_catalog', 'pg_toast'];
    if (protectedSchemas.includes(name.toLowerCase())) {
      throw new Error(`Cannot drop protected schema: ${name}`);
    }

    const client = await this.pool.connect();
    
    try {
      await client.query('BEGIN');
      
      const dropQuery = `DROP SCHEMA "${this.sanitizeIdentifier(name)}" ${cascade ? 'CASCADE' : 'RESTRICT'};`;
      await client.query(dropQuery);
      
      await client.query('COMMIT');
      console.log(`✅ Schema "${name}" dropped successfully for company ${this.companyCode}`);
      
    } catch (error) {
      await client.query('ROLLBACK');
      throw new Error(`Failed to drop schema: ${error instanceof Error ? error.message : 'Unknown error'}`);
    } finally {
      client.release();
    }
  }

  /**
   * Validate การสร้างตาราง
   */
  private validateTableCreation(options: CreateTableOptions): void {
    const { tableName, columns } = options;
    
    if (!tableName || tableName.trim().length === 0) {
      throw new Error('Table name is required');
    }
    
    // ตรวจสอบชื่อตารางว่าเป็น valid identifier
    if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(tableName)) {
      throw new Error('Table name must start with a letter and contain only letters, numbers, and underscores');
    }
    
    if (!columns || columns.length === 0) {
      throw new Error('At least one column is required');
    }
    
    // ตรวจสอบ column names
    for (const col of columns) {
      if (!col.name || col.name.trim().length === 0) {
        throw new Error('All columns must have names');
      }
      
      if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(col.name)) {
        throw new Error(`Invalid column name "${col.name}". Must start with a letter and contain only letters, numbers, and underscores`);
      }
      
      if (!col.type || col.type.trim().length === 0) {
        throw new Error(`Column "${col.name}" must have a data type`);
      }
    }
    
    // ตรวจสอบ primary keys
    const primaryKeys = columns.filter(col => col.isPrimary);
    if (primaryKeys.length === 0) {
      console.warn('⚠️ No primary key defined for table. Consider adding one for better performance.');
    }
  }

  /**
   * ปิดการเชื่อมต่อ
   */
  async close(): Promise<void> {
    await this.pool.end();
  }
}