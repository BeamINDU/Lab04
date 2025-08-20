// utils/sqlGenerator.ts
import { Column } from '../types/schema';

export class SQLGenerator {
  static generateCreateTable(
    tableName: string, 
    columns: Column[], 
    schema: string = 'public'
  ): string {
    const columnDefs = columns.map(col => this.generateColumnDefinition(col));
    const foreignKeys = this.generateForeignKeys(columns);

    let sql = `CREATE TABLE ${schema}.${tableName} (\n  ${columnDefs.join(',\n  ')}`;
    
    if (foreignKeys.length > 0) {
      sql += `,\n  ${foreignKeys.join(',\n  ')}`;
    }
    
    sql += '\n);';
    return sql;
  }

  private static generateColumnDefinition(column: Column): string {
    let def = `${column.name} ${column.type}`;
    
    // Add length for VARCHAR, CHAR, etc.
    if (column.type === 'VARCHAR' && column.length) {
      def += `(${column.length})`;
    }
    
    // Add constraints
    if (column.isPrimary) {
      def += ' PRIMARY KEY';
    }
    
    if (column.isRequired && !column.isPrimary) {
      def += ' NOT NULL';
    }
    
    if (column.isUnique && !column.isPrimary) {
      def += ' UNIQUE';
    }
    
    if (column.defaultValue && !column.isPrimary) {
      def += ` DEFAULT ${column.defaultValue}`;
    }
    
    return def;
  }

  private static generateForeignKeys(columns: Column[]): string[] {
    return columns
      .filter(col => col.references?.table && col.references?.column)
      .map(col => 
        `FOREIGN KEY (${col.name}) REFERENCES ${col.references!.table}(${col.references!.column})`
      );
  }

  static generateDropTable(tableName: string, schema: string = 'public', cascade: boolean = false): string {
    return `DROP TABLE ${schema}.${tableName}${cascade ? ' CASCADE' : ' RESTRICT'};`;
  }

  static generateCreateSchema(schemaName: string, description?: string): string {
    let sql = `CREATE SCHEMA IF NOT EXISTS "${schemaName}";`;
    
    if (description) {
      sql += `\nCOMMENT ON SCHEMA "${schemaName}" IS '${description}';`;
    }
    
    return sql;
  }

  static generateDropSchema(schemaName: string, cascade: boolean = false): string {
    return `DROP SCHEMA "${schemaName}"${cascade ? ' CASCADE' : ' RESTRICT'};`;
  }
}
