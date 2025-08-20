export interface AppError {
  message: string;
  code?: string;
  statusCode?: number;
}

export class DatabaseError extends Error {
  code?: string;
  statusCode: number;

  constructor(message: string, code?: string, statusCode: number = 500) {
    super(message);
    this.name = 'DatabaseError';
    this.code = code;
    this.statusCode = statusCode;
  }
}

export class ValidationError extends Error {
  field?: string;
  statusCode: number;

  constructor(message: string, field?: string, statusCode: number = 400) {
    super(message);
    this.name = 'ValidationError';
    this.field = field;
    this.statusCode = statusCode;
  }
}

// Helper function สำหรับ error handling
export function handleApiError(error: unknown): { message: string; statusCode: number } {
  if (error instanceof DatabaseError || error instanceof ValidationError) {
    return {
      message: error.message,
      statusCode: error.statusCode
    };
  }

  if (error instanceof Error) {
    return {
      message: error.message,
      statusCode: 500
    };
  }

  return {
    message: 'An unknown error occurred',
    statusCode: 500
  };
}