FROM node:18-alpine

# Install OpenSSL for Prisma
RUN apk add --no-cache openssl

WORKDIR /app

# Copy package files
COPY package*.json ./
COPY prisma ./prisma/

# Install dependencies
RUN npm ci --only=production

# Generate Prisma Client
RUN npx prisma generate

# Copy scripts และ source code
COPY scripts ./scripts/
COPY . .

# Build the application
RUN npm run build

# 🔥 รัน Database Setup ตอน Build (ถ้า DATABASE_URL available)
RUN if [ "$NODE_ENV" = "production" ]; then \
      npx prisma db push && \
      node scripts/setup.js; \
    fi

# Expose port
EXPOSE 3001

# Start the application
CMD ["npm", "start"]