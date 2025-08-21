FROM node:20-alpine

# Install OpenSSL for Prisma
RUN apk add --no-cache openssl

WORKDIR /app

# Copy package files - ให้ชัดเจนว่า copy ทั้ง package.json และ package-lock.json
COPY package.json package-lock.json ./
COPY prisma ./prisma/

# Install dependencies (ใช้ --omit=dev แทน --only=production)
RUN npm ci --omit=dev

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