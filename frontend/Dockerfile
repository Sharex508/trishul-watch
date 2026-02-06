# Use a single stage build for simplicity
FROM node:18-alpine

# Add network configuration to help with IPv6 connectivity issues
ENV DOCKER_OPTS="--ipv6=false"
ENV NODE_ENV=development

WORKDIR /app

# Copy package files
COPY package.json package-lock.json* ./

# Install dependencies using npm install instead of npm ci
RUN npm install

# Copy application code
COPY . .

EXPOSE 3000

CMD ["npm", "start"]
