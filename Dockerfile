FROM ghcr.io/berriai/litellm:main-latest

# Copy config file
COPY litellm_config.yaml /app/litellm_config.yaml

# Set working directory
WORKDIR /app

# Expose port
EXPOSE 4000

# Run litellm
CMD ["--config", "/app/litellm_config.yaml", "--port", "4000", "--num_workers", "1"]