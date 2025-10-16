# Dedicated Dockerfile for the Python web app image
# This allows building and pushing the app independently.

# Stage 1: prepare Pixi-managed web environment
FROM ghcr.io/prefix-dev/pixi:0.39.5-noble AS install

WORKDIR /app

# Only what we need to resolve the environment
COPY pyproject.toml .
COPY pixi.lock .

# Use cache for rattler/pixi package downloads
RUN --mount=type=cache,target=/root/.cache/rattler/cache,sharing=private pixi install -e web

# Create a shell hook that activates the web environment
RUN pixi shell-hook -e web > /shell-hook
RUN echo 'exec "$@"' >> /shell-hook

# Stage 2: runtime image for the web app
FROM ubuntu:24.04 AS web

# Copy only the production environment and shell hook into the runtime image
COPY --from=install /app/.pixi/envs/web /app/.pixi/envs/web
COPY --from=install /shell-hook /shell-hook

# Create non-root user (optional; currently not switching to it to keep parity with existing image)
RUN groupadd -r user \
 && useradd --create-home --home-dir /home/user -g user -s /bin/bash user

# Copy application source
COPY app/ /app/web/

WORKDIR /app/web

# Expose the default app port
EXPOSE 8000/tcp

# Entry and default command: gunicorn serving main:app
ENTRYPOINT ["/bin/bash", "/shell-hook"]
CMD ["gunicorn", "-b", "0.0.0.0:8000", "--timeout", "120", "--workers", "4", "--access-logfile", "-", "main:app"]
