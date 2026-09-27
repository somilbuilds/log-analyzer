FROM rocker/r-ver:4.3.1

# Install system dependencies required for mongolite and plotting
RUN apt-get update && apt-get install -y \
    libssl-dev \
    libsasl2-dev \
    libcurl4-openssl-dev \
    zlib1g-dev \
    libxml2-dev \
    pkg-config \
    libfontconfig1-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    libfreetype6-dev \
    libpng-dev \
    libtiff5-dev \
    libjpeg-dev \
    && rm -rf /var/lib/apt/lists/*

# Install R packages
RUN Rscript -e "install.packages(c('mongolite', 'ggplot2', 'jsonlite'), repos='https://cloud.r-project.org')"

# Default working directory
WORKDIR /app

# Copy application files
COPY . /app

# Entry point
CMD ["Rscript", "analysis.R", "--mongo-uri", "mongodb://mongo:27017/log_analytics"]
