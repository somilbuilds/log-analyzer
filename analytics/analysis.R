#!/usr/bin/env Rscript
# ============================================================
# R Analytics — Time-Series Decomposition & Anomaly Detection
# ============================================================
#
# Connects to MongoDB to pull hourly aggregated request/error
# data, performs STL decomposition, flags anomalous hours, and
# produces diagnostic plots.
#
# Prerequisites:
#   install.packages(c("mongolite", "ggplot2", "jsonlite"))
#
# Usage:
#   Rscript analysis.R
#   # Or with custom MongoDB URI:
#   Rscript analysis.R --mongo-uri "mongodb://localhost:27018/log_analytics"
#
# Output files (saved to analytics/ directory):
#   - traffic_timeseries.png   : Hourly traffic with anomalies highlighted
#   - error_rate.png           : Error rate over time
#   - status_distribution.png  : Status code distribution bar chart
# ============================================================

# ─── Load Libraries ─────────────────────────────────────────
if (!requireNamespace("mongolite", quietly = TRUE)) {
  install.packages("mongolite", repos = "https://cloud.r-project.org")
}
if (!requireNamespace("ggplot2", quietly = TRUE)) {
  install.packages("ggplot2", repos = "https://cloud.r-project.org")
}
if (!requireNamespace("jsonlite", quietly = TRUE)) {
  install.packages("jsonlite", repos = "https://cloud.r-project.org")
}

library(mongolite)
library(ggplot2)
library(jsonlite)

# ─── Configuration ──────────────────────────────────────────
args <- commandArgs(trailingOnly = TRUE)
mongo_uri <- "mongodb://localhost:27018/log_analytics"
if (length(args) >= 2 && args[1] == "--mongo-uri") {
  mongo_uri <- args[2]
}

output_dir <- "."

cat("=== R Analytics: Time-Series Analysis & Anomaly Detection ===\n")
cat(paste("MongoDB URI:", mongo_uri, "\n"))
cat(paste("Output dir:", output_dir, "\n"))

# ─── Connect to MongoDB & Fetch Data ────────────────────────
cat("\n[1/5] Connecting to MongoDB...\n")
conn <- mongo(
  collection = "window_aggregates",
  db = "log_analytics",
  url = mongo_uri
)

# Fetch all window aggregate records
data <- conn$find('{}', sort = '{"window_id": 1}')
cat(paste("  Fetched", nrow(data), "window records\n"))

if (nrow(data) == 0) {
  cat("ERROR: No data found in MongoDB. Run the streaming job first.\n")
  quit(status = 1)
}

# ─── Prepare Data ───────────────────────────────────────────
cat("[2/5] Preparing time-series data...\n")

# Create sequential time index (each window = one time unit)
data$time_index <- seq_len(nrow(data))

# Extract key metrics
data$requests <- data$total_requests
data$error_rate_pct <- data$error_rate * 100

# Flatten status_distribution into separate columns for plotting
status_long <- data.frame(
  window_id = integer(),
  status_code = character(),
  count = integer(),
  stringsAsFactors = FALSE
)

for (i in seq_len(nrow(data))) {
  if ("status_distribution" %in% names(data)) {
    sd <- data$status_distribution[[i]]
    if (!is.null(sd) && length(sd) > 0) {
      codes <- names(sd)
      for (code in codes) {
        status_long <- rbind(status_long, data.frame(
          window_id = data$window_id[i],
          status_code = code,
          count = as.integer(sd[[code]]),
          stringsAsFactors = FALSE
        ))
      }
    }
  }
}

# ─── Time-Series Decomposition (STL) ────────────────────────
cat("[3/5] Performing STL decomposition...\n")

# Create a time series object
# We need at least 2 full periods for STL; set frequency based on data
# If we have enough windows, use a period of ~10 windows (representing
# one "cycle" in the replay)
n <- nrow(data)
freq <- min(max(4, n %/% 4), 20)  # adaptive frequency
if (n < 2 * freq) {
  freq <- max(2, n %/% 3)
}

ts_requests <- ts(data$requests, frequency = freq)

# STL decomposition: Seasonal + Trend + Remainder
# s.window = "periodic" uses a fixed seasonal pattern
# robust = TRUE uses robust fitting (resistant to outliers)
stl_result <- tryCatch({
  stl(ts_requests, s.window = "periodic", robust = TRUE)
}, error = function(e) {
  cat(paste("  STL with periodic failed:", e$message, "\n"))
  cat("  Falling back to simple moving average decomposition...\n")
  # Simple fallback: just decompose
  decompose(ts_requests, type = "additive")
})

# ─── Anomaly Detection ──────────────────────────────────────
cat("[4/5] Detecting anomalies...\n")

# Extract residuals (remainder component)
if (inherits(stl_result, "stl")) {
  residuals <- stl_result$time.series[, "remainder"]
  trend <- stl_result$time.series[, "trend"]
  seasonal <- stl_result$time.series[, "seasonal"]
} else {
  residuals <- stl_result$random
  trend <- stl_result$trend
  seasonal <- stl_result$seasonal
  # Replace NA with 0 for decompose results
  residuals[is.na(residuals)] <- 0
  trend[is.na(trend)] <- mean(data$requests, na.rm = TRUE)
  seasonal[is.na(seasonal)] <- 0
}

# Flag anomalies: residual > 2 standard deviations from mean
residual_mean <- mean(residuals, na.rm = TRUE)
residual_sd <- sd(residuals, na.rm = TRUE)
threshold <- 2  # number of SDs

data$residual <- as.numeric(residuals)
data$trend <- as.numeric(trend)
data$seasonal <- as.numeric(seasonal)
data$is_anomaly <- abs(data$residual - residual_mean) > threshold * residual_sd

anomaly_count <- sum(data$is_anomaly, na.rm = TRUE)
cat(paste("  Found", anomaly_count, "anomalous windows",
          "(threshold:", round(threshold * residual_sd, 1), ")\n"))

# ─── Generate Plots ─────────────────────────────────────────
cat("[5/5] Generating plots...\n")

# -- Plot 1: Traffic over time with anomalies highlighted --
p1 <- ggplot(data, aes(x = time_index, y = requests)) +
  geom_line(color = "#3498db", linewidth = 0.8) +
  geom_line(aes(y = trend), color = "#2ecc71", linewidth = 1.2,
            linetype = "dashed", alpha = 0.8) +
  geom_point(data = data[data$is_anomaly, ],
             aes(x = time_index, y = requests),
             color = "#e74c3c", size = 3, shape = 17) +
  labs(
    title = "Hourly Request Volume with Anomaly Detection",
    subtitle = paste("Green dashed = trend |",
                     "Red triangles =", anomaly_count, "anomalies detected"),
    x = "Window (Time)",
    y = "Request Count"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 14),
    plot.subtitle = element_text(color = "gray40")
  )

traffic_path <- file.path(output_dir, "traffic_timeseries.png")
ggsave(traffic_path, p1, width = 12, height = 6, dpi = 150)
cat(paste("  Saved:", traffic_path, "\n"))

# -- Plot 2: Error rate over time --
p2 <- ggplot(data, aes(x = time_index, y = error_rate_pct)) +
  geom_area(fill = "#e74c3c", alpha = 0.3) +
  geom_line(color = "#e74c3c", linewidth = 0.8) +
  geom_hline(yintercept = mean(data$error_rate_pct),
             linetype = "dashed", color = "#c0392b", linewidth = 0.5) +
  labs(
    title = "Error Rate Over Time",
    subtitle = paste("Dashed line = average error rate:",
                     round(mean(data$error_rate_pct), 2), "%"),
    x = "Window (Time)",
    y = "Error Rate (%)"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 14),
    plot.subtitle = element_text(color = "gray40")
  )

error_path <- file.path(output_dir, "error_rate.png")
ggsave(error_path, p2, width = 12, height = 6, dpi = 150)
cat(paste("  Saved:", error_path, "\n"))

# -- Plot 3: Status code distribution --
if (nrow(status_long) > 0) {
  status_agg <- aggregate(count ~ status_code, data = status_long, FUN = sum)
  status_agg <- status_agg[order(-status_agg$count), ]

  # Color by status code class
  status_agg$class <- ifelse(
    substr(status_agg$status_code, 1, 1) == "2", "2xx Success",
    ifelse(substr(status_agg$status_code, 1, 1) == "3", "3xx Redirect",
    ifelse(substr(status_agg$status_code, 1, 1) == "4", "4xx Client Error",
    ifelse(substr(status_agg$status_code, 1, 1) == "5", "5xx Server Error",
           "Other"))))

  p3 <- ggplot(status_agg, aes(x = reorder(status_code, -count),
                                y = count, fill = class)) +
    geom_bar(stat = "identity", alpha = 0.85) +
    scale_fill_manual(values = c(
      "2xx Success" = "#2ecc71",
      "3xx Redirect" = "#3498db",
      "4xx Client Error" = "#f39c12",
      "5xx Server Error" = "#e74c3c",
      "Other" = "#95a5a6"
    )) +
    labs(
      title = "HTTP Status Code Distribution",
      subtitle = paste("Total requests:", sum(status_agg$count)),
      x = "Status Code",
      y = "Count",
      fill = "Category"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title = element_text(face = "bold", size = 14),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )

  status_path <- file.path(output_dir, "status_distribution.png")
  ggsave(status_path, p3, width = 10, height = 6, dpi = 150)
  cat(paste("  Saved:", status_path, "\n"))
} else {
  cat("  WARNING: No status distribution data to plot.\n")
}

# ─── Summary ────────────────────────────────────────────────
cat("\n=== Analysis Complete ===\n")
cat(paste("Total windows analyzed:", n, "\n"))
cat(paste("Anomalies detected:", anomaly_count, "\n"))
cat(paste("Average requests/window:", round(mean(data$requests), 1), "\n"))
cat(paste("Average error rate:", round(mean(data$error_rate_pct), 2), "%\n"))
cat(paste("Output files in:", output_dir, "\n"))

# Close MongoDB connection
conn$disconnect()
