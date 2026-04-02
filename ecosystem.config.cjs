module.exports = {
  apps: [
    {
      name: "sciscidb-dagster",
      script: "/users/j/s/jstonge1/.local/bin/uv",
      args: "run dg dev -p 3017",
      cwd: "/users/j/s/jstonge1/scisciDB",
      interpreter: "none",
      env: {
        DAGSTER_HOME: "/users/j/s/jstonge1/scisciDB/.dagster",
        HOME: "/users/j/s/jstonge1",
        PATH: "/users/j/s/jstonge1/.local/bin:/users/j/s/jstonge1/miniconda3/bin:/usr/local/bin:/usr/bin:/bin",
      },
      autorestart: true,
      restart_delay: 5000,     // wait 5s before restarting on crash
      max_restarts: 10,         // stop retrying after 10 crashes
      min_uptime: "30s",        // must stay up 30s to count as "stable"
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      out_file: "/users/j/s/jstonge1/.pm2/logs/sciscidb-dagster-out.log",
      error_file: "/users/j/s/jstonge1/.pm2/logs/sciscidb-dagster-error.log",
    },
  ],
};
