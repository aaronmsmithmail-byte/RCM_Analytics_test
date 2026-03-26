// Cube.js configuration for Healthcare RCM Analytics
// Uses DuckDB as the database backend (Snowflake-like columnar engine)
module.exports = {
  dbType: 'duckdb',
  dbDuckdbDatabasePath: '/cube/data/rcm_analytics.db',
  devServer: true,
  apiSecret: process.env.CUBEJS_API_SECRET || 'rcm_analytics_dev_secret',
  schemaPath: 'model',
};
