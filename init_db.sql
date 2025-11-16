CREATE TABLE IF NOT EXISTS weather_logs (
  id SERIAL PRIMARY KEY,
  station_id TEXT NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  temperature DOUBLE PRECISION,
  humidity DOUBLE PRECISION,
  pressure DOUBLE PRECISION,
  status TEXT NOT NULL,     
  error_message TEXT,       
  raw_payload JSONB,       
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_weather_station_ts
ON weather_logs (station_id, timestamp);
