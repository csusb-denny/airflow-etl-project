-- Creates a new file "create_tables.sql" inside includes/sql
-- writing to destination
-- EOF tells shell to take everything until u see EOF (End of File)
cat > includes/sql/create_tables.sql << 'EOF'
CREATE TABLE IF NOT EXISTS weather_hourly (
	ts TIMESTAMP PRIMARY KEY,		--time stamp (unique key fo reach weather record)
	temperature_c DOUBLE PRECISION,		--temp in celc	
	windspeed_ms DOUBLE PRECISION		-- wind speed in meters/second
);

CREATE TABLE IF NOT EXISTS finance_daily(
	trade_date DATE PRIMARY KEY,		--date of fin data (unique key)
	symbol TEXT NOT NULL,			--stock ticker symbol
	open DOUBLE PRECISION,			--opening price	
	high DOUBLE PRECISION,			--highest of the day
	low DOUBLE PRECISION,			--lowest
	close DOUBLE PRECISION,			--closing price
	volume BIGINT				-- trading volume
);
EOF
