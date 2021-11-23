package org.example.ste.streamabletableexporter.dao;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.stream.Stream;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class StreamableTableToCvsExportRepository {

	private static final CvsRowMapper ROW_MAPPER = new CvsRowMapper(true);
	private static final int FETCH_SIZE = 100;
	private static final char SEPARATOR = ',';
	private JdbcTemplate jdbcTemplate;

	public StreamableTableToCvsExportRepository(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public Stream<String> export(String table, String condition) {
		validateTable(table);
		validateCondition(condition);
		String sql = "SELECT * FROM " + table + " WHERE" + condition;
		return createStream(sql);
	}

	public Stream<String> export(String table) {
		validateTable(table);
		String sql = "SELECT * FROM " + table;
		return createStream(sql);
	}

	private Stream<String> createStream(String sql) {
		jdbcTemplate.setFetchSize(FETCH_SIZE);
		return jdbcTemplate.queryForStream(sql, ROW_MAPPER);
	}

	private static final class CvsRowMapper implements RowMapper<String> {
		public CvsRowMapper(boolean printHeader) {
			this.printHeader = printHeader;
		}

		private final boolean printHeader;

		@Override
		public String mapRow(ResultSet resultSet, int rowNum) throws SQLException {
			StringBuilder builder = new StringBuilder();
			ResultSetMetaData metaData = resultSet.getMetaData();
			if (printHeader && rowNum == 0) {
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					if (i > 1) {
						builder.append(SEPARATOR);
					}
					builder.append(metaData.getColumnName(i));
				}
				builder.append('\n');
			}
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				if (i > 1) {
					builder.append(SEPARATOR);
				}
				Object obj = resultSet.getObject(i);
				if (obj != null) {
					builder.append(obj.toString());
				}
			}
			return builder.toString();
		}
	}

	private void validateTable(String table) {
		// TODO validate that it is a valid table name
	}

	private void validateCondition(String condition) {
		// TODO validate condition to avoid sql injection
	}

}
