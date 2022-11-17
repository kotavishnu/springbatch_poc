package com.spring.etl.manager.partition;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcOperations;

/**
 * Simple minded partitioner for a range of values of a column in a database table. Works
 * best if the values are uniformly distributed (e.g. auto-generated primary key values).
 *
 * @author Dave Syer
 *
 */
public class ColumnRangePartitioner implements Partitioner {

	private final JdbcOperations jdbcTemplate;

	private final String table;

	private final String column;

	public ColumnRangePartitioner(JdbcOperations jdbcTemplate, String table, String column) {
		this.jdbcTemplate = jdbcTemplate;
		this.table = table;
		this.column = column;
	}

	/**
	 * Partition a database table assuming that the data in the column specified are
	 * uniformly distributed. The execution context values will have keys
	 * <code>minValue</code> and <code>maxValue</code> specifying the range of
	 * values to consider in each partition.
	 *
	 * @see Partitioner#partition(int)
	 */
	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		int min = jdbcTemplate.queryForObject("SELECT MIN(" + column + ") from ( " + table + " ) as aliastab ", Integer.class);
		int max = jdbcTemplate.queryForObject("SELECT MAX(" + column + ") from ( " + table + " ) as aliastab ", Integer.class);
		int targetSize = (max - min) / gridSize + 1;

		Map<String, ExecutionContext> result = new HashMap<>();
		int number = 0;
		int start = min;
		int end = start + targetSize - 1;

		while (start <= max) {
			ExecutionContext value = new ExecutionContext();
			result.put("partition" + number, value);

			if (end >= max) {
				end = max;
			}
			value.putInt("minValue", start);
			value.putInt("maxValue", end);
			start += targetSize;
			end += targetSize;
			number++;
		}

		return result;
	}
}