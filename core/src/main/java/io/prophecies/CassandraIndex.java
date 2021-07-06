package io.prophecies;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;

import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CassandraIndex {
	private final Cassandra cassandra;
	private final IndexConfig indexConfig;

	public CassandraIndex(Cassandra cassandra, IndexConfig indexConfig) {
		this.cassandra = cassandra;
		this.indexConfig = indexConfig;
	}

	public boolean insert(String sql, Consumer<ICassandraSettableData<?>> statementBinder) {
		try (CassandraBatch batch = cassandra.batch()) {
			batch.byIndex(indexConfig).insert(sql, statementBinder);
			return batch.wasApplied();
		}
	}

	public boolean delete(String sql, Consumer<ICassandraSettableData<?>> statementBinder) {
		try (CassandraBatch batch = cassandra.batch()) {
			batch.byIndex(indexConfig).delete(sql, statementBinder);
			return batch.wasApplied();
		}
	}

	public CassandraBatchResultSet select(String query, Consumer<ICassandraSettableData<?>> statementBinder) {
		return select(query, statementBinder, null);
	}

	public CassandraBatchResultSet select(String query, Consumer<ICassandraSettableData<?>> statementBinder, Integer limit) {
		ResultSet indexResult = cassandra.execute(query, statementBinder, limit);
		return getBaseIndexResults(indexConfig, StreamSupport.stream(indexResult.spliterator(), true));
	}

	/**
	 * The query field :partition will be set by this method
	 */
	public CassandraBatchResultSet partitionSelect(String query, Consumer<ICassandraSettableData<?>> statementBinder) {
		return partitionSelect(query, statementBinder, null);
	}

	/**
	 * The query field :partition will be set by this method
	 */
	public CassandraBatchResultSet partitionSelect(String query, Consumer<ICassandraSettableData<?>> statementBinder, Integer limit) {
		CassandraBatchResultSet indexResult = cassandra.partitionExecute(query, statementBinder, limit);

		return getBaseIndexResults(indexConfig, indexResult.stream());
	}

	private CassandraBatchResultSet getBaseIndexResults(IndexConfig indexConfig, Stream<Row> indexResult) {
		//Might be a mis-use of a batch since it might never be closed
		CassandraBatch batch = cassandra.batch();
		try {
			indexResult.forEach(row -> {
				batch.execute(indexConfig.buildSelectCql(), stmt -> {
					indexConfig.baseIndex.getFields().forEach(field -> {
						DataType type = row.getType(field);
						TypeCodec<Object> codec = stmt.codecRegistry().codecFor(type);
						stmt.set(field, row.get(field, codec), codec);
					});
				});
			});
			return batch.results();
		} catch (RuntimeException ex) {
			batch.close();
			throw ex;
		}
	}

}
