package io.prophecies;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

@Singleton
public class Cassandra {

	protected final CqlSession session;
	private final ICassandraConfig cassandraConfig;

	@Inject
	public Cassandra(CqlSession session, ICassandraConfig cassandraConfig) {
		this.session = session;
		this.cassandraConfig = cassandraConfig;
	}

	public CassandraResultSet insert(String table, Consumer<ICassandraSettableData<?>> statementBinder) {
		return execute(buildInsertSql(table, statementBinder), statementBinder);
	}

	protected String buildInsertSql(String table, Consumer<ICassandraSettableData<?>> statementBinder) {
		InsertStatementCreator insertStatementCreator = new InsertStatementCreator();
		statementBinder.accept(insertStatementCreator);
		return "INSERT INTO " + table + " " + insertStatementCreator.getInsertStatement();
	}

	public CassandraResultSet select(String table, Consumer<WhereStatementCreator> statementBinder) {
		return select(table, statementBinder, 0);
	}

	public CassandraResultSet select(String table, Consumer<WhereStatementCreator> statementBinder, int limit) {
		WhereStatementCreatorImpl statementCreator = new WhereStatementCreatorImpl();
		statementBinder.accept(statementCreator);
		StringBuilder sql = new StringBuilder("SELECT * FROM " + table + " WHERE " + statementCreator.getSelectStatement());
		if (limit > 0) {
			sql.append(" LIMIT ")
				.append(limit);
		}

		return execute(sql.toString(), statementCreator.getConsumer());
	}

	public CassandraResultSet delete(String table, Consumer<WhereStatementCreator> statementBinder) {
		WhereStatementCreatorImpl statementCreator = new WhereStatementCreatorImpl();
		statementBinder.accept(statementCreator);
		String sql = "DELETE FROM " + table + " WHERE " + statementCreator.getSelectStatement();
		return execute(sql, statementCreator.getConsumer());
	}

	public CassandraResultSet execute(String query, Consumer<ICassandraSettableData<?>> statementBinder) {
		try {
			CassandraSettableData stmt = new CassandraSettableData(session.prepare(query).bind());
			statementBinder.accept(stmt);
			return new CassandraResultSet(session.execute(stmt.unwrapStatement()));
		} catch (InvalidQueryException exception) {
			throw new RuntimeException("Invalid query: "+query+". See: "+exception.getMessage(), exception);
		}
	}

	public ResultSet execute(String query, Consumer<ICassandraSettableData<?>> statementBinder, Integer limit) {
		return appendLimitIfNeeded(q -> execute(q.sql, q.statementBinder), query, statementBinder, limit);
	}

	/**
	 * The query field :partition will be set by this method
	 */
	public CassandraBatchResultSet partitionExecute(String query, Consumer<ICassandraSettableData<?>> statementBinder) {
		return partitionExecute(query, statementBinder, null);
	}

	/**
	 * The query field :partition will be set by this method
	 */
	public CassandraBatchResultSet partitionExecute(String query, Consumer<ICassandraSettableData<?>> statementBinder, Integer limit) {
		return batch().partitionExecute(query, statementBinder, limit).results();
	}

	public CassandraIndex byIndex(IndexConfig indexConfig) {
		return new CassandraIndex(this, indexConfig);
	}

	CompletionStage<AsyncResultSet> executeAsync(String query, Consumer<ICassandraSettableData<?>> statementBinder) {
		CassandraSettableData stmt = new CassandraSettableData(session.prepare(query).bind());
		statementBinder.accept(stmt);
		return session.executeAsync(stmt.unwrapStatement());
	}

	CompletionStage<AsyncResultSet> executeAsync(String query, Consumer<ICassandraSettableData<?>> statementBinder, Integer limit) {
		return appendLimitIfNeeded(q -> executeAsync(q.sql, q.statementBinder), query, statementBinder, limit);
	}

	public CassandraBatch batch() {
		return new CassandraBatch(this);
	}

	public CassandraBatch slowBatch() {
		return new CassandraBatch(this, true);
	}

	static class CassandraQuery {
		String sql;
		Consumer<ICassandraSettableData<?>> statementBinder;
	}

	public <Result> Result appendLimitIfNeeded(Function<CassandraQuery, Result> func, String sql, Consumer<ICassandraSettableData<?>> statementBinder, Integer limit) {
		CassandraQuery query = new CassandraQuery();
		query.sql = sql;
		query.statementBinder = statementBinder;

		if (limit != null) {
			query.sql += " LIMIT :queryLimit";
			query.statementBinder = stmt -> {
				stmt.setInt("queryLimit", limit);
				statementBinder.accept(stmt);
			};
		}

		return func.apply(query);
	}

	ICassandraConfig getCassandraConfig() {
		return cassandraConfig;
	}
}

