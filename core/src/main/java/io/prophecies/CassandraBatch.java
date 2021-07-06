package io.prophecies;


import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.io.Closeable;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class CassandraBatch implements Closeable {

	private final Cassandra cassandra;
	private final Semaphore limiter;
	private final Queue<FutureResult> futures;
	private long asyncQueryTimeout;

	public CassandraBatch(Cassandra cassandra, boolean slow) {
		this.cassandra = cassandra;
		limiter = new Semaphore(slow
				? cassandra.getCassandraConfig().getPartitionBatchSize()
				: cassandra.getCassandraConfig().getMaxActiveAsyncFuturesPerThread());
		asyncQueryTimeout = slow
				? cassandra.getCassandraConfig().getAsyncStatementTimeOutInSec() * 100
				: cassandra.getCassandraConfig().getAsyncStatementTimeOutInSec();
		futures = new ConcurrentLinkedQueue<>();
	}

	public CassandraBatch(Cassandra cassandra) {
		this(cassandra, false);
	}

	public CassandraBatch execute(String query, Consumer<ICassandraSettableData<?>> statementBinder, Integer limit) {
		return execute(query, statementBinder, limit, asyncResultSet -> {});
	}

	public CassandraBatch execute(String query, Consumer<ICassandraSettableData<?>> statementBinder, Integer limit, Consumer<Row> callback) {
		limiter.acquireUninterruptibly();

		CompletableFuture<AsyncResultSet> future;

		try {
			future = cassandra.executeAsync(query, statementBinder, limit).toCompletableFuture();
		} catch (RuntimeException e) {
			limiter.release();
			throw e;
		}

		futures.add(new FutureResult(future, callback));
		future.thenRunAsync(() -> limiter.release());
		return this;
	}

	public CassandraBatch insert(String table, Consumer<ICassandraSettableData<?>> statementBinder) {
		return execute(cassandra.buildInsertSql(table, statementBinder), statementBinder);
	}

	public CassandraBatch partitionExecute(String query, Consumer<ICassandraSettableData<?>> statementBinder) {
		return partitionExecute(query, statementBinder, null);
	}

	public CassandraBatch partitionExecute(String query, Consumer<ICassandraSettableData<?>> statementBinder, Integer limit) {
		int partitionCount = cassandra.getCassandraConfig().getMaxSaltValue() - 1;
		StreamUtils.range(0, partitionCount, 1).forEach(partition -> {
			execute(query, stmt -> {
				stmt.setInt("partition", partition);
				statementBinder.accept(stmt);
			}, limit);
		});
		return this;
	}

	public CassandraBatch select(String table, Consumer<WhereStatementCreator> statementBinder) {
		return select(table, statementBinder, 0);
	}

	public CassandraBatch select(String table, Consumer<WhereStatementCreator> statementBinder, int limit) {
		return select(table, statementBinder, limit, asyncResultSet -> {});
	}

	public CassandraBatch select(String table, Consumer<WhereStatementCreator> statementBinder, int limit, Consumer<Row> callback) {
		WhereStatementCreatorImpl statementCreator = new WhereStatementCreatorImpl();
		statementBinder.accept(statementCreator);
		StringBuilder sql = new StringBuilder("SELECT * FROM " + table + " WHERE " + statementCreator.getSelectStatement());
		if (limit > 0) {
			sql.append(" LIMIT ")
					.append(limit);
		}

		return execute(sql.toString(), statementCreator.getConsumer(), null, callback);
	}

	public CassandraBatch delete(String table, Consumer<WhereStatementCreator> statementBinder) {
		WhereStatementCreatorImpl statementCreator = new WhereStatementCreatorImpl();
		statementBinder.accept(statementCreator);
		String sql = "DELETE FROM " + table + " WHERE " + statementCreator.getSelectStatement();
		return execute(sql, statementCreator.getConsumer());
	}

	public CassandraBatch execute(String query, Consumer<ICassandraSettableData<?>> statementBinder) {
		return execute(query, statementBinder, null);
	}

	public CassandraBatchResultSet results() {
		return new CassandraBatchResultSet(futures);
	}

	public boolean wasApplied() {
		return appliedCount() == futures.size();
	}

	public long appliedCount() {
		return futures
			.stream()
			.filter(resultSetFuture -> {
				try {
					return resultSetFuture.completableFuture.get().wasApplied();
				} catch (InterruptedException | ExecutionException e) {
					if (e.getCause() instanceof RuntimeException) {
						throw (RuntimeException) e.getCause();
					}
					throw new RuntimeException(e);
				}
			}).count();
	}

	public CassandraBatchIndex byIndex(IndexConfig indexConfig) {
		return new CassandraBatchIndex(this, indexConfig);
	}


	@Override
	public void close() {
		for (FutureResult future : futures) {
			try {
			if (!future.completableFuture.isDone()) {
					AsyncResultSet res = future.completableFuture.get(asyncQueryTimeout, TimeUnit.SECONDS);
					applyAsyncResultSet(res, future.rowConsumer);
				} else {
					AsyncResultSet res = future.completableFuture.get();
					applyAsyncResultSet(res, future.rowConsumer);
				}
			} catch (TimeoutException | InterruptedException | ExecutionException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void applyAsyncResultSet(AsyncResultSet res, Consumer<Row> rowConsumer) {
		res.currentPage().forEach(rowConsumer::accept);
	}

}
