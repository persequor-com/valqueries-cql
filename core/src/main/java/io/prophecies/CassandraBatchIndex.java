package io.prophecies;

import java.util.function.Consumer;

public class CassandraBatchIndex {

	private final CassandraBatch batch;
	private final IndexConfig indexConfig;

	public CassandraBatchIndex(CassandraBatch batch, IndexConfig indexConfig) {
		this.batch = batch;
		this.indexConfig = indexConfig;
	}

	public void delete(String sql, Consumer<ICassandraSettableData<?>> statementBinder) {
		batch.execute(sql, stmt -> statementBinder.accept(indexConfig.baseIndex.binder(stmt)));
		for (IndexConfig.Index index : indexConfig.indices) {
			batch.execute(index.buildDeleteCql(), stmt -> statementBinder.accept(index.binder(stmt)));
		}
	}

	public void insert(String sql, Consumer<ICassandraSettableData<?>> statementBinder) {
		batch.execute(sql, statementBinder);
		for (IndexConfig.Index index : indexConfig.indices) {
			batch.execute(index.buildInsertCql(), stmt -> statementBinder.accept(index.binder(stmt)));
		}
	}
}
