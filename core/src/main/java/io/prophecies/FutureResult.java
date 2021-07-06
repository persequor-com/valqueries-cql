package io.prophecies;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class FutureResult {
	public CompletableFuture<AsyncResultSet> completableFuture;
	public Consumer<Row> rowConsumer;

	public FutureResult(CompletableFuture<AsyncResultSet> completableFuture, Consumer<Row> rowConsumer) {
		this.completableFuture = completableFuture;
		this.rowConsumer = rowConsumer;
	}
}
