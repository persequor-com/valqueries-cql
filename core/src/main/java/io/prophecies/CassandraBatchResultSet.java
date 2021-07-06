package io.prophecies;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CassandraBatchResultSet implements Iterator<Row>, Iterable<Row> {
	private final Iterator<FutureResult> futures;
	private Iterator<Row> current = null;
	private AsyncResultSet currentResultSet;
	private FutureResult currentFutureResult;


	CassandraBatchResultSet(Queue<FutureResult> futures) {
		this.futures = new FutureIterator(futures);
		nextResultSet();
	}

	@Override
	public boolean hasNext() {
		while (current== null || !current.hasNext()) {
			if (currentResultSet == null || current == null) {
				return false;
			}

			if (!current.hasNext()) {
				if (currentResultSet.hasMorePages()) {
					current = getNextPage();
				} else {
					nextResultSet();
				}
			}
		}
		return true;
	}

	private Iterator<Row> getNextPage() {
		try {
			return currentResultSet.fetchNextPage().toCompletableFuture().get().currentPage().iterator();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	private void nextResultSet() {
		if (!futures.hasNext()) {
			currentResultSet = null;
			current = null;
			return;
		}
		try {
			currentFutureResult = futures.next();
			currentResultSet = currentFutureResult.completableFuture.get();
			current = currentResultSet.currentPage().iterator();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Row next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}

		Row row = current.next();
//		currentFutureResult.rowConsumer.accept(row);
		return row;
	}

	public List<Row> all() {
		return Lists.newArrayList((Iterator<? extends Row>) this);
	}

	public Stream<Row> stream() {
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, 0), true);
	}

	public <T> Stream<T> stream(Function<Row, T> mapper) {
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, 0), true).map(mapper);
	}

	@Override
	public Iterator<Row> iterator() {
		return this;
	}
}
