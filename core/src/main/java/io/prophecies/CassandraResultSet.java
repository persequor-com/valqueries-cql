package io.prophecies;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CassandraResultSet implements ResultSet {
	private final ResultSet resultSet;

	public CassandraResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
	}


	public <T> Stream<T> stream(Function<Row, T> mapper)  {
		return StreamSupport.stream(resultSet.spliterator(), false).map(mapper);
	}

	public Stream<Row> stream() {
		return StreamSupport.stream(resultSet.spliterator(), false);
	}

	public Optional<Row> optionalOne(){
		return Optional.ofNullable(one());
	}

	@NonNull
	@Override
	public ColumnDefinitions getColumnDefinitions() {
		return resultSet.getColumnDefinitions();
	}

	@NonNull
	@Override
	public List<ExecutionInfo> getExecutionInfos() {
		return resultSet.getExecutionInfos();
	}

	@Override
	public boolean isFullyFetched() {
		return resultSet.isFullyFetched();
	}

	@Override
	public int getAvailableWithoutFetching() {
		return resultSet.getAvailableWithoutFetching();
	}

	@Override
	public boolean wasApplied() {
		return resultSet.wasApplied();
	}

	@Override
	public Iterator<Row> iterator() {
		return resultSet.iterator();
	}
}
