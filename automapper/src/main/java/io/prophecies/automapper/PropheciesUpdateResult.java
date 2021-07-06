package io.prophecies.automapper;

import io.ran.CrudRepository;
import io.prophecies.CassandraResultSet;

public class PropheciesUpdateResult implements CrudRepository.CrudUpdateResult {
	private CassandraResultSet resultSet;

	public PropheciesUpdateResult(CassandraResultSet resultSet) {
		this.resultSet = resultSet;
	}

	@Override
	public int affectedRows() {
		return 0;
	}
}
