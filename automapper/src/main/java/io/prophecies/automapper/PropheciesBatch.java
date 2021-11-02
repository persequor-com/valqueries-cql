package io.prophecies.automapper;

import io.prophecies.CassandraBatch;
import io.prophecies.ICassandraSettableData;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

public class PropheciesBatch implements Closeable {
	private CassandraBatch cassandraBatch;

	public PropheciesBatch(CassandraBatch cassandraBatch) {
		this.cassandraBatch = cassandraBatch;
	}

	public CassandraBatch getCassandraBatch() {
		return cassandraBatch;
	}

	@Override
	public void close() throws IOException {
		if (cassandraBatch != null) {
			cassandraBatch.close();
		}
	}

	public void insert(String table, Consumer<ICassandraSettableData<?>> settableData) {
		if (cassandraBatch != null) {
			cassandraBatch.insert(table, settableData);
		}
	}
}
