package io.prophecies;

import com.fasterxml.jackson.core.json.async.NonBlockingJsonParser;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class WhereStatementCreatorImpl implements WhereStatementCreator {

	private List<String> parts = new ArrayList<>();
	private List<Consumer<ICassandraSettableData<?>>> consumers = new ArrayList<>();

	public Consumer<ICassandraSettableData<?>> getConsumer() {
		return stmt -> consumers.forEach(c -> c.accept(stmt));
	}

	public String getSelectStatement() {
		return String.join(" AND ", parts);
	}

	@Override
	public WhereStatementCreator andEquals(String field, Object value) {
		consumers.add(stmt -> {
			stmt.set(field, value, (Class)value.getClass());
		});
		parts.add(field+" = :"+field);
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, String value) {
		consumers.add(stmt -> {
			stmt.setString(field, value);
		});
		parts.add(field+" = :"+field);
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, UUID value) {
		consumers.add(stmt -> {
			stmt.setUuid(field, value);
		});
		parts.add(field+" = :"+field);
		return this;
	}

	@Override
	public WhereStatementCreator andRange(String field, ZonedDateTime fromIncluding, ZonedDateTime toExcluding) {
		if (fromIncluding != null) {
			andGreaterThanOrEquals(field, fromIncluding);
		}
		if (toExcluding != null) {
			andLessThan(field, toExcluding);
		}
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThanOrEquals(String field, ZonedDateTime fromIncluding) {
		consumers.add(stmt -> {
			stmt.setZonedDateTime(field+"_from", fromIncluding);
		});
		parts.add(field+" >= :"+field+"_from");
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThan(String transformKey, Object value) {
		return null;
	}

	@Override
	public WhereStatementCreator andIsNull(String field) {
		consumers.add(stmt -> {
			stmt.setToNull(field);
		});
		parts.add(field+" = :"+field);
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThan(String field, ZonedDateTime from) {
		consumers.add(stmt -> {
			stmt.setZonedDateTime(field + "_from", from);
		});
		parts.add(field + " > :" + field + "_from");
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThan(String field, Instant from) {
		consumers.add(stmt -> {
			stmt.setInstant(field + "_from", from);
		});
		parts.add(field + " > :" + field + "_from");
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThan(String field, Integer from) {
		consumers.add(stmt -> {
			stmt.setInt(field + "_from", from);
		});
		parts.add(field + " > :" + field + "_from");
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThan(String field, Long from) {
		consumers.add(stmt -> {
			stmt.setLong(field + "_from", from);
		});
		parts.add(field + " > :" + field + "_from");
		return this;
	}

	@Override
	public WhereStatementCreator andLessThan(String field, Object toExcluding) {
		consumers.add(stmt -> {
			stmt.set(field + "_to", toExcluding, (Class)toExcluding.getClass());
		});
		parts.add(field + " < :" + field + "_to");
		return this;
	}

	@Override
	public WhereStatementCreator andLessThan(String field, ZonedDateTime toExcluding) {
		consumers.add(stmt -> {
			stmt.setZonedDateTime(field + "_to", toExcluding);
		});
		parts.add(field + " < :" + field + "_to");
		return this;
	}

	@Override
	public WhereStatementCreator andTimeUUIDRange(String field, ZonedDateTime fromIncluding, ZonedDateTime toExcluding) {
		if (fromIncluding != null) {
			andTimeUUIDGreaterThanOrEquals(field, fromIncluding);
		}
		if (toExcluding != null) {
			andTimeUUIDLessThan(field, toExcluding);
		}
		return this;
	}

	@Override
	public WhereStatementCreator andTimeUUIDGreaterThanOrEquals(String field, ZonedDateTime fromIncluding) {
		consumers.add(stmt -> {
			stmt.setZonedDateTime(field+"_from", fromIncluding);
		});
		parts.add(field+" >= maxTimeuuid(:"+field+"_from)");
		return this;
	}

	@Override
	public WhereStatementCreator andTimeUUIDLessThan(String field, ZonedDateTime toExcluding) {
		consumers.add(stmt -> {
			stmt.setZonedDateTime(field + "_to", toExcluding);
		});
		parts.add(field + " < minTimeuuid(:" + field + "_to)");
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, int value) {
		consumers.add(stmt -> {
			stmt.setInt(field, value);
		});
		parts.add(field+" = :"+field);
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, long value) {
		consumers.add(stmt -> {
			stmt.setLong(field, value);
		});
		parts.add(field+" = :"+field);
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, ZonedDateTime value) {
		consumers.add(stmt -> {
			stmt.setZonedDateTime(field, value);
		});
		parts.add(field+" = :"+field);
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, Instant value) {
		consumers.add(stmt -> {
			stmt.setInstant(field, value);
		});
		parts.add(field+" = :"+field);
		return this;
	}

}
