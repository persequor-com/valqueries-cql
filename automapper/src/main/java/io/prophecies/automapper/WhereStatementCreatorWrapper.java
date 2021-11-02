package io.prophecies.automapper;

import io.prophecies.WhereStatementCreator;

import java.lang.reflect.Array;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

public class WhereStatementCreatorWrapper implements WhereStatementCreator {
	List<Consumer<WhereStatementCreator>> predicates = new ArrayList<>();
	@Override
	public WhereStatementCreator andEquals(String field, Object value) {
		predicates.add(w -> w.andEquals(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, String value) {
		predicates.add(w -> w.andEquals(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, UUID value) {
		predicates.add(w -> w.andEquals(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andRange(String field, ZonedDateTime fromIncluding, ZonedDateTime toExcluding) {
		predicates.add(w -> w.andRange(field, fromIncluding, toExcluding));
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThanOrEquals(String field, ZonedDateTime fromIncluding) {
		predicates.add(w -> w.andGreaterThanOrEquals(field, fromIncluding));
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThan(String field, ZonedDateTime from) {
		predicates.add(w -> w.andGreaterThan(field, from));
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThan(String field, Instant from) {
		predicates.add(w -> w.andGreaterThan(field, from));
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThan(String field, Integer from) {
		predicates.add(w -> w.andGreaterThan(field, from));
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThan(String field, Long from) {
		predicates.add(w -> w.andGreaterThan(field, from));
		return this;
	}

	@Override
	public WhereStatementCreator andLessThan(String field, Object toExcluding) {
		predicates.add(w -> w.andLessThan(field, toExcluding));
		return this;
	}

	@Override
	public WhereStatementCreator andLessThan(String field, ZonedDateTime toExcluding) {
		predicates.add(w -> w.andLessThan(field, toExcluding));
		return this;
	}

	@Override
	public WhereStatementCreator andTimeUUIDRange(String field, ZonedDateTime fromIncluding, ZonedDateTime toExcluding) {
		predicates.add(w -> w.andTimeUUIDRange(field, fromIncluding, toExcluding));
		return this;
	}

	@Override
	public WhereStatementCreator andTimeUUIDGreaterThanOrEquals(String field, ZonedDateTime fromIncluding) {
		predicates.add(w -> w.andTimeUUIDGreaterThanOrEquals(field, fromIncluding));
		return this;
	}

	@Override
	public WhereStatementCreator andTimeUUIDLessThan(String field, ZonedDateTime toExcluding) {
		predicates.add(w -> w.andTimeUUIDLessThan(field, toExcluding));
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, int value) {
		predicates.add(w -> w.andEquals(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, long value) {
		predicates.add(w -> w.andEquals(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, ZonedDateTime value) {
		predicates.add(w -> w.andEquals(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, Instant value) {
		predicates.add(w -> w.andEquals(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThan(String field, Object value) {
		predicates.add(w -> w.andGreaterThan(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andIsNull(String field) {
		predicates.add(w -> w.andIsNull(field));
		return this;
	}

	public void applyTo(WhereStatementCreator whereStatementCreator) {
		predicates.forEach(c -> c.accept(whereStatementCreator));
	}
}
