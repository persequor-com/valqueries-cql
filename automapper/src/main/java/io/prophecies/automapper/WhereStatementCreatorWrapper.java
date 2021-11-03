package io.prophecies.automapper;

import io.prophecies.WhereStatementCreator;
import io.ran.KeySet;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

public class WhereStatementCreatorWrapper implements WhereStatementCreator {
	Map<String,List<Consumer<WhereStatementCreator>>> predicates = new HashMap<>();

	private void add(String field, Consumer<WhereStatementCreator> predicate) {
		predicates.computeIfAbsent(field, f -> new ArrayList<>()).add(predicate);
	}

	@Override
	public WhereStatementCreator andEquals(String field, Object value) {
		add(field, w -> w.andEquals(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, String value) {
		add(field, w -> w.andEquals(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, UUID value) {
		add(field, w -> w.andEquals(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andRange(String field, ZonedDateTime fromIncluding, ZonedDateTime toExcluding) {
		add(field, w -> w.andRange(field, fromIncluding, toExcluding));
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThanOrEquals(String field, ZonedDateTime fromIncluding) {
		add(field, w -> w.andGreaterThanOrEquals(field, fromIncluding));
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThan(String field, ZonedDateTime from) {
		add(field, w -> w.andGreaterThan(field, from));
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThan(String field, Instant from) {
		add(field, w -> w.andGreaterThan(field, from));
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThan(String field, Integer from) {
		add(field, w -> w.andGreaterThan(field, from));
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThan(String field, Long from) {
		add(field, w -> w.andGreaterThan(field, from));
		return this;
	}

	@Override
	public WhereStatementCreator andLessThan(String field, Object toExcluding) {
		add(field, w -> w.andLessThan(field, toExcluding));
		return this;
	}

	@Override
	public WhereStatementCreator andLessThan(String field, ZonedDateTime toExcluding) {
		add(field, w -> w.andLessThan(field, toExcluding));
		return this;
	}

	@Override
	public WhereStatementCreator andTimeUUIDRange(String field, ZonedDateTime fromIncluding, ZonedDateTime toExcluding) {
		add(field, w -> w.andTimeUUIDRange(field, fromIncluding, toExcluding));
		return this;
	}

	@Override
	public WhereStatementCreator andTimeUUIDGreaterThanOrEquals(String field, ZonedDateTime fromIncluding) {
		add(field, w -> w.andTimeUUIDGreaterThanOrEquals(field, fromIncluding));
		return this;
	}

	@Override
	public WhereStatementCreator andTimeUUIDLessThan(String field, ZonedDateTime toExcluding) {
		add(field, w -> w.andTimeUUIDLessThan(field, toExcluding));
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, int value) {
		add(field, w -> w.andEquals(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, long value) {
		add(field, w -> w.andEquals(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, ZonedDateTime value) {
		add(field, w -> w.andEquals(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andEquals(String field, Instant value) {
		add(field, w -> w.andEquals(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andGreaterThan(String field, Object value) {
		add(field, w -> w.andGreaterThan(field, value));
		return this;
	}

	@Override
	public WhereStatementCreator andIsNull(String field) {
		add(field, w -> w.andIsNull(field));
		return this;
	}

	public boolean applyTo(WhereStatementCreator whereStatementCreator) {
		predicates.values().forEach(list -> list.forEach(predicate -> predicate.accept(whereStatementCreator)));
		return true;
	}

	public boolean applyTo(KeySet fields, WhereStatementCreator whereStatementCreator) {
		for(int i=0;i<fields.size();i++) {
			KeySet.Field f = fields.get(i);
			if(!predicates.containsKey(f.getToken().snake_case())) {
				if (i == 0 || f.getProperty().getAnnotations().get(PartitioningKey.class) != null) {
					return false;
				} else {
					break;
				}
			} else {
				predicates.remove(f.getToken().snake_case()).forEach(c -> c.accept(whereStatementCreator));
			}
		}
		return true;
	}

	public boolean matches(KeySet fields) {
		for(int i=0;i<fields.size();i++) {
			KeySet.Field f = fields.get(i);
			if(!predicates.containsKey(f.getToken().snake_case())) {
				if (i == 0 || f.getProperty().getAnnotations().get(PartitioningKey.class) != null) {
					return false;
				} else {
					break;
				}
			}
		}
		return true;

	}
}
