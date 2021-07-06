package io.prophecies.automapper;/* Copyright (C) Persequor ApS - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Persequor Development Team <partnersupport@persequor.com>, 
 */

import io.ran.Mapping;
import io.ran.MappingHelper;
import io.ran.ObjectMapColumnizer;
import io.ran.TypeDescriber;
import io.ran.token.Token;
import io.prophecies.ICassandraSettableData;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

public class ProphesiesColumnizer<T> implements ObjectMapColumnizer, Consumer<ICassandraSettableData<?>> {
	private final List<Consumer<ICassandraSettableData<?>>> statements = new ArrayList<>();
	private final TypeDescriber<T> typeDescriber;
	private MappingHelper mappingHelper;

	public ProphesiesColumnizer(TypeDescriber<T> typeDescriber, MappingHelper mappingHelper) {
		this.typeDescriber = typeDescriber;
		this.mappingHelper = mappingHelper;
	}

	private String transformKey(Token key) {
		return key.snake_case();
	}

	private void add(Token key, Consumer<ICassandraSettableData<?>> consumer) {
		statements.add(consumer);
	}

	@Override
	public void set(Token key, String value) {
		add(key, s -> s.setString(transformKey(key), value));
	}

	@Override
	public void set(Token token, Character value) {
		add(token, s -> s.setString(transformKey(token), value.toString()));
	}

	@Override
	public void set(Token key, ZonedDateTime value) {
		add(key, s -> s.setZonedDateTime(transformKey(key), value));
	}

	@Override
	public void set(Token key, Integer value) {
		add(key, s -> s.setInt(transformKey(key), value));
	}

	@Override
	public void set(Token token, Short value) {
		add(token, s -> s.setShort(transformKey(token), value));
	}

	@Override
	public void set(Token key, Long value) {
		statements.add(s -> s.setLong(transformKey(key), value));
	}

	@Override
	public void set(Token key, UUID value) {
		add(key, s -> s.setUuid(transformKey(key), value));
	}

	@Override
	public void set(Token key, Double value) {
		add(key, s -> s.setDouble(transformKey(key), value));
	}

	@Override
	public void set(Token key, BigDecimal value) {
		add(key, s -> s.setBigDecimal(transformKey(key), value));
	}

	@Override
	public void set(Token key, Float value) {
		add(key, s -> s.setFloat(transformKey(key), value));
	}

	@Override
	public void set(Token token, Boolean aBoolean) {
		add(token, s -> s.setBoolean(transformKey(token), aBoolean));
	}

	@Override
	public void set(Token token, Byte value) {
		add(token, s -> s.setByte(transformKey(token), value));
	}

	@Override
	public void set(Token token, Enum<?> anEnum) {
		add(token, s -> {
			if (anEnum == null) {
				s.setToNull(transformKey(token));
			} else {
				s.setString(transformKey(token), anEnum.name());
			}
		});
	}

	@Override
	public void set(Token token, Collection<?> collection) {
		if (collection instanceof List) {
			add(token, s -> s.setList(transformKey(token), (List<String>) collection, String.class));
		} else if (collection instanceof Set) {
			add(token, s -> s.setSet(transformKey(token), (Set<String>) collection, String.class));
		} else {
			throw new RuntimeException("Only sets and lists of string is supported by valqueries at this point");
		}
	}

	@Override
	public void accept(ICassandraSettableData<?> iCassandraSettableData) {
		statements.forEach(s -> s.accept(iCassandraSettableData));
	}

	public Consumer<ICassandraSettableData<?>> init(T t) {
		mappingHelper.columnize(t, this);
		return this;
	}
}
