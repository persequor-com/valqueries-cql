package io.prophecies.automapper;/* Copyright (C) Persequor ApS - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Persequor Development Team <partnersupport@persequor.com>, 
 */

import com.datastax.oss.driver.api.core.cql.Row;
import io.ran.Mapping;
import io.ran.MappingHelper;
import io.ran.ObjectMapHydrator;
import io.ran.TypeDescriber;
import io.ran.token.Token;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

public class ProphesiesHydrator<T> implements ObjectMapHydrator {
	private final T t;
	private Row row;

	public ProphesiesHydrator(T t, Row row, MappingHelper mappingHelper) {
		this.t = t;
		this.row = row;
		mappingHelper.hydrate(t, this);
	}

	private String transformToken(Token token) {
		return token.snake_case();
	}

	@Override
	public String getString(Token key) {
		return row.getString(transformToken(key));
	}

	@Override
	public Character getCharacter(Token token) {
		return row.getString(transformToken(token)).charAt(0);
	}

	@Override
	public ZonedDateTime getZonedDateTime(Token key) {
		return row.get(transformToken(key), ZonedDateTime.class);
	}

	@Override
	public Instant getInstant(Token key) {
		return row.getInstant(transformToken(key));
	}

	@Override
	public LocalDateTime getLocalDateTime(Token token) {
		return row.getInstant(transformToken(token)).atZone(ZoneOffset.systemDefault()).toLocalDateTime();
	}

	@Override
	public Integer getInteger(Token key) {
		return row.getInt(transformToken(key));
	}

	@Override
	public Short getShort(Token token) {
		return row.getShort(transformToken(token));
	}

	@Override
	public Long getLong(Token key) {
		return row.getLong(transformToken(key));
	}

	@Override
	public UUID getUUID(Token key) {
		return row.getUuid(transformToken(key));
	}

	@Override
	public Double getDouble(Token key) {
		return row.getDouble(transformToken(key));
	}

	@Override
	public BigDecimal getBigDecimal(Token key) {
		return row.getBigDecimal(transformToken(key));
	}

	@Override
	public Float getFloat(Token key) {
		return row.getFloat(transformToken(key));
	}

	@Override
	public Boolean getBoolean(Token token) {
		return row.getBoolean(transformToken(token));
	}

	@Override
	public Byte getByte(Token token) {
		return row.getByte(transformToken(token));
	}

	@Override
	public <T extends Enum<T>> T getEnum(Token token, Class<T> aClass) {
		String value = row.getString(transformToken(token));
		if (value == null) {
			return null;
		}
		return Enum.valueOf(aClass, value);
	}

	@Override
	public <T> Collection<T> getCollection(Token token, Class<T> aClass, Class<? extends Collection<T>> collectionClass) {
		List<T> collection = row.getList(transformToken(token), aClass);
		if (List.class.isAssignableFrom(collectionClass)) {
			return collection;
		} else if (Set.class.isAssignableFrom(collectionClass)) {
			return new HashSet<>(collection);
		} else {
			throw new RuntimeException("Only Lists or Sets are supported collection types by valqueries");
		}
	}

	public T get() {
		return t;
	}
}
