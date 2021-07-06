package io.prophecies;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

public interface WhereStatementCreator {

	WhereStatementCreator andEquals(String field, Object value);

	WhereStatementCreator andEquals(String field, String value);

	WhereStatementCreator andEquals(String field, UUID value);

	WhereStatementCreator andRange(String field, ZonedDateTime fromIncluding, ZonedDateTime toExcluding);

	WhereStatementCreator andGreaterThanOrEquals(String field, ZonedDateTime fromIncluding);

	WhereStatementCreator andGreaterThan(String field, ZonedDateTime from);

	WhereStatementCreator andGreaterThan(String field, Instant from);

	WhereStatementCreator andGreaterThan(String field, Integer from);

	WhereStatementCreator andGreaterThan(String field, Long from);

	WhereStatementCreator andLessThan(String field, Object toExcluding);

	WhereStatementCreator andLessThan(String field, ZonedDateTime toExcluding);

	WhereStatementCreator andTimeUUIDRange(String field, ZonedDateTime fromIncluding, ZonedDateTime toExcluding);

	WhereStatementCreator andTimeUUIDGreaterThanOrEquals(String field, ZonedDateTime fromIncluding);

	WhereStatementCreator andTimeUUIDLessThan(String field, ZonedDateTime toExcluding);

	WhereStatementCreator andEquals(String field, int value);

	WhereStatementCreator andEquals(String field, long value);

	WhereStatementCreator andEquals(String field, ZonedDateTime value);

	WhereStatementCreator andEquals(String field, Instant value);

	WhereStatementCreator andGreaterThan(String transformKey, Object value);

	WhereStatementCreator andIsNull(String transformKey);
}
