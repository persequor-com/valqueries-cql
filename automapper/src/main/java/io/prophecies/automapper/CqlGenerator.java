package io.prophecies.automapper;

import io.ran.TypeDescriber;
import io.ran.token.Token;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class CqlGenerator {
	public String getTableName(TypeDescriber<?> typeDescriber) {
		return Token.CamelCase(typeDescriber.clazz().getSimpleName()).snake_case();
	}

	public String generate(TypeDescriber<?> typeDescriber) {
		return "CREATE TABLE IF NOT EXISTS "+ getTableName(typeDescriber)+" ("+typeDescriber.fields().stream().map(property -> {
			return property.getToken().snake_case()+ " "+getSqlType(property.getType().clazz);
		}).collect(Collectors.joining(", "))+", PRIMARY KEY("+typeDescriber.primaryKeys().stream().map(property -> {
			return property.getToken().snake_case();
		}).collect(Collectors.joining(", "))+"));";
	}

	private String getSqlType(Class<?> type) {
		if (type == String.class) {
			return "TEXT";
		}
		if (type == UUID.class) {
			return "uuid";
		}
		if (type == ZonedDateTime.class) {
			return "TIMESTAMP";
		}
		if (type == List.class) {
			return "list<text>";
		}
		if (type.isEnum()) {
			return "TEXT";
		}
		throw new RuntimeException("So far unsupported column type: "+type.getName());
	}
}
