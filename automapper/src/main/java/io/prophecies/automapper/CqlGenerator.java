package io.prophecies.automapper;

import io.ran.Property;
import io.ran.TypeDescriber;
import io.ran.token.Token;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class CqlGenerator {
	public String getTableName(TypeDescriber<?> typeDescriber) {
		return Token.CamelCase(typeDescriber.clazz().getSimpleName()).snake_case();
	}

	public List<String> getTableNames(TypeDescriber<?> typeDescriber) {
		List<String> result = new ArrayList<>();
		result.add(Token.CamelCase(typeDescriber.clazz().getSimpleName()).snake_case());
		CqlDescriber cqlDescriber = CqlDescriber.get(typeDescriber);
		cqlDescriber.getIndices().forEach(index -> {
			result.add(index.getName());
		});
		return result;
	}

	public List<String> generate(TypeDescriber<?> typeDescriber) {
		CqlDescriber cqlDescriber = CqlDescriber.get(typeDescriber);
		List<String> cqls = new ArrayList<>();
		cqls.add("CREATE TABLE IF NOT EXISTS "+ getTableName(typeDescriber)+" ("+typeDescriber.fields().stream().map(property -> {
			return property.getToken().snake_case()+ " "+getSqlType(property.getType().clazz);
		}).collect(Collectors.joining(", "))+", PRIMARY KEY("+typeDescriber.primaryKeys().stream().map(property -> {
			return property.getToken().snake_case();
		}).collect(Collectors.joining(", "))+"));");
		cqlDescriber.getIndices().forEach(index -> {
			cqls.add("CREATE TABLE IF NOT EXISTS "+ index.getName()+" ("+index.getFields().stream().map(fieldName -> {
				Property property = typeDescriber.fields().get(Token.snake_case(fieldName));
				return property.getToken().snake_case()+ " "+getSqlType(property.getType().clazz);
			}).collect(Collectors.joining(", "))+", PRIMARY KEY("+index.getFields().stream().map(fieldName -> {
				Property property = typeDescriber.fields().get(Token.snake_case(fieldName));
				return property.getToken().snake_case();
			}).collect(Collectors.joining(", "))+"));");
		});
		return cqls;
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
