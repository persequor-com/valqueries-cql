package io.prophecies.automapper;

import io.prophecies.IndexConfig;
import io.ran.KeySet;
import io.ran.RelationDescriber;
import io.ran.TypeDescriber;
import io.ran.TypeDescriberImpl;
import io.ran.token.Token;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class CqlDescriber {
	private static Map<Class, CqlDescriber> describerMap = Collections.synchronizedMap(new HashMap<>());
	private TypeDescriber<?> typeDescriber;
	private IndexConfig index;

	private CqlDescriber(TypeDescriber<?> typeDescriber) {
		this.typeDescriber = typeDescriber;
		index = IndexConfig.build(getTableName(), primaryKey -> {
			TypeDescriberImpl.getTypeDescriber(typeDescriber.clazz()).primaryKeys().forEach(field -> {
				primaryKey.add(field.getToken().snake_case());
			});
		});
		typeDescriber.relations().forEach(relationDescriber -> {
			String indexName = getTableName(relationDescriber.getToClass().clazz) + "_to_" + getTableName();
			KeySet fromKeys = relationDescriber.getFromKeys();
			KeySet primaryKeys = typeDescriber.primaryKeys();
			boolean matchesPrimaryKey = primaryKeys.size() == fromKeys.size();
			if (matchesPrimaryKey) {
				for (int i = 0; i < primaryKeys.size(); i++) {
					if (!primaryKeys.get(i).getToken().equals(fromKeys.get(i).getToken())) {
						matchesPrimaryKey = false;
						break;
					}
				}
			}
			if (matchesPrimaryKey) {
				return;
			}
			index.add(indexName, f -> {
				relationDescriber.getFromKeys().forEach(field -> {
					f.add(field.getToken().snake_case());
				});
				typeDescriber.primaryKeys().forEach(field -> {
					if (relationDescriber.getFromKeys().stream().noneMatch(fk -> fk.getToken().equals(field.getToken()))) {
						f.add(field.getToken().snake_case());
					}
				});
			});
		});
		typeDescriber.indexes().forEach(idx -> {
			String indexName = getTableName()+"_"+getTableName(idx.getName());
			index.add(indexName, f -> {
				idx.forEach(field -> {
					f.add(field.getToken().snake_case());
				});
				typeDescriber.primaryKeys().forEach(field -> {
					if (idx.stream().noneMatch(fk -> fk.getToken().equals(field.getToken()))) {
						f.add(field.getToken().snake_case());
					}
				});
			});
		});
	}

	public static CqlDescriber get(Class<?> aClsas) {
		TypeDescriber<?> typeDescriber = TypeDescriberImpl.getTypeDescriber(aClsas);
		return get(typeDescriber);
	}

	public static CqlDescriber get(TypeDescriber<?> typeDescriber) {
		return describerMap.computeIfAbsent(typeDescriber.clazz(), (c) -> new CqlDescriber(typeDescriber));
	}


	private String getTableName() {
		return getTableName(typeDescriber.clazz());
	}

	private String getTableName(Class<?> clazz) {
		return Token.CamelCase(clazz.getSimpleName()).snake_case();
	}

	private String getTableName(String name) {
		return Token.get(name).snake_case();
	}

	public List<IndexConfig.Index> getIndices() {
		return index.getIndices();
	}

	public Optional<IndexConfig.Index> forReverseRelation(RelationDescriber relationDescriber) {
		return forIndex(relationDescriber.getToKeys());
	}

	public Optional<IndexConfig.Index> forRelation(RelationDescriber relationDescriber) {
		return forIndex(relationDescriber.getFromKeys());
	}

	public Optional<IndexConfig.Index> forIndex(KeySet keySet) {
		return index.getIndices().stream().map(idx -> {
			boolean matches = true;
			for(int i = 0;i<keySet.size();i++) {
				if (!idx.getFields().get(i).equals(keySet.get(i).getToken().snake_case())) {
					matches = false;
					break;
				}
			}
			if (matches) {
				return idx;
			}

			return null;
		}).filter(Objects::nonNull).findFirst();
	}
}
