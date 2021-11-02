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
			boolean matchesPrimaryKey = true;
			for(int i=0;i<fromKeys.size();i++) {
				if (!primaryKeys.get(i).getToken().equals(fromKeys.get(i).getToken())) {
					matchesPrimaryKey = false;
					break;
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

	public List<IndexConfig.Index> getIndices() {
		return index.getIndices();
	}

	public Optional<RelationIndex> forReverseRelation(RelationDescriber relationDescriber) {
		return index.getIndices().stream().map(idx -> {
			KeySet toKeys = relationDescriber.getToKeys();
			boolean matches = true;
			for(int i = 0;i<toKeys.size();i++) {
				if (!idx.getFields().get(i).equals(toKeys.get(i).getToken().snake_case())) {
					matches = false;
					break;
				}
			}
			if (matches) {
				return new RelationIndex(idx, relationDescriber);
			}

			return null;
		}).filter(Objects::nonNull).findFirst();
	}

	public Optional<RelationIndex> forRelation(RelationDescriber relationDescriber) {
		return index.getIndices().stream().map(idx -> {
			KeySet fromK = relationDescriber.getFromKeys();
			boolean matches = true;
			for(int i = 0;i<fromK.size();i++) {
				if (!idx.getFields().get(i).equals(fromK.get(i).getToken().snake_case())) {
					matches = false;
					break;
				}
			}
			if (matches) {
				return new RelationIndex(idx, relationDescriber);
			}

			return null;
		}).filter(Objects::nonNull).findFirst();
	}

	public static class RelationIndex {
		private IndexConfig.Index index;
		private RelationDescriber relationDescriber;

		public RelationIndex(IndexConfig.Index index, RelationDescriber relationDescriber) {
			this.index = index;
			this.relationDescriber = relationDescriber;
		}

		public IndexConfig.Index getIndex() {
			return index;
		}

		public RelationDescriber getRelationDescriber() {
			return relationDescriber;
		}
	}
}
