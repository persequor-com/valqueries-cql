package io.prophecies;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class IndexConfig {
	Index baseIndex;
	List<Index> indices = new ArrayList<>();

	private IndexConfig(String tableName, Consumer<Index> primaryKey) {
		this.baseIndex = new Index(tableName, primaryKey, true);
	}

	public static IndexConfig build(String tableName, Consumer<Index> idSetter) {
		return new IndexConfig(tableName, idSetter);
	}

	public IndexConfig add(String name, Consumer<Index> fieldSetter) {
		Index index = new Index(name, fieldSetter, false);
		indices.add(index);
		return this;
	}

	public String buildSelectCql() {
		String cql = "select * from " + baseIndex.name + " ";
		cql += " WHERE " + baseIndex.fields.stream().map(s -> s + " = :" + s).collect(Collectors.joining(" AND ")) + "";
		return cql;
	}

	public List<Index> getIndices() {
		return indices;
	}

	public class Index {
		private String name;
		private List<String> fields = new ArrayList<>();

		public Index(String name, Consumer<Index> fieldSetter, boolean isBaseIndex) {
			this.name = name;
			fieldSetter.accept(this);
			if (!isBaseIndex) {
				String missing = baseIndex.fields.stream().filter(id -> !this.fields.contains(id)).collect(Collectors.joining(", "));
				if (missing.length() > 0) {
					throw new IndexConfigException("The entire primary key must be represented in the index. Missing fields where: " + missing);
				}
			}
		}

		public Index add(String fieldName) {
			fields.add(fieldName);
			return this;
		}

		public boolean contains(String name) {
			return fields.contains(name);
		}

		public IndexBinder binder(ICassandraSettableData stmt) {
			return new IndexBinder(this, stmt);
		}


		public String buildInsertCql() {
			String cql = "insert into " + name + " ";
			cql += "(" + fields.stream().collect(Collectors.joining(", ")) + ")";
			cql += " VALUES (" + fields.stream().map(s -> ":" + s).collect(Collectors.joining(", ")) + ")";
			return cql;
		}

		public String buildDeleteCql() {
			String cql = "delete from " + name + " ";
			cql += " WHERE " + fields.stream().map(s -> s + " = :" + s).collect(Collectors.joining(" AND ")) + "";
			return cql;
		}

		public List<String> getFields() {
			return fields;
		}

		public String getName() {
			return name;
		}
	}
}
