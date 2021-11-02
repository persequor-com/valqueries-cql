package io.prophecies.automapper;

import com.datastax.oss.driver.api.core.cql.Row;
import io.prophecies.Cassandra;
import io.prophecies.CassandraBatch;
import io.prophecies.IndexConfig;
import io.prophecies.WhereStatementCreator;
import io.ran.Clazz;
import io.ran.CompoundKey;
import io.ran.CrudRepoBaseQuery;
import io.ran.CrudRepository;
import io.ran.GenericFactory;
import io.ran.KeySet;
import io.ran.Mapping;
import io.ran.MappingHelper;
import io.ran.Property;
import io.ran.RelationDescriber;
import io.ran.TypeDescriber;
import io.ran.TypeDescriberImpl;
import io.ran.token.Token;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PropheciesQueryImpl<T> extends CrudRepoBaseQuery<T, PropheciesQuery<T>> implements PropheciesQuery<T> {
	private final CqlDescriber cqlDescriber;
	private List<Consumer<WhereStatementCreator>> predicates = new ArrayList<>();
	private final Cassandra cassandra;
	private final Class<T> modelType;
	private CqlGenerator cqlGenerator;
	private String tableName;
	private List<RelationDescriber> eagers = new ArrayList<>();
	private List<PropheciesQuery> queries = new ArrayList<>();
	private GenericFactory factory;
	private TypeDescriber<T> typeDescriber;
	private MappingHelper mappingHelper;
	private List<SortingElement> sort = new ArrayList<>();
	private Integer limit = null;
	private int offset = 0;
	private List<Consumer<WhereStatementCreator>> subQueries = new ArrayList<>();
	private boolean isEmpty = false;

	public PropheciesQueryImpl(Cassandra cassandra, Class<T> modelType, CqlGenerator cqlGenerator, GenericFactory factory, MappingHelper mappingHelper) {
		super(modelType, factory);
		this.cassandra = cassandra;
		this.modelType = modelType;

		this.cqlGenerator = cqlGenerator;
		this.factory = factory;
		this.typeDescriber = TypeDescriberImpl.getTypeDescriber(modelType);
		this.cqlDescriber = CqlDescriber.get(typeDescriber);
		tableName= cqlGenerator.getTableName(typeDescriber);
		this.mappingHelper = mappingHelper;
	}


	private String transformKey(Property<?> property) {
		return property.getToken().snake_case();
	}

	@Override
	public PropheciesQuery<T> eq(Property.PropertyValue<?> propertyValue) {
		predicates.add(statementCreator -> statementCreator.andEquals(transformKey(propertyValue.getProperty()), propertyValue.getValue()));
		return this;
	}


	@Override
	public PropheciesQuery<T> gt(Property.PropertyValue<?> propertyValue) {
		predicates.add(statementCreator -> statementCreator.andGreaterThan(transformKey(propertyValue.getProperty()), propertyValue.getValue()));
		return this;
	}

	@Override
	public PropheciesQuery<T> lt(Property.PropertyValue<?> propertyValue) {
		predicates.add(statementCreator -> statementCreator.andLessThan(transformKey(propertyValue.getProperty()), propertyValue.getValue()));
		return this;
	}

	@Override
	public PropheciesQuery<T> isNull(Property<?> property) {
		predicates.add(statementCreator -> statementCreator.andIsNull(transformKey(property)));
		return this;
	}

	public PropheciesQuery<T> withEager(RelationDescriber relationDescriber) {
		this.eagers.add(relationDescriber);
		return this;
	}

	@Override
	public <X extends Comparable<X>> PropheciesQuery<T> sortAscending(Property<X> property) {
		sort.add(new SortingElement(property.getToken(), true));
		return this;
	}

	@Override
	public <X extends Comparable<X>> PropheciesQuery<T> sortDescending(Property<X> property) {
		sort.add(new SortingElement(property.getToken(), false));
		return this;
	}

	@Override
	public PropheciesQuery<T> limit(int offset, int limit) {
		this.limit = limit;
		this.offset = offset;
		return this;
	}

	@Override
	public PropheciesQuery<T> limit(int limit) {
		this.limit = limit;
		return this;
	}

	@Override
	public <X, Z extends CrudRepository.InlineQuery<X, Z>> PropheciesQuery<T> subQuery(RelationDescriber relation, Consumer<Z> consumer) {
		if (!relation.getVia().isEmpty()) {
			throw new RuntimeException("Subquery via many to many tables is not supported yet by Valqueries cql");
		}

		PropheciesQueryImpl subQuery = (PropheciesQueryImpl<Z>)new PropheciesQueryImpl(cassandra, relation.getToClass().clazz, cqlGenerator, factory,mappingHelper);
		consumer.accept((Z)subQuery);
		subQueries.add(stmt -> {
			Stream<X> res = ((PropheciesQueryImpl<X>) subQuery).execute();
			boolean[] found = new boolean[1];
			found[0] = false;
			Optional<CqlDescriber.RelationIndex> index = cqlDescriber.forRelation(relation);
			try(CassandraBatch batch = cassandra.batch()) {
				res.forEach(x -> {

					if (index.isPresent()) {
						batch.select(index.get().getIndex().getName(), st -> {
							for (int i = 0; i < relation.getFromKeys().size(); i++) {
								KeySet.Field fromKey = relation.getFromKeys().get(i);
								KeySet.Field toKey = relation.getToKeys().get(i);
								Object value = mappingHelper.getValue(x, toKey.getProperty());
								st.andEquals(fromKey.getToken().snake_case(), value);
							}
						}, -1, row -> {
							for (int i = 0; i < relation.getFromClass().getKeys().getPrimary().size(); i++) {
								KeySet.Field primaryKey = relation.getFromClass().getKeys().getPrimary().get(i);
								stmt.andEquals(primaryKey.getToken().snake_case(), row.get(primaryKey.getToken().snake_case(), primaryKey.getProperty().getType().clazz));
							}
						});
					} else {
						for (int i = 0; i < relation.getFromKeys().size(); i++) {
							KeySet.Field fromKey = relation.getFromKeys().get(i);
							KeySet.Field toKey = relation.getToKeys().get(i);
							Object value = mappingHelper.getValue(x, toKey.getProperty());
							stmt.andEquals(fromKey.getToken().snake_case(), value);
						}
					}
				found[0] = true;
				});
			}
			if (!found[0]) {
				this.isEmpty = true;
			}
		});

		return this;
	}

	private PropheciesQueryImpl<?> query(Class type) {
		return new PropheciesQueryImpl(cassandra, type, cqlGenerator, factory, mappingHelper);
	}

	@Override
	public Stream<T> execute() {
		WhereStatementCreatorWrapper wsc = new WhereStatementCreatorWrapper();
		subQueries.forEach(subQuery -> {
			subQuery.accept(wsc);
		});
		if (isEmpty) {
			return Stream.empty();
		}
		Stream<T> stream = StreamSupport.stream(cassandra.select(tableName, whereStatementCreator -> {
			wsc.applyTo(whereStatementCreator);
			predicates.forEach(p -> {
				p.accept(whereStatementCreator);
			});
		}).map(r -> new ProphesiesHydrator<T>(factory.get(modelType), r, mappingHelper).get()).spliterator(), false);
		if (!eagers.isEmpty()) {
			List<T> list = stream.collect(Collectors.toList());
			try (CassandraBatch batch = cassandra.batch()) {
				for (T t : list) {
					this.eagers.forEach(relationDescriber -> {
						Class toType = relationDescriber.getToClass().clazz;
						TypeDescriber typeDescriber = TypeDescriberImpl.getTypeDescriber(relationDescriber.getToClass().clazz);
						CqlDescriber cqlDescriber = CqlDescriber.get(typeDescriber);
						Optional<CqlDescriber.RelationIndex> index = cqlDescriber.forReverseRelation(relationDescriber);
						List<Object> relations = new ArrayList<>();
						if (relationDescriber.isCollectionRelation()) {
							((Mapping) t)._setRelation(relationDescriber, relations);
						}
						CompoundKey fromKey = getRelationKey(typeDescriber, relationDescriber.getFromKeys(), t);
						String tableName = Token.CamelCase(relationDescriber.getToClass().clazz.getSimpleName()).snake_case();
						if (index.isPresent()) {
							batch.select(index.get().getIndex().getName(), whereStatementCreator -> {
								KeySet toKey = relationDescriber.getToKeys();
								query(toType).accept(fromKey, toKey, whereStatementCreator);
							}, -1, row -> {

								batch.select(tableName, whereStatementCreator -> {
									KeySet primaryKeys = this.typeDescriber.primaryKeys();
									query(toType).accept(primaryKeys, row, whereStatementCreator);
								}, -1, r -> {
									if (relationDescriber.isCollectionRelation()) {
										relations.add(new ProphesiesHydrator<>(factory.get(toType), r, mappingHelper).get());
									} else {
										((Mapping) t)._setRelation(relationDescriber, new ProphesiesHydrator<>(factory.get(toType), r, mappingHelper).get());
									}
								});

							});


						} else {
							batch.select(tableName, whereStatementCreator -> {
								KeySet toKey = relationDescriber.getToKeys();
								query(toType).accept(fromKey, toKey, whereStatementCreator);
							}, -1, r -> {
								Object obj = new ProphesiesHydrator<>(factory.get(toType), r, mappingHelper).get();

								((Mapping) t)._setRelation(relationDescriber, obj);
								;
							});
						}


					});
				}
			}
			stream = list.stream();
		}
		return stream;
	}

	private void accept(CompoundKey fromKey, KeySet toKey, WhereStatementCreator whereStatementCreator) {
		int i = 0;
		for (Property.PropertyValue<?> k : ((Property.PropertyValueList<?>) fromKey.getValues())) {
			Property to = toKey.toProperties().get(i);
			eq(to.value(k.getValue()));
			i++;
		}
		predicates.forEach(c -> c.accept(whereStatementCreator));
	}

	private void accept(KeySet primaryKeys, Row row, WhereStatementCreator whereStatementCreator) {
		for (Property k : primaryKeys.toProperties()) {
			eq(k.value(row.get(k.getToken().snake_case(), k.getType().clazz)));
		}
		predicates.forEach(c -> c.accept(whereStatementCreator));
	}

	@Override
	public long count() {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public CrudRepository.CrudUpdateResult delete() {
		throw new RuntimeException("Not implemented");
	}

	private CompoundKey getRelationKey(TypeDescriber<T> typeDescriber, KeySet fromKeys, T t) {
		CompoundKey compoundKey = new CompoundKey();
		fromKeys.forEach(f -> {
			compoundKey.add(((Property)f.getProperty()).value(mappingHelper.getValue(t, f.getProperty())));
		});
		return compoundKey;
	}

	@Override
	public <X> PropheciesQuery<T> subQuery(Function<T, X> field, Consumer<PropheciesQuery<X>> subQuery) {
		field.apply(instance);
		this.subQuery(typeDescriber.relations().get(queryWrapper.getCurrentProperty().getToken().snake_case()), subQuery);
		return this;
	}

	@Override
	public <X> PropheciesQuery<T> subQuery(BiConsumer<T, X> field, Consumer<PropheciesQuery<X>> subQuery) {
		field.accept(instance, null);
		this.subQuery(typeDescriber.relations().get(queryWrapper.getCurrentProperty().getToken().snake_case()), subQuery);
		return this;
	}

	@Override
	public <X> PropheciesQuery<T> subQueryList(Function<T, List<X>> field, Consumer<PropheciesQuery<X>> subQuery) {
		field.apply(instance);
		this.subQuery(typeDescriber.relations().get(queryWrapper.getCurrentProperty().getToken().snake_case()), subQuery);
		return this;
	}

	@Override
	public <X> PropheciesQuery<T> subQueryList(BiConsumer<T, List<X>> field, Consumer<PropheciesQuery<X>> subQuery) {
		field.accept(instance, null);
		this.subQuery(typeDescriber.relations().get(queryWrapper.getCurrentProperty().getToken().snake_case()), subQuery);
		return this;
	}

	private class SortingElement {
		Token token;
		boolean ascending;

		public SortingElement(Token token, boolean ascending) {
			this.token = token;
			this.ascending = ascending;
		}
	}
}
