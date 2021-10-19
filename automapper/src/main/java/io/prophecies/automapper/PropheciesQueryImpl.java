package io.prophecies.automapper;

import io.prophecies.Cassandra;
import io.prophecies.CassandraBatch;
import io.prophecies.IndexConfig;
import io.prophecies.WhereStatementCreator;
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
import java.util.function.Consumer;
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
	public <X, Z extends CrudRepository.InlineQuery<X, Z>> PropheciesQuery<T> subQuery(RelationDescriber relationDescriber, Consumer<Z> consumer) {
		return null;
	}


	@Override
	public Stream<T> execute() {
		Stream<T> stream = StreamSupport.stream(cassandra.select(tableName, whereStatementCreator -> {
			predicates.forEach(p -> {
				p.accept(whereStatementCreator);
			});
		}).map(r -> new ProphesiesHydrator<T>(factory.get(modelType), r, mappingHelper).get()).spliterator(), false);
		if (!eagers.isEmpty()) {
			List<T> list = stream.collect(Collectors.toList());
			try (CassandraBatch batch = cassandra.batch()) {
				try (CassandraBatch indexBatch = cassandra.batch()) {
					for (T t : list) {

						this.eagers.forEach(relationDescriber -> {
							Class toType = relationDescriber.getToClass().clazz;
							TypeDescriber typeDescriber = (TypeDescriber) TypeDescriberImpl.getTypeDescriber(relationDescriber.getToClass().clazz);
							CqlDescriber cqlDescriber = CqlDescriber.get(typeDescriber);
							Optional<CqlDescriber.RelationIndex> index = cqlDescriber.forReverseRelation(relationDescriber);
							List<Object> relations = new ArrayList<>();
							if (relationDescriber.isCollectionRelation()) {
								((Mapping) t)._setRelation(relationDescriber, relations);
							}
							if (index.isPresent()) {
								CompoundKey fromKey = getRelationKey(typeDescriber, relationDescriber.getFromKeys(), t);
								String tableName = Token.CamelCase(relationDescriber.getToClass().clazz.getSimpleName()).snake_case();

								indexBatch.select(index.get().getIndex().getName(), whereStatementCreator -> {
									KeySet toKey = relationDescriber.getToKeys();
									PropheciesQueryImpl<?> query = new PropheciesQueryImpl(cassandra, toType, cqlGenerator, factory, mappingHelper);
									int i = 0;
									for (Property.PropertyValue<?> k : ((Property.PropertyValueList<?>) fromKey.getValues())) {
										Property to = toKey.toProperties().get(i);
										query = (PropheciesQueryImpl<?>) query.eq(to.value(k.getValue()));
										i++;
									}
									query.predicates.forEach(c -> c.accept(whereStatementCreator));

								}, -1, row -> {

									batch.select(tableName, whereStatementCreator -> {
										KeySet primaryKeys = this.typeDescriber.primaryKeys();
										PropheciesQueryImpl<?> query = new PropheciesQueryImpl(cassandra, toType, cqlGenerator, factory, mappingHelper);
										int i = 0;
										for (Property k : primaryKeys.toProperties()) {
											query.eq(k.value(row.get(k.getToken().snake_case(), k.getType().clazz)));
											i++;
										}
										query.predicates.forEach(c -> c.accept(whereStatementCreator));

									}, -1, r -> {
										if (relationDescriber.isCollectionRelation()) {
											relations.add(new ProphesiesHydrator<>(factory.get(toType), r, mappingHelper).get());
										} else {
											((Mapping) t)._setRelation(relationDescriber, new ProphesiesHydrator<>(factory.get(toType), r, mappingHelper).get());
										}
									});

								});


							} else {
								CompoundKey fromKey = getRelationKey(this.typeDescriber, relationDescriber.getFromKeys(), t);
								String tableName = Token.CamelCase(relationDescriber.getToClass().clazz.getSimpleName()).snake_case();
								batch.select(tableName, whereStatementCreator -> {
									KeySet toKey = relationDescriber.getToKeys();
									PropheciesQueryImpl<?> query = new PropheciesQueryImpl(cassandra, toType, cqlGenerator, factory, mappingHelper);
									int i = 0;
									for (Property.PropertyValue<?> k : ((Property.PropertyValueList<?>) fromKey.getValues())) {
										Property to = toKey.toProperties().get(i);
										query = (PropheciesQueryImpl<?>) query.eq(to.value(k.getValue()));
										i++;
									}
									query.predicates.forEach(c -> c.accept(whereStatementCreator));
								}, -1, r -> {
									Object obj = new ProphesiesHydrator<>(factory.get(toType), r, mappingHelper).get();

									((Mapping) t)._setRelation(relationDescriber, obj);
									;
								});
							}


						});
					}
				}
			}
			stream = list.stream();
		}
		return stream;
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

	private class SortingElement {
		Token token;
		boolean ascending;

		public SortingElement(Token token, boolean ascending) {
			this.token = token;
			this.ascending = ascending;
		}
	}
}
