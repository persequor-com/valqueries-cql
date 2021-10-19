package io.prophecies.automapper;/* Copyright (C) Persequor ApS - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Persequor Development Team <partnersupport@persequor.com>, 
 */

import io.prophecies.CassandraBatch;
import io.ran.GenericFactory;
import io.ran.MappingHelper;
import io.ran.TypeDescriber;
import io.ran.TypeDescriberImpl;
import io.ran.token.Token;
import io.prophecies.Cassandra;
import io.prophecies.WhereStatementCreator;

import javax.inject.Inject;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ProphesiesCrudRepositoryBase<T,K> implements PropheciesBaseCrudRepository<T, K> {
	protected CqlDescriber cqlDescriber;
	protected Cassandra cassandra;
	protected Class<T> modelType;
	protected Class<K> keyType;
	protected GenericFactory factory;
	protected TypeDescriber<T> typeDescriber;
	protected CqlGenerator cqlGenerator;
	protected MappingHelper mappingHelper;

	@Inject
	public ProphesiesCrudRepositoryBase(Cassandra cassandra, Class<T> modelType, Class<K> keyType, GenericFactory factory, CqlGenerator cqlGenerator, MappingHelper mappingHelper) {
		this.cassandra = cassandra;
		this.modelType = modelType;
		this.keyType = keyType;
		this.factory = factory;
		this.cqlGenerator = cqlGenerator;
		this.mappingHelper = mappingHelper;
		this.typeDescriber = TypeDescriberImpl.getTypeDescriber(modelType);
		this.cqlDescriber = CqlDescriber.get(typeDescriber);

	}

	private void setKey(WhereStatementCreator b, K id) {
		if (keyType == String.class) {
			b.andEquals("id", (String) id);
		} else if (keyType == UUID.class) {
			b.andEquals("id",(UUID)id);
		} else {
			throw new RuntimeException("So far unhandled key type: "+keyType.getName());
		}
	}



	@Override
	public Optional<T> get(K id) {
		return Optional.ofNullable(cassandra.select(getTableName(), s -> setKey(s, id))
			.map(r -> new ProphesiesHydrator<T>(factory.get(modelType), r, mappingHelper).get())
			.one());
	}

	@Override
	public Stream<T> getAll() {
		return StreamSupport.stream(cassandra.select(getTableName(), s -> {})
				.map(r -> new ProphesiesHydrator<T>(factory.get(modelType), r, mappingHelper).get()).spliterator(), false);
	}

	@Override
	public CrudUpdateResult deleteById(K id) {
		return new PropheciesUpdateResult(cassandra.delete("DELETE FROM "+getTableName()+" WHERE id = :id", s -> setKey(s, id)));
	}

	@Override
	public CrudUpdateResult save(T t) {
		try(CassandraBatch batch = cassandra.batch()) {
			return save(new PropheciesBatch(batch), t, modelType);
		}
	}

	@Override
	public PropheciesQuery<T> query() {
		return new PropheciesQueryImpl<T>(cassandra, modelType, cqlGenerator,  factory, mappingHelper);
	}

	private String getTableName() {
		return getTableName(modelType);
	}

	private String getTableName(Class<?> clazz) {
		return Token.CamelCase(clazz.getSimpleName()).snake_case();
	}

	@Override
	public <O, OK> CrudUpdateResult save(PropheciesBatch batch, O o, Class<O> oClass) {
		TypeDescriber<O> typeDescriber = TypeDescriberImpl.getTypeDescriber(oClass);
		CqlDescriber cqlDescriber = CqlDescriber.get(typeDescriber);

		cqlDescriber.getIndices().forEach(index -> {
			batch.insert(index.getName(), new ProphesiesIndexColumnizer<O>(index,typeDescriber, mappingHelper).init(o));
		});
		batch.insert(getTableName(oClass), new ProphesiesColumnizer<O>(typeDescriber, mappingHelper).init(o));
		return new PropheciesUpdateResult(null);
	}

	@Override
	public PropheciesBatch getBatch() {
		return new PropheciesBatch(cassandra.batch());
	}

	@Override
	public Class<T> getModelType() {
		return modelType;
	}

	@Override
	public Class<K> getKeyType() {
		return keyType;
	}
}
