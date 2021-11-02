package io.prophecies.automapper;

import io.ran.CompoundKey;
import io.ran.GenericFactory;
import io.ran.MappingHelper;
import io.ran.Property;
import io.ran.TestDoubleDb;
import io.ran.TypeDescriber;
import io.ran.TypeDescriberImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class PropheciesTestDoubleRepository<T, K> implements PropheciesBaseCrudRepository<T, K> {
	private final TestDoubleDb store;
	protected GenericFactory genericFactory;
	protected Class<T> modelType;
	protected Class<K> keyType;
	protected TypeDescriber<T> typeDescriber;
	private MappingHelper mappingHelper;

	public PropheciesTestDoubleRepository(GenericFactory genericFactory, Class<T> modelType, Class<K> keyType, TestDoubleDb store, MappingHelper mappingHelper) {
		this.store = store;
		this.mappingHelper = mappingHelper;
		this.genericFactory = genericFactory;
		this.modelType = modelType;

		this.keyType = keyType;
		this.typeDescriber = TypeDescriberImpl.getTypeDescriber(modelType);
	}

	<O, OK> Map<OK, O> getStore(Class<O> modelType) {
		return (Map<OK, O>)store.store.computeIfAbsent(modelType, t -> Collections.synchronizedMap(new HashMap<>()));
	}

	@Override
	public Optional<T> get(K k) {
		return Optional.ofNullable(getStore(modelType).get(k));
	}



	@Override
	public Stream<T> getAll() {
		return getStore(modelType).values().stream();
	}

	@Override
	public CrudUpdateResult deleteById(K k) {
		T existing = getStore(modelType).remove(k);
		return new CrudUpdateResult() {
			@Override
			public int affectedRows() {
				return existing != null ? 1 : 0;
			}
		};
	}

	@Override
	public CrudUpdateResult save(T t) {
		return save(new PropheciesBatch(null), t, modelType);
	}

	private <X, O> X getKey(O o) {
		X key;
		CompoundKey compoundKey = mappingHelper.getKey(o);
		if (compoundKey.getValues().size() > 1) {
			key = (X)mappingHelper.getKey(o);
		} else {
			key = (X)((Property.PropertyValueList<?>)mappingHelper.getKey(o).getValues()).get(0).getValue();
		}
		return key;
	}

	@Override
	public PropheciesQuery<T> query() {
		return new TestDoubleQuery<T>(modelType, mappingHelper, genericFactory, store);
	}


	@Override
	public <O, OK> CrudUpdateResult save(PropheciesBatch batch, O o, Class<O> oClass) {
		Object key = getKey(o);
		O existing = getStore(oClass).put(key, o);
		return new CrudUpdateResult() {
			@Override
			public int affectedRows() {
				return existing != null && !existing.equals(o) ? 1 : 0;
			}
		};
	}

	@Override
	public PropheciesBatch getBatch() {
		return new PropheciesBatch(null);
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
