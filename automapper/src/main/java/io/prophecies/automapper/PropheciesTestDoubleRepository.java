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

	Map<K, T> getStore(Class<T> modelType) {
		return (Map<K, T>)store.store.computeIfAbsent(modelType, t -> Collections.synchronizedMap(new HashMap<>()));
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
		K key = getKey(t);
		T existing = getStore(modelType).put(key, t);
		return new CrudUpdateResult() {
			@Override
			public int affectedRows() {
				return existing != null && !existing.equals(t) ? 1 : 0;
			}
		};
	}

	private K getKey(T t) {
		K key;
		if (CompoundKey.class.isAssignableFrom(keyType)) {
			key = (K)mappingHelper.getKey(t);
		} else {
			key = (K)((Property.PropertyValueList<?>)mappingHelper.getKey(t).getValues()).get(0).getValue();
		}
		return key;
	}

	@Override
	public PropheciesQuery<T> query() {
		return new TestDoubleQuery<T>(modelType, mappingHelper, genericFactory, store);
	}

}
