package io.prophecies.automapper;

import io.ran.GenericFactory;
import io.ran.MappingHelper;
import io.ran.TestDoubleDb;

import javax.inject.Inject;

public class PropheciesTestDoubleRepositoryFactory extends PropheciesRepositoryFactory {
	private TestDoubleDb testStore;

	@Inject
	public PropheciesTestDoubleRepositoryFactory(GenericFactory genericFactory, CqlGenerator cqlGenerator, TestDoubleDb testStore, MappingHelper mappingHelper) {
		super(() -> null, genericFactory, cqlGenerator, mappingHelper);
		this.testStore = testStore;
	}

	@Override
	public <T, K> PropheciesBaseCrudRepository<T, K> getBaseRepository(Class<T> modelType, Class<K> keyType) {
		return new PropheciesTestDoubleRepository(genericFactory, modelType, keyType, testStore, mappingHelper);
	}
}
