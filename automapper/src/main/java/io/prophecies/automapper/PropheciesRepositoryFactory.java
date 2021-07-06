package io.prophecies.automapper;

import io.ran.GenericFactory;
import io.ran.MappingHelper;
import io.prophecies.Cassandra;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class PropheciesRepositoryFactory {
	private Provider<Cassandra> cassandra;
	GenericFactory genericFactory;
	private CqlGenerator cqlGenerator;
	protected MappingHelper mappingHelper;

	@Inject
	public PropheciesRepositoryFactory(Provider<Cassandra> cassandra, GenericFactory genericFactory, CqlGenerator cqlGenerator, MappingHelper mappingHelper) {
		this.cassandra = cassandra;
		this.genericFactory = genericFactory;
		this.cqlGenerator = cqlGenerator;
		this.mappingHelper = mappingHelper;
	}

	public <T, K> PropheciesBaseCrudRepository<T, K> getBaseRepository(Class<T> modelType, Class<K> keyType) {
		return new ProphesiesCrudRepositoryBase<T, K>(cassandra.get(), modelType, keyType, genericFactory, cqlGenerator, mappingHelper);
	}
}
