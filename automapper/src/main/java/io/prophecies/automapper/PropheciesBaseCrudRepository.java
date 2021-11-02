package io.prophecies.automapper;

import io.prophecies.CassandraBatch;
import io.ran.CrudRepository;
import io.ran.CrudRepositoryBaseRepo;

public interface PropheciesBaseCrudRepository<T,K> extends CrudRepositoryBaseRepo<T,K, PropheciesQuery<T>> {
	<O, OK> CrudRepository.CrudUpdateResult save(PropheciesBatch batch, O o, Class<O> oClass);
	PropheciesBatch getBatch();
	Class<T> getModelType();
	Class<K> getKeyType();
}
