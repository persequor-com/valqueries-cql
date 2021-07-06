package io.prophecies.automapper;

import io.ran.CrudRepositoryBaseRepo;

public interface PropheciesBaseCrudRepository<T,K> extends CrudRepositoryBaseRepo<T,K, PropheciesQuery<T>> {

}
