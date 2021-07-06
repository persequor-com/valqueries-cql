package io.prophecies.automapper;

import java.util.Optional;
import java.util.stream.Stream;

public class ProphesiesCrudRepositoryImpl<T, K> implements ProphesiesCrudRepository<T, K> {
	private PropheciesBaseCrudRepository<T, K> baseRepo;

	public ProphesiesCrudRepositoryImpl(PropheciesBaseCrudRepository<T, K> baseRepo) {
		this.baseRepo = baseRepo;
	}

	@Override
	public Optional<T> get(K k) {
		return baseRepo.get(k);
	}

	@Override
	public Stream<T> getAll() {
		return baseRepo.getAll();
	}

	@Override
	public CrudUpdateResult deleteById(K k) {
		return baseRepo.deleteById(k);
	}

	@Override
	public CrudUpdateResult save(T t) {
		return baseRepo.save(t);
	}

	protected  PropheciesQuery<T> query() {
		return baseRepo.query();
	}
}
