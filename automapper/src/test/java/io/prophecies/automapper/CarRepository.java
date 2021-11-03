package io.prophecies.automapper;

import io.ran.GenericFactory;

import javax.inject.Inject;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;

public class CarRepository extends ProphesiesCrudRepositoryImpl<Car, UUID> {

	@Inject
	public CarRepository(PropheciesRepositoryFactory factory) {
		super(factory.getBaseRepository(Car.class, UUID.class));
	}

	public Collection<Car> getEager(UUID id) {
		return query().eq(Car::getId,id).withEager(Car::getExhaust).execute().collect(Collectors.toList());
	}
}
