package io.prophecies.automapper;

import io.ran.Mapper;
import io.ran.Relation;

import java.util.List;
import java.util.UUID;

@Mapper(dbType = Prophecies.class)
public class Engine {
	private UUID id;
	@Relation(collectionElementType = io.prophecies.automapper.Car.class, via = io.prophecies.automapper.EngineCar.class)
	private transient List<io.prophecies.automapper.Car> cars;

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public List<io.prophecies.automapper.Car> getCars() {
		return cars;
	}

	public void setCars(List<io.prophecies.automapper.Car> cars) {
		this.cars = cars;
	}
}
