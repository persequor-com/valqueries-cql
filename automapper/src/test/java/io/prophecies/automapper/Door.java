package io.prophecies.automapper;

import io.ran.Mapper;
import io.ran.Relation;

import java.util.UUID;

@Mapper(dbType = Prophecies.class)
public class Door {
	private UUID id;
	private String title;
	@Relation()
	private transient io.prophecies.automapper.Car car;
	private UUID carId;

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public io.prophecies.automapper.Car getCar() {
		return car;
	}

	public void setCar(Car car) {
		this.carId = car.getId();
		this.car = car;
	}

	public UUID getCarId() {
		return carId;
	}

	public void setCarId(UUID carId) {
		this.carId = carId;
	}
}
