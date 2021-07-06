package io.prophecies.automapper;

import io.ran.Key;
import io.ran.Mapper;
import io.ran.PrimaryKey;

import java.util.UUID;

@Mapper(dbType = Prophecies.class)
public class EngineCar {
	@PrimaryKey
	private UUID carId;
	@PrimaryKey
	private UUID engineId;

	public UUID getCarId() {
		return carId;
	}

	public void setCarId(UUID carId) {
		this.carId = carId;
	}

	public UUID getEngineId() {
		return engineId;
	}

	public void setEngineId(UUID engineId) {
		this.engineId = engineId;
	}
}
