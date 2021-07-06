package io.prophecies.automapper;

import io.ran.CrudRepository;

import javax.inject.Inject;
import java.util.UUID;

public class EngineRepository extends ProphesiesCrudRepositoryImpl<Engine, UUID> {
	private io.prophecies.automapper.EngineCarRepository engineCarRepository;

	@Inject
	public EngineRepository(PropheciesRepositoryFactory factory, EngineCarRepository engineCarRepository) {
		super(factory.getBaseRepository(Engine.class, UUID.class));
		this.engineCarRepository = engineCarRepository;
	}

	@Override
	public CrudRepository.CrudUpdateResult save(Engine engine) {
		engine.getCars().forEach(car -> {
			io.prophecies.automapper.EngineCar engineCar = new io.prophecies.automapper.EngineCar();
			engineCar.setCarId(car.getId());
			engineCar.setEngineId(engine.getId());
			engineCarRepository.save(engineCar);
		});
		return super.save(engine);
	}
}
