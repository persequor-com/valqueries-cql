package io.prophecies.automapper;

import javax.inject.Inject;

public class EngineCarRepository extends ProphesiesCrudRepositoryImpl<EngineCar, EngineCarKey> {
	@Inject
	public EngineCarRepository(PropheciesRepositoryFactory factory) {
		super(factory.getBaseRepository(EngineCar.class, EngineCarKey.class));
	}
}
