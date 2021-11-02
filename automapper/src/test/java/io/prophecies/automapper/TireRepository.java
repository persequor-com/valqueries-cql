package io.prophecies.automapper;

import javax.inject.Inject;

public class TireRepository extends ProphesiesCrudRepositoryImpl<Tire, Tire> {
	@Inject
	public TireRepository(PropheciesRepositoryFactory factory) {
		super(factory.getBaseRepository(Tire.class, Tire.class));
	}
}
