package io.prophecies.automapper;

import javax.inject.Inject;
import java.util.UUID;

public class ExhaustRepository extends ProphesiesCrudRepositoryImpl<Exhaust, UUID> {
	@Inject
	public ExhaustRepository(PropheciesRepositoryFactory factory) {
		super(factory.getBaseRepository(Exhaust.class, UUID.class));
	}
}
