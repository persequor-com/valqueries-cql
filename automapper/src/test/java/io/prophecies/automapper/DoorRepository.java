package io.prophecies.automapper;

import javax.inject.Inject;
import java.util.UUID;

public class DoorRepository extends ProphesiesCrudRepositoryImpl<Door, UUID> {
	@Inject
	public DoorRepository(PropheciesRepositoryFactory factory) {
		super(factory.getBaseRepository(Door.class, UUID.class));
	}
}
