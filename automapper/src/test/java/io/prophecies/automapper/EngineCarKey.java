package io.prophecies.automapper;

import io.ran.Clazz;
import io.ran.CompoundKey;
import io.ran.Property;

import java.util.UUID;

public class EngineCarKey extends CompoundKey {
	private Property.PropertyValue<UUID> carId;
	private Property.PropertyValue<UUID> engineId;
	public   EngineCarKey(UUID carId, UUID engineId)  {
		this.carId = new Property.PropertyValue(Property.get("car_id",(Clazz)Clazz.of(UUID.class)).setOn(Clazz.of(EngineCar.class)), carId);
		values.add(this.carId);
		this.engineId = new Property.PropertyValue(Property.get("engine_id",(Clazz)Clazz.of(UUID.class)).setOn(Clazz.of(EngineCar.class)), engineId);
		values.add(this.engineId);
	}

	public  UUID getCarId()  {
		return this.carId.getValue();
	}

	public  UUID getEngineId()  {
		return this.engineId.getValue();
	}

}