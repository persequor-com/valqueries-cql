package io.prophecies.automapper;


import io.prophecies.IndexConfig;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CqlDescriberTest {
	@Test
	public void happyPath_car() {
		CqlDescriber describer = CqlDescriber.get(Car.class);
		List<IndexConfig.Index> actual = describer.getIndices();
		assertEquals(1, actual.size());
		assertEquals("exhaust_to_car", actual.get(0).getName());
		assertEquals(2, actual.get(0).getFields().size());
		assertEquals("exhaust_id", actual.get(0).getFields().get(0));
		assertEquals("id", actual.get(0).getFields().get(1));
	}

	@Test
	public void happyPath_door() {
		CqlDescriber describer = CqlDescriber.get(Door.class);
		List<IndexConfig.Index> actual = describer.getIndices();
		assertEquals(1, actual.size());
		assertEquals("car_to_door", actual.get(0).getName());
		assertEquals(2, actual.get(0).getFields().size());
		assertEquals("car_id", actual.get(0).getFields().get(0));
		assertEquals("id", actual.get(0).getFields().get(1));
	}
}