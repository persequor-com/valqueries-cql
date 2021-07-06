package io.prophecies;

import org.junit.Test;

import static org.junit.Assert.*;

public class IndexConfigTest {
	@Test
	public void happyPath() {
		IndexConfig.build("mytable", primaryKey -> primaryKey.add("id"))
			.add("myindex", field -> field.add("otherfield").add("id"));
	}

	@Test(expected = IndexConfigException.class)
	public void ifPrimaryKeyIsNotIndexKeyThrow() {
		IndexConfig.build("mytable", primaryKey -> primaryKey.add("id"))
			.add("myindex", field -> field.add("otherfield").add("notid"));
	}

	@Test
	public void showMissingBitsInExceptionMessage() {
		try {
			IndexConfig.build("mytable", primaryKey -> primaryKey.add("id").add("otherPrimaryKey"))
				.add("myindex", field -> field.add("otherfield").add("notid"));
			fail("Expected a message to be thrown");
		} catch (IndexConfigException exception) {
			assertEquals("The entire primary key must be represented in the index. Missing fields where: id, otherPrimaryKey", exception.getMessage());
		}
	}
}
