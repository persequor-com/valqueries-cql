package io.prophecies.automapper;

import io.ran.DbType;

public class Prophecies  implements DbType {
	@Override
	public String getName() {
		return "Prophecies";
	}
}
