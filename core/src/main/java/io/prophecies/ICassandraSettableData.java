package io.prophecies;


import com.datastax.oss.driver.api.core.data.SettableByName;

import java.time.ZonedDateTime;

public interface ICassandraSettableData<T extends ICassandraSettableData<T>> extends SettableByName<T> {

	T setZonedDateTime(String name, ZonedDateTime value);
}
