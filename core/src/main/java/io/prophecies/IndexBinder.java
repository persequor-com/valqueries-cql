package io.prophecies;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class IndexBinder implements ICassandraSettableData<IndexBinder> {
	private final IndexConfig.Index indexConfig;
	private final ICassandraSettableData<?> stmt;

	public IndexBinder(IndexConfig.Index indexConfig, ICassandraSettableData<?> stmt) {
		this.indexConfig = indexConfig;
		this.stmt = stmt;
	}

	@Override
	public IndexBinder setZonedDateTime(String name, ZonedDateTime value) {
		if (indexConfig.contains(name)) {
			stmt.set(name, value, ZonedDateTime.class);
		}
		return this;
	}

	@Override
	public int firstIndexOf(@NonNull String s) {
		return stmt.firstIndexOf(s);
	}

	@NonNull
	@Override
	public IndexBinder setBytesUnsafe(int i, @Nullable ByteBuffer byteBuffer) {
		stmt.setBytesUnsafe(i, byteBuffer);
		return this;
	}

	@Override
	public int size() {
		return stmt.size();
	}

	@NonNull
	@Override
	public DataType getType(int i) {
		return stmt.getType(i);
	}

	@NonNull
	@Override
	public CodecRegistry codecRegistry() {
		return stmt.codecRegistry();
	}

	@NonNull
	@Override
	public ProtocolVersion protocolVersion() {
		return stmt.protocolVersion();
	}

	@Override
	public IndexBinder setBoolean(String name, boolean v) {
		if (indexConfig.contains(name)) {
			stmt.setBoolean(name, v);
		}
		return this;
	}

	@Override
	public IndexBinder setByte(String name, byte v) {
		if (indexConfig.contains(name)) {
			stmt.setByte(name, v);
		}
		return this;
	}

	@Override
	public IndexBinder setShort(String name, short v) {
		if (indexConfig.contains(name)) {
			stmt.setShort(name, v);
		}
		return this;
	}

	@Override
	public IndexBinder setInt(String name, int v) {
		if (indexConfig.contains(name)) {
			stmt.setInt(name, v);
		}
		return this;
	}

	@Override
	public IndexBinder setLong(String name, long v) {
		if (indexConfig.contains(name)) {
			stmt.setLong(name, v);
		}
		return this;
	}

	@Override
	public IndexBinder setFloat(String name, float v) {
		if (indexConfig.contains(name)) {
			stmt.setFloat(name, v);
		}
		return this;
	}

	@Override
	public IndexBinder setDouble(String name, double v) {
		if (indexConfig.contains(name)) {
			stmt.setDouble(name, v);
		}
		return this;
	}

	@Override
	public IndexBinder setString(String name, String v) {
		if (indexConfig.contains(name)) {
			stmt.setString(name, v);
		}
		return this;
	}

	@Override
	public IndexBinder setByteBuffer(String name, ByteBuffer v) {
		if (indexConfig.contains(name)) {
			stmt.setByteBuffer(name, v);
		}
		return this;
	}

	@Override
	public IndexBinder setBytesUnsafe(String name, ByteBuffer v) {
		if (indexConfig.contains(name)) {
			stmt.setBytesUnsafe(name, v);
		}
		return this;
	}

	@Override
	public IndexBinder setBigDecimal(String name, BigDecimal v) {
		if (indexConfig.contains(name)) {
			stmt.setBigDecimal(name, v);
		}
		return this;
	}

	@Override
	public IndexBinder setUuid(String name, UUID v) {
		if (indexConfig.contains(name)) {
			stmt.setUuid(name, v);
		}
		return this;
	}

	@Override
	public IndexBinder setInetAddress(String name, InetAddress v) {
		if (indexConfig.contains(name)) {
			stmt.setInetAddress(name, v);
		}
		return this;
	}

	@Override
	public <E> IndexBinder setList(String name, List<E> v, Class<E> elementsClass) {
		if (indexConfig.contains(name)) {
			stmt.setList(name, v, elementsClass);
		}
		return this;
	}


	@Override
	public <K, V> IndexBinder setMap(String name, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass) {
		if (indexConfig.contains(name)) {
			stmt.setMap(name, v, keysClass, valuesClass);
		}
		return this;
	}


	@Override
	public <E> IndexBinder setSet(String name, Set<E> v, Class<E> elementsClass) {
		if (indexConfig.contains(name)) {
			stmt.setSet(name, v, elementsClass);
		}
		return this;
	}


	@Override
	public IndexBinder setUdtValue(String name, UdtValue v) {
		if (indexConfig.contains(name)) {
			stmt.setUdtValue(name, v);
		}
		return this;
	}

	@Override
	public IndexBinder setToNull(String name) {
		if (indexConfig.contains(name)) {
			stmt.setToNull(name);
		}
		return this;
	}

	@NonNull
	@Override
	public IndexBinder setLocalDate(String name, @Nullable LocalDate v) {
		if (indexConfig.contains(name)) {
			stmt.setLocalDate(name, v);
		}
		return this;
	}

	@Override
	public <V> IndexBinder set(String name, V v, Class<V> targetClass) {
		if (indexConfig.contains(name)) {
			stmt.set(name, v, targetClass);
		}
		return this;
	}
}
