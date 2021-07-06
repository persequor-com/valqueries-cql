package io.prophecies;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class InsertStatementCreator implements ICassandraSettableData<InsertStatementCreator> {

	private final List<String> fields = new ArrayList<>();

	private void setField(String name) {
		fields.add(name);
	}

	public String getInsertStatement() {
		return "(" + String.join(", ", fields) + ") VALUES (" + fields.stream().map(s -> ":" + s).collect(Collectors.joining(", ")) + ")";
	}

	@NonNull
	@Override
	public InsertStatementCreator setInstant(@NonNull String name, @Nullable Instant v) {
		setField(name);
		return this;
	}

	@NonNull
	@Override
	public <ValueT> InsertStatementCreator set(@NonNull String name, @Nullable ValueT v, @NonNull TypeCodec<ValueT> codec) {
		setField(name);
		return this;
	}

	@NonNull
	@Override
	public <ValueT> InsertStatementCreator set(@NonNull String name, @Nullable ValueT v, @NonNull GenericType<ValueT> targetType) {
		setField(name);
		return this;
	}

	@NonNull
	@Override
	public InsertStatementCreator setLocalTime(@NonNull String name, @Nullable LocalTime v) {
		setField(name);
		return this;
	}

	@NonNull
	@Override
	public InsertStatementCreator setBigInteger(@NonNull String name, @Nullable BigInteger v) {
		setField(name);
		return this;
	}

	@NonNull
	@Override
	public InsertStatementCreator setCqlDuration(@NonNull String name, @Nullable CqlDuration v) {
		setField(name);
		return this;
	}

	@NonNull
	@Override
	public InsertStatementCreator setToken(@NonNull String name, @NonNull Token v) {
		setField(name);
		return this;
	}

	@NonNull
	@Override
	public InsertStatementCreator setTupleValue(@NonNull String name, @Nullable TupleValue v) {
		setField(name);
		return this;
	}

	@Override
	public InsertStatementCreator setZonedDateTime(String name, ZonedDateTime value) {
		setField(name);
		return this;
	}


	@Override
	public InsertStatementCreator setBoolean(String name, boolean v) {
		setField(name);
		return this;
	}

	@Override
	public InsertStatementCreator setByte(String name, byte v) {
		setField(name);
		return this;
	}

	@Override
	public InsertStatementCreator setShort(String name, short v) {
		setField(name);
		return this;
	}

	@Override
	public InsertStatementCreator setInt(String name, int v) {
		setField(name);
		return this;
	}

	@Override
	public InsertStatementCreator setLong(String name, long v) {
		setField(name);
		return this;
	}

	@Override
	public InsertStatementCreator setFloat(String name, float v) {
		setField(name);
		return this;
	}

	@Override
	public InsertStatementCreator setDouble(String name, double v) {
		setField(name);
		return this;
	}

	@Override
	public InsertStatementCreator setString(String name, String v) {
		setField(name);
		return this;
	}

	@Override
	public InsertStatementCreator setByteBuffer(String name, ByteBuffer v) {
		setField(name);
		return this;
	}

	@Override
	public InsertStatementCreator setBytesUnsafe(String name, ByteBuffer v) {
		setField(name);
		return this;
	}

	@Override
	public InsertStatementCreator setBigDecimal(String name, BigDecimal v) {
		setField(name);
		return this;
	}

	@Override
	public InsertStatementCreator setUuid(String name, UUID v) {
		setField(name);
		return this;
	}

	@Override
	public InsertStatementCreator setInetAddress(String name, InetAddress v) {
		setField(name);
		return this;
	}

	@Override
	public <E> InsertStatementCreator setList(String name, List<E> v, Class<E> elementsClass) {
		setField(name);
		return this;
	}


	@Override
	public <K, V> InsertStatementCreator setMap(String name, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass) {
		setField(name);
		return this;
	}


	@Override
	public <E> InsertStatementCreator setSet(String name, Set<E> v, Class<E> elementsClass) {
		setField(name);
		return this;
	}


	@Override
	public InsertStatementCreator setUdtValue(String name, UdtValue v) {
		setField(name);
		return this;
	}

	@Override
	public InsertStatementCreator setToNull(String name) {
		setField(name);
		return this;
	}

	@NonNull
	@Override
	public InsertStatementCreator setLocalDate(String name, @Nullable LocalDate v) {
		setField(name);
		return this;
	}

	@Override
	public <V> InsertStatementCreator set(String name, V v, Class<V> targetClass) {
		setField(name);
		return this;
	}

	@Override
	public int firstIndexOf(@NonNull String name) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setBytesUnsafe(int i, @Nullable ByteBuffer v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}


	@Override
	public int size() {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public DataType getType(int i) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public CodecRegistry codecRegistry() {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setToNull(int i) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public <ValueT> InsertStatementCreator set(int i, @Nullable ValueT v, @NonNull TypeCodec<ValueT> codec) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public <ValueT> InsertStatementCreator set(int i, @Nullable ValueT v, @NonNull GenericType<ValueT> targetType) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public <ValueT> InsertStatementCreator set(int i, @Nullable ValueT v, @NonNull Class<ValueT> targetClass) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setBoolean(int i, boolean v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setByte(int i, byte v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setDouble(int i, double v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setFloat(int i, float v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setInt(int i, int v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setLong(int i, long v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setShort(int i, short v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setInstant(int i, @Nullable Instant v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setLocalDate(int i, @Nullable LocalDate v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setLocalTime(int i, @Nullable LocalTime v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setByteBuffer(int i, @Nullable ByteBuffer v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setString(int i, @Nullable String v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setBigInteger(int i, @Nullable BigInteger v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setBigDecimal(int i, @Nullable BigDecimal v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setUuid(int i, @Nullable UUID v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setInetAddress(int i, @Nullable InetAddress v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setCqlDuration(int i, @Nullable CqlDuration v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setToken(int i, @NonNull Token v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public <ElementT> InsertStatementCreator setList(int i, @Nullable List<ElementT> v, @NonNull Class<ElementT> elementsClass) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public <ElementT> InsertStatementCreator setSet(int i, @Nullable Set<ElementT> v, @NonNull Class<ElementT> elementsClass) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public <KeyT, ValueT> InsertStatementCreator setMap(int i, @Nullable Map<KeyT, ValueT> v, @NonNull Class<KeyT> keyClass, @NonNull Class<ValueT> valueClass) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setUdtValue(int i, @Nullable UdtValue v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public InsertStatementCreator setTupleValue(int i, @Nullable TupleValue v) {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}

	@NonNull
	@Override
	public ProtocolVersion protocolVersion() {
		throw new UnsupportedOperationException("Not supported in: " + getClass());
	}
}
